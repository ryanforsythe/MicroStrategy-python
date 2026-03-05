"""
UserGroups.py — Audit, export, and document MicroStrategy user groups.

Subcommands
───────────
  audit      — Identify groups with no direct members (empty) and/or groups
               with directly-assigned privileges.  Writes separate output
               files for each category.

  export     — Export all groups with their members and privileges in a
               single file.

  privileges — List the privileges directly assigned to each group
               (inherited privileges are excluded).

  members    — List the direct members of each group.  Pass --resolve to
               recursively expand subgroups and return the effective set
               of users.

Usage
─────
  python UserGroups.py audit      <env>  [--format csv|json] [--output-dir PATH]
  python UserGroups.py export     <env>  [--format csv|json] [--output-dir PATH]
  python UserGroups.py privileges <env>  [--format csv|json] [--output-dir PATH]
  python UserGroups.py members    <env>  [--format csv|json] [--resolve]
                                         [--output-dir PATH]

Examples
────────
  # Preview empty groups and privileged groups on dev
  python UserGroups.py audit dev

  # Export all group details from prod to JSON
  python UserGroups.py export prod --format json

  # Show directly-assigned privileges on QA
  python UserGroups.py privileges qa

  # List direct members of every group on prod
  python UserGroups.py members prod

  # Recursively resolve effective user membership on prod
  python UserGroups.py members prod --resolve
"""

import argparse
import json
from pathlib import Path

from loguru import logger
from mstrio.access_and_security.privilege_mode import PrivilegeMode
from mstrio.users_and_groups import UserGroup, list_user_groups

from mstrio_core import MstrConfig, get_mstrio_connection, write_csv
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]
DEPRECATE_PREFIX = "DEPRECATE-"

# ── Column schemas ─────────────────────────────────────────────────────────────

_AUDIT_EMPTY_COLS = ["id", "name", "description"]
_AUDIT_PRIV_COLS = ["id", "name", "description", "privilege_count", "privilege_names"]
_EXPORT_CSV_COLS = [
    "id", "name", "description",
    "member_count", "members",
    "privilege_count", "privileges",
]
_PRIV_CSV_COLS = [
    "group_id", "group_name", "group_description",
    "priv_id", "priv_name", "priv_type",
]
_MEMBERS_CSV_COLS = [
    "group_id", "group_name",
    "member_id", "member_name", "member_type",
]
_RESOLVED_CSV_COLS = ["group_id", "group_name", "user_id", "user_name"]

# ── Privilege helpers ──────────────────────────────────────────────────────────


def _priv_to_dict(priv) -> dict:
    """Normalise a Privilege object to a minimal standard shape.

    Privilege attributes (mstrio-py): id, name, description, categories,
    is_project_level_privilege.  'categories' is mapped to the 'type' key
    used throughout this script's output schemas.
    """
    cat = getattr(priv, "categories", "") or ""
    return {
        "id": str(getattr(priv, "id", "")),
        "name": getattr(priv, "name", ""),
        "type": ", ".join(cat) if isinstance(cat, (list, tuple)) else str(cat),
    }


def _list_direct_privileges(group) -> list[dict]:
    """
    Return the privileges directly assigned to *group* (inherited excluded).

    Falls back to an empty list on any API error so a single bad group does
    not abort the operation.
    """
    try:
        raw = group.list_privileges(mode=PrivilegeMode.GRANTED, to_dataframe=False) or []
        return [_priv_to_dict(p) for p in raw]
    except Exception as exc:
        logger.warning(
            "Could not list privileges for group {name!r} ({id}): {exc}",
            name=getattr(group, "name", "?"),
            id=getattr(group, "id", "?"),
            exc=exc,
        )
        return []


# ── Member helpers ─────────────────────────────────────────────────────────────


def _member_type(member) -> str:
    """Return 'group' for UserGroup instances, 'user' otherwise."""
    return "group" if isinstance(member, UserGroup) else "user"


def _member_to_dict(member) -> dict:
    """Normalise a member object to a minimal standard shape."""
    return {
        "id": getattr(member, "id", ""),
        "name": getattr(member, "name", ""),
        "type": _member_type(member),
    }


def _list_direct_members(group) -> list[dict]:
    """
    Return the direct members of *group* as normalised dicts.

    Falls back to an empty list on any API error.
    """
    try:
        raw = group.members or []
        return [_member_to_dict(m) for m in raw]
    except Exception as exc:
        logger.warning(
            "Could not list members for group {name!r} ({id}): {exc}",
            name=getattr(group, "name", "?"),
            id=getattr(group, "id", "?"),
            exc=exc,
        )
        return []


def _resolve_members_recursive(group, conn, visited: set | None = None) -> list[dict]:
    """
    Recursively expand subgroups and return the effective set of users.

    Tracks visited group IDs to prevent infinite cycles.  Only *user*-type
    members are included in the result; subgroups themselves are not returned.

    Args:
        group:   A UserGroup object (must have .members populated).
        conn:    An active mstrio-py Connection (used to fetch subgroup members).
        visited: Set of group IDs already visited (populated internally).

    Returns:
        List of dicts with keys ``id``, ``name``, ``type`` (always "user").
    """
    if visited is None:
        visited = set()

    group_id = getattr(group, "id", None)
    if group_id in visited:
        return []
    visited.add(group_id)

    users: list[dict] = []
    members = _list_direct_members(group)

    for m in members:
        if m["type"] == "user":
            users.append(m)
        elif m["type"] == "group":
            try:
                sub = UserGroup(conn, id=m["id"])
                users.extend(_resolve_members_recursive(sub, conn, visited))
            except Exception as exc:
                logger.warning(
                    "Could not fetch subgroup {id} while resolving {parent!r}: {exc}",
                    id=m["id"],
                    parent=getattr(group, "name", "?"),
                    exc=exc,
                )

    return users


def _deduplicate(users: list[dict]) -> list[dict]:
    """Deduplicate users by ID, preserving first-seen order."""
    seen: set = set()
    result: list[dict] = []
    for u in users:
        uid = u.get("id")
        if uid and uid not in seen:
            seen.add(uid)
            result.append(u)
    return result


# ── Output helpers ─────────────────────────────────────────────────────────────


def _write_json(data: list | dict, path: Path) -> None:
    """Write *data* to *path* as pretty-printed JSON (UTF-8, no BOM)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    logger.success("Written → {p}", p=path)


def _out_path(output_dir: Path, filename: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / filename


# ── Subcommand: audit ─────────────────────────────────────────────────────────


def cmd_audit(
    env: str,
    fmt: str = "csv",
    output_dir: Path | None = None,
) -> None:
    """
    Identify user groups with no direct members and/or directly-assigned
    privileges.  Writes two separate output files.

    Args:
        env:        Environment to connect to ("dev", "qa", or "prod").
        fmt:        Output format: "csv" or "json".
        output_dir: Directory for output files.
    """
    config = MstrConfig(environment=MstrEnvironment(env))
    out = output_dir or config.output_dir
    Path(out).mkdir(parents=True, exist_ok=True)

    conn = get_mstrio_connection(config=config)
    try:
        groups = list_user_groups(conn)
        logger.info("Retrieved {n} group(s) from {env}.", n=len(groups), env=env)

        empty_rows: list = []
        priv_rows: list = []

        for g in groups:
            members = _list_direct_members(g)
            privs = _list_direct_privileges(g)
            desc = getattr(g, "description", "") or ""

            if not members:
                if fmt == "csv":
                    empty_rows.append([g.id, g.name, desc])
                else:
                    empty_rows.append({"id": g.id, "name": g.name, "description": desc})

            if privs:
                priv_names = "; ".join(p["name"] for p in privs)
                if fmt == "csv":
                    priv_rows.append([
                        g.id, g.name, desc,
                        str(len(privs)), priv_names,
                    ])
                else:
                    priv_rows.append({
                        "id": g.id,
                        "name": g.name,
                        "description": desc,
                        "privilege_count": len(privs),
                        "privileges": privs,
                    })

        # ── Empty groups ──────────────────────────────────────────────────────
        empty_path = _out_path(Path(out), f"user_groups_audit_empty.{fmt}")
        if empty_rows:
            if fmt == "csv":
                write_csv(empty_rows, columns=_AUDIT_EMPTY_COLS, path=empty_path)
            else:
                _write_json(empty_rows, empty_path)
        else:
            logger.info("No empty groups found.")

        # ── Privileged groups ─────────────────────────────────────────────────
        priv_path = _out_path(Path(out), f"user_groups_audit_privileged.{fmt}")
        if priv_rows:
            if fmt == "csv":
                write_csv(priv_rows, columns=_AUDIT_PRIV_COLS, path=priv_path)
            else:
                _write_json(priv_rows, priv_path)
        else:
            logger.info("No groups with directly-assigned privileges found.")

        logger.success(
            "Audit complete: {ne} empty group(s), {np} group(s) with direct privileges.",
            ne=len(empty_rows),
            np=len(priv_rows),
        )

    finally:
        conn.close()


# ── Subcommand: export ────────────────────────────────────────────────────────


def cmd_export(
    env: str,
    fmt: str = "json",
    output_dir: Path | None = None,
) -> None:
    """
    Export all groups with their direct members and direct privileges.

    Args:
        env:        Environment to connect to ("dev", "qa", or "prod").
        fmt:        Output format: "csv" or "json".
        output_dir: Directory for output files.
    """
    config = MstrConfig(environment=MstrEnvironment(env))
    out = output_dir or config.output_dir
    Path(out).mkdir(parents=True, exist_ok=True)

    conn = get_mstrio_connection(config=config)
    try:
        groups = list_user_groups(conn)
        logger.info("Retrieved {n} group(s) from {env}.", n=len(groups), env=env)

        rows_csv: list = []
        rows_json: list = []

        for g in groups:
            members = _list_direct_members(g)
            privs = _list_direct_privileges(g)
            desc = getattr(g, "description", "") or ""

            if fmt == "csv":
                rows_csv.append([
                    g.id,
                    g.name,
                    desc,
                    str(len(members)),
                    json.dumps([{"id": m["id"], "name": m["name"], "type": m["type"]}
                                for m in members]),
                    str(len(privs)),
                    json.dumps(privs),
                ])
            else:
                rows_json.append({
                    "id": g.id,
                    "name": g.name,
                    "description": desc,
                    "member_count": len(members),
                    "members": members,
                    "privilege_count": len(privs),
                    "privileges": privs,
                })

        out_path = _out_path(Path(out), f"user_groups_export.{fmt}")
        if fmt == "csv":
            write_csv(rows_csv, columns=_EXPORT_CSV_COLS, path=out_path)
        else:
            _write_json(rows_json, out_path)

        logger.success("Exported {n} group(s).", n=len(groups))

    finally:
        conn.close()


# ── Subcommand: privileges ────────────────────────────────────────────────────


def cmd_privileges(
    env: str,
    fmt: str = "json",
    output_dir: Path | None = None,
) -> None:
    """
    List the privileges directly assigned to each group (inherited excluded).

    Args:
        env:        Environment to connect to ("dev", "qa", or "prod").
        fmt:        Output format: "csv" or "json".
        output_dir: Directory for output files.
    """
    config = MstrConfig(environment=MstrEnvironment(env))
    out = output_dir or config.output_dir
    Path(out).mkdir(parents=True, exist_ok=True)

    conn = get_mstrio_connection(config=config)
    try:
        groups = list_user_groups(conn)
        logger.info("Retrieved {n} group(s) from {env}.", n=len(groups), env=env)

        rows_csv: list = []
        rows_json: list = []
        total_privs = 0

        for g in groups:
            privs = _list_direct_privileges(g)
            if not privs:
                continue

            desc = getattr(g, "description", "") or ""
            total_privs += len(privs)

            if fmt == "csv":
                for p in privs:
                    rows_csv.append([
                        g.id, g.name, desc,
                        p["id"], p["name"], p["type"],
                    ])
            else:
                rows_json.append({
                    "id": g.id,
                    "name": g.name,
                    "description": desc,
                    "privilege_count": len(privs),
                    "privileges": privs,
                })

        out_path = _out_path(Path(out), f"user_groups_privileges.{fmt}")

        if fmt == "csv":
            if rows_csv:
                write_csv(rows_csv, columns=_PRIV_CSV_COLS, path=out_path)
            else:
                logger.info("No groups with directly-assigned privileges found.")
        else:
            if rows_json:
                _write_json(rows_json, out_path)
            else:
                logger.info("No groups with directly-assigned privileges found.")

        logger.success(
            "Privileges: {ng} group(s) with direct privileges, {np} total privilege assignment(s).",
            ng=len(rows_json) if fmt == "json" else len({r[0] for r in rows_csv}),
            np=total_privs,
        )

    finally:
        conn.close()


# ── Subcommand: members ───────────────────────────────────────────────────────


def cmd_members(
    env: str,
    fmt: str = "json",
    resolve: bool = False,
    output_dir: Path | None = None,
) -> None:
    """
    List the members of each group.

    Args:
        env:        Environment to connect to ("dev", "qa", or "prod").
        fmt:        Output format: "csv" or "json".
        resolve:    When True, recursively expand subgroups and return only
                    the effective set of users (deduplicated).
        output_dir: Directory for output files.
    """
    config = MstrConfig(environment=MstrEnvironment(env))
    out = output_dir or config.output_dir
    Path(out).mkdir(parents=True, exist_ok=True)

    conn = get_mstrio_connection(config=config)
    try:
        groups = list_user_groups(conn)
        logger.info("Retrieved {n} group(s) from {env}.", n=len(groups), env=env)

        if resolve:
            logger.info("Resolving effective (recursive) user membership ...")

        rows_csv: list = []
        rows_json: list = []

        for g in groups:
            if resolve:
                members = _deduplicate(_resolve_members_recursive(g, conn))
                logger.debug(
                    "  {name}: {n} effective user(s)", name=g.name, n=len(members)
                )
            else:
                members = _list_direct_members(g)

            if fmt == "csv":
                if resolve:
                    for u in members:
                        rows_csv.append([g.id, g.name, u["id"], u["name"]])
                else:
                    for m in members:
                        rows_csv.append([
                            g.id, g.name, m["id"], m["name"], m["type"],
                        ])
            else:
                rows_json.append({
                    "id": g.id,
                    "name": g.name,
                    "member_count": len(members),
                    "members": members,
                })

        suffix = "resolved" if resolve else "direct"
        out_path = _out_path(Path(out), f"user_groups_members_{suffix}.{fmt}")

        columns = _RESOLVED_CSV_COLS if resolve else _MEMBERS_CSV_COLS

        if fmt == "csv":
            if rows_csv:
                write_csv(rows_csv, columns=columns, path=out_path)
            else:
                logger.info("No members found.")
        else:
            _write_json(rows_json, out_path)

        logger.success(
            "Members ({mode}): {n} group(s) processed.",
            mode="resolved" if resolve else "direct",
            n=len(groups),
        )

    finally:
        conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────────


def _add_common_args(sub: argparse.ArgumentParser, default_fmt: str = "json") -> None:
    """Add --format and --output-dir arguments shared by all subcommands."""
    sub.add_argument(
        "--format",
        dest="fmt",
        choices=["csv", "json"],
        default=default_fmt,
        metavar="csv|json",
        help=f"Output format (default: {default_fmt}).",
    )
    sub.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        metavar="PATH",
        help="Output directory (default: MSTR_OUTPUT_DIR or c:/tmp).",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Audit, export, and document MicroStrategy user groups.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # ── audit ─────────────────────────────────────────────────────────────────
    p_audit = subparsers.add_parser(
        "audit",
        help=(
            "Identify empty groups (no direct members) and groups with "
            "directly-assigned privileges."
        ),
    )
    p_audit.add_argument("env", choices=ENVS, help="Environment to audit.")
    _add_common_args(p_audit, default_fmt="csv")

    # ── export ────────────────────────────────────────────────────────────────
    p_export = subparsers.add_parser(
        "export",
        help="Export all groups with their members and directly-assigned privileges.",
    )
    p_export.add_argument("env", choices=ENVS, help="Environment to export.")
    _add_common_args(p_export, default_fmt="json")

    # ── privileges ────────────────────────────────────────────────────────────
    p_priv = subparsers.add_parser(
        "privileges",
        help="List privileges directly assigned to each group (inherited excluded).",
    )
    p_priv.add_argument("env", choices=ENVS, help="Environment to query.")
    _add_common_args(p_priv, default_fmt="json")

    # ── members ───────────────────────────────────────────────────────────────
    p_members = subparsers.add_parser(
        "members",
        help="List direct members of each group.  Use --resolve for effective users.",
    )
    p_members.add_argument("env", choices=ENVS, help="Environment to query.")
    p_members.add_argument(
        "--resolve",
        action="store_true",
        default=False,
        help=(
            "Recursively expand subgroups and return the effective (deduplicated) "
            "set of users.  Without this flag only direct members are listed."
        ),
    )
    _add_common_args(p_members, default_fmt="json")

    # ── Dispatch ──────────────────────────────────────────────────────────────
    args = parser.parse_args()

    if args.subcommand == "audit":
        cmd_audit(env=args.env, fmt=args.fmt, output_dir=args.output_dir)
    elif args.subcommand == "export":
        cmd_export(env=args.env, fmt=args.fmt, output_dir=args.output_dir)
    elif args.subcommand == "privileges":
        cmd_privileges(env=args.env, fmt=args.fmt, output_dir=args.output_dir)
    elif args.subcommand == "members":
        cmd_members(
            env=args.env,
            fmt=args.fmt,
            resolve=args.resolve,
            output_dir=args.output_dir,
        )
