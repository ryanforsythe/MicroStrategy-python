"""
SecurityRoles.py — Export and compare MicroStrategy security role privileges.

Subcommands
───────────
  export   — Show privileges for a security role.  By default lists only
             enabled privileges; pass --all to list every privilege with an
             enabled/disabled indicator.

  compare  — Compare privileges between two security roles (same or different
             environments).  By default shows only differences; pass --all to
             show every privilege with match/mismatch indicators.

Usage
─────
  python SecurityRoles.py export  <env> <role>
                                  [--all] [--format csv|json] [--output-dir PATH]

  python SecurityRoles.py compare <env> <role> <env2> <role2>
                                  [--all] [--format csv|json] [--output-dir PATH]

Examples
────────
  # List enabled privileges for "Normal Users" on dev
  python SecurityRoles.py export dev "Normal Users"

  # List ALL privileges (enabled + disabled) for "Normal Users" on dev
  python SecurityRoles.py export dev "Normal Users" --all

  # Compare "Normal Users" on dev vs qa — show only differences
  python SecurityRoles.py compare dev "Normal Users" qa "Normal Users"

  # Compare two different roles on prod — full view with match status
  python SecurityRoles.py compare prod "Normal Users" prod "Power Users" --all

  # JSON output
  python SecurityRoles.py export dev "Normal Users" --all --format json
"""

import argparse
import json as _json
from pathlib import Path

from loguru import logger
from mstrio.access_and_security.privilege import Privilege
from mstrio.access_and_security.security_role import (
    SecurityRole,
    list_security_roles,
)

from mstrio_core import MstrConfig, get_mstrio_connection, write_csv
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]

_EXPORT_CSV_COLS = [
    "role_id",
    "role_name",
    "priv_id",
    "priv_name",
    "priv_description",
    "priv_category",
    "is_project_level",
    "enabled",
]

_COMPARE_CSV_COLS = [
    "priv_id",
    "priv_name",
    "priv_category",
    "is_project_level",
    "source_role_id",
    "source_role_name",
    "source_env",
    "source_enabled",
    "target_role_id",
    "target_role_name",
    "target_env",
    "target_enabled",
    "match",
]

# ── Internal helpers ──────────────────────────────────────────────────────────


def _make_config(env: str) -> MstrConfig:
    """Build a MstrConfig pinned to a specific environment."""
    return MstrConfig(environment=MstrEnvironment(env))


def _out_dir(config: MstrConfig, output_dir: Path | None) -> Path:
    d = output_dir or config.output_dir
    d.mkdir(parents=True, exist_ok=True)
    return d


def _resolve_role(conn, role_name: str) -> SecurityRole:
    """
    Look up a security role by name (case-insensitive).

    Raises ValueError if no role matches.
    """
    roles = list_security_roles(conn)
    for role in roles:
        if role.name.lower() == role_name.lower():
            return role
    available = [r.name for r in roles]
    raise ValueError(
        f"Security role {role_name!r} not found.  "
        f"Available roles: {available}"
    )


def _get_all_privileges(conn) -> list[dict]:
    """
    Return the master list of all privileges on the server.

    Each item: {id, name, description, categories, is_project_level_privilege}
    """
    privs = Privilege.list_privileges(conn, to_dictionary=True)
    logger.debug("Loaded {n} privileges from server", n=len(privs))
    return privs


def _get_role_privilege_ids(role: SecurityRole) -> set[int]:
    """
    Return the set of privilege IDs that are enabled (granted) on a role.

    SecurityRole.privileges is a list of dicts: [{'id': ..., 'name': ...}, ...]
    SecurityRole.list_privileges() returns {id_int: name, ...}.
    """
    priv_map = role.list_privileges(to_dataframe=False)
    if isinstance(priv_map, dict):
        return set(priv_map.keys())
    # Fallback: extract from the raw privileges list
    raw = role.privileges or []
    ids = set()
    for p in raw:
        pid = p.get("id") if isinstance(p, dict) else getattr(p, "id", None)
        if pid is not None:
            try:
                ids.add(int(pid))
            except (ValueError, TypeError):
                ids.add(pid)
    return ids


def _build_export_rows(
    role: SecurityRole,
    all_privs: list[dict],
    enabled_ids: set,
    show_all: bool,
) -> list[dict]:
    """
    Build output rows for the export subcommand.

    Args:
        role:        The resolved SecurityRole.
        all_privs:   Master privilege list from the server.
        enabled_ids: Set of privilege IDs enabled on this role.
        show_all:    True → every privilege; False → enabled only.
    """
    rows = []
    for p in all_privs:
        pid = p.get("id")
        try:
            pid_int = int(pid)
        except (ValueError, TypeError):
            pid_int = pid
        enabled = pid_int in enabled_ids

        if not show_all and not enabled:
            continue

        rows.append(
            {
                "role_id": role.id,
                "role_name": role.name,
                "priv_id": pid,
                "priv_name": p.get("name", ""),
                "priv_description": p.get("description", ""),
                "priv_category": _format_categories(p.get("categories")),
                "is_project_level": p.get("is_project_level_privilege", ""),
                "enabled": enabled,
            }
        )
    rows.sort(key=lambda r: (not r["enabled"], r["priv_name"].lower()))
    return rows


def _build_compare_rows(
    src_role: SecurityRole,
    tgt_role: SecurityRole,
    src_env: str,
    tgt_env: str,
    all_privs: list[dict],
    src_enabled: set,
    tgt_enabled: set,
    show_all: bool,
) -> list[dict]:
    """
    Build output rows for the compare subcommand.

    Args:
        src_role / tgt_role:   Resolved SecurityRole objects.
        src_env / tgt_env:     Environment labels.
        all_privs:             Master privilege list (superset from either env).
        src_enabled/tgt_enabled: Enabled privilege IDs per role.
        show_all:              True → all privileges; False → differences only.
    """
    rows = []
    for p in all_privs:
        pid = p.get("id")
        try:
            pid_int = int(pid)
        except (ValueError, TypeError):
            pid_int = pid

        s_on = pid_int in src_enabled
        t_on = pid_int in tgt_enabled
        match = s_on == t_on

        if not show_all and match:
            continue

        rows.append(
            {
                "priv_id": pid,
                "priv_name": p.get("name", ""),
                "priv_category": _format_categories(p.get("categories")),
                "is_project_level": p.get("is_project_level_privilege", ""),
                "source_role_id": src_role.id,
                "source_role_name": src_role.name,
                "source_env": src_env,
                "source_enabled": s_on,
                "target_role_id": tgt_role.id,
                "target_role_name": tgt_role.name,
                "target_env": tgt_env,
                "target_enabled": t_on,
                "match": match,
            }
        )
    rows.sort(key=lambda r: (r["match"], r["priv_name"].lower()))
    return rows


def _format_categories(cats) -> str:
    """Flatten categories to a readable string."""
    if cats is None:
        return ""
    if isinstance(cats, list):
        return ", ".join(str(c) for c in cats)
    return str(cats)


def _merge_privilege_lists(*priv_lists: list[dict]) -> list[dict]:
    """
    Merge multiple privilege lists into a deduplicated superset, keyed by ID.

    Needed when comparing across environments that may have different privilege
    sets (e.g. different I-Server versions).
    """
    seen: dict[str, dict] = {}
    for plist in priv_lists:
        for p in plist:
            pid = str(p.get("id", ""))
            if pid not in seen:
                seen[pid] = p
    return list(seen.values())


# ── Operations ────────────────────────────────────────────────────────────────


def export_role(
    env: str,
    role_name: str,
    show_all: bool = False,
    fmt: str = "csv",
    output_dir: Path | None = None,
) -> None:
    """
    Export the privileges of a security role.

    Args:
        env:        Environment to connect to.
        role_name:  Name of the security role.
        show_all:   True → every privilege with enabled flag;
                    False → enabled privileges only.
        fmt:        Output format: "csv" or "json".
        output_dir: Output directory (default: MstrConfig.output_dir).
    """
    config = _make_config(env)
    conn = get_mstrio_connection(config=config)
    try:
        # ── Resolve role ──────────────────────────────────────────────────
        logger.info(
            "Looking up security role {role!r} on {env} ...",
            role=role_name,
            env=env,
        )
        role = _resolve_role(conn, role_name)
        logger.info(
            "Found role: {name}  (ID: {id})",
            name=role.name,
            id=role.id,
        )

        # ── Privileges ────────────────────────────────────────────────────
        all_privs = _get_all_privileges(conn)
        enabled_ids = _get_role_privilege_ids(role)
        logger.info(
            "Role {name!r} has {n}/{total} privileges enabled",
            name=role.name,
            n=len(enabled_ids),
            total=len(all_privs),
        )

        rows = _build_export_rows(role, all_privs, enabled_ids, show_all)

        if not rows:
            logger.warning("No privileges to report.")
            return

        # ── Write output ──────────────────────────────────────────────────
        safe_name = role.name.replace(" ", "_").replace("/", "-")
        suffix = "all" if show_all else "enabled"
        out = _out_dir(config, output_dir)

        if fmt == "csv":
            path = out / f"security_role_{safe_name}_{env}_{suffix}.csv"
            write_csv(rows, columns=_EXPORT_CSV_COLS, path=path)
        elif fmt == "json":
            path = out / f"security_role_{safe_name}_{env}_{suffix}.json"
            path.write_text(_json.dumps(rows, indent=2, default=str), encoding="utf-8")
        else:
            raise ValueError(f"Unsupported format {fmt!r}. Use 'csv' or 'json'.")

        logger.success(
            "Exported {n} privileges → {path}",
            n=len(rows),
            path=path,
        )
    finally:
        conn.close()


def compare_roles(
    src_env: str,
    src_role_name: str,
    tgt_env: str,
    tgt_role_name: str,
    show_all: bool = False,
    fmt: str = "csv",
    output_dir: Path | None = None,
) -> None:
    """
    Compare privileges between two security roles.

    Roles can be on the same or different environments.

    Args:
        src_env / tgt_env:           Environment labels.
        src_role_name/tgt_role_name: Role names to compare.
        show_all:                    True → all privileges with match flag;
                                     False → differences only.
        fmt:                         Output format: "csv" or "json".
        output_dir:                  Output directory.
    """
    same_env = src_env == tgt_env

    # ── Source ─────────────────────────────────────────────────────────────
    src_config = _make_config(src_env)
    src_conn = get_mstrio_connection(config=src_config)
    try:
        logger.info(
            "Looking up source role {role!r} on {env} ...",
            role=src_role_name,
            env=src_env,
        )
        src_role = _resolve_role(src_conn, src_role_name)
        logger.info(
            "Source: {name}  (ID: {id})",
            name=src_role.name,
            id=src_role.id,
        )
        src_privs = _get_all_privileges(src_conn)
        src_enabled = _get_role_privilege_ids(src_role)

        # If same environment, resolve target on the same connection
        if same_env:
            logger.info(
                "Looking up target role {role!r} on {env} ...",
                role=tgt_role_name,
                env=tgt_env,
            )
            tgt_role = _resolve_role(src_conn, tgt_role_name)
            logger.info(
                "Target: {name}  (ID: {id})",
                name=tgt_role.name,
                id=tgt_role.id,
            )
            tgt_privs = src_privs  # same server → same privilege catalog
            tgt_enabled = _get_role_privilege_ids(tgt_role)
    finally:
        src_conn.close()

    # ── Target (cross-environment only) ───────────────────────────────────
    if not same_env:
        tgt_config = _make_config(tgt_env)
        tgt_conn = get_mstrio_connection(config=tgt_config)
        try:
            logger.info(
                "Looking up target role {role!r} on {env} ...",
                role=tgt_role_name,
                env=tgt_env,
            )
            tgt_role = _resolve_role(tgt_conn, tgt_role_name)
            logger.info(
                "Target: {name}  (ID: {id})",
                name=tgt_role.name,
                id=tgt_role.id,
            )
            tgt_privs = _get_all_privileges(tgt_conn)
            tgt_enabled = _get_role_privilege_ids(tgt_role)
        finally:
            tgt_conn.close()

    # ── Merge privilege catalogs & build rows ─────────────────────────────
    all_privs = _merge_privilege_lists(src_privs, tgt_privs)

    logger.info(
        "Source {src_role!r} ({src_env}): {sn}/{total} enabled  |  "
        "Target {tgt_role!r} ({tgt_env}): {tn}/{total} enabled",
        src_role=src_role.name,
        src_env=src_env,
        sn=len(src_enabled),
        tgt_role=tgt_role.name,
        tgt_env=tgt_env,
        tn=len(tgt_enabled),
        total=len(all_privs),
    )

    rows = _build_compare_rows(
        src_role, tgt_role, src_env, tgt_env,
        all_privs, src_enabled, tgt_enabled, show_all,
    )

    if not rows:
        logger.info("No differences found — roles have identical privileges.")
        return

    # ── Summarise ─────────────────────────────────────────────────────────
    n_diff = sum(1 for r in rows if not r["match"])
    n_match = sum(1 for r in rows if r["match"])
    logger.info(
        "{diff} difference(s), {match} match(es) across {total} privileges",
        diff=n_diff,
        match=n_match,
        total=len(all_privs),
    )

    # ── Write output ──────────────────────────────────────────────────────
    src_safe = src_role.name.replace(" ", "_").replace("/", "-")
    tgt_safe = tgt_role.name.replace(" ", "_").replace("/", "-")
    suffix = "all" if show_all else "diff"
    out = _out_dir(src_config, output_dir)

    if fmt == "csv":
        path = out / f"security_role_compare_{src_safe}_{src_env}_vs_{tgt_safe}_{tgt_env}_{suffix}.csv"
        write_csv(rows, columns=_COMPARE_CSV_COLS, path=path)
    elif fmt == "json":
        path = out / f"security_role_compare_{src_safe}_{src_env}_vs_{tgt_safe}_{tgt_env}_{suffix}.json"
        path.write_text(_json.dumps(rows, indent=2, default=str), encoding="utf-8")
    else:
        raise ValueError(f"Unsupported format {fmt!r}. Use 'csv' or 'json'.")

    logger.success(
        "Comparison ({n} rows) written → {path}",
        n=len(rows),
        path=path,
    )


# ── CLI ───────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export and compare MicroStrategy security role privileges.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            '  python SecurityRoles.py export dev "Normal Users"\n'
            '  python SecurityRoles.py export dev "Normal Users" --all\n'
            '  python SecurityRoles.py export dev "Normal Users" --all --format json\n'
            '  python SecurityRoles.py compare dev "Normal Users" qa "Normal Users"\n'
            '  python SecurityRoles.py compare prod "Normal Users" prod "Power Users" --all\n'
        ),
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # ── export ────────────────────────────────────────────────────────────
    exp = sub.add_parser(
        "export",
        help="Show privileges for a security role.",
    )
    exp.add_argument("env", choices=ENVS, help="Environment (dev, qa, prod).")
    exp.add_argument("role", help="Security role name (use quotes for spaces).")
    exp.add_argument(
        "--all",
        dest="show_all",
        action="store_true",
        default=False,
        help="List all privileges with enabled/disabled status (default: enabled only).",
    )
    exp.add_argument(
        "--format",
        choices=["csv", "json"],
        default="csv",
        help="Output format (default: csv).",
    )
    exp.add_argument("--output-dir", type=Path, default=None, metavar="PATH")

    # ── compare ───────────────────────────────────────────────────────────
    cmp = sub.add_parser(
        "compare",
        help="Compare privileges between two security roles.",
    )
    cmp.add_argument("env", choices=ENVS, help="Source environment.")
    cmp.add_argument("role", help="Source security role name.")
    cmp.add_argument("env2", choices=ENVS, help="Target environment.")
    cmp.add_argument("role2", help="Target security role name.")
    cmp.add_argument(
        "--all",
        dest="show_all",
        action="store_true",
        default=False,
        help="Show all privileges with match status (default: differences only).",
    )
    cmp.add_argument(
        "--format",
        choices=["csv", "json"],
        default="csv",
        help="Output format (default: csv).",
    )
    cmp.add_argument("--output-dir", type=Path, default=None, metavar="PATH")

    # ── Dispatch ──────────────────────────────────────────────────────────
    args = parser.parse_args()

    if args.command == "export":
        export_role(
            env=args.env,
            role_name=args.role,
            show_all=args.show_all,
            fmt=args.format,
            output_dir=args.output_dir,
        )
    elif args.command == "compare":
        compare_roles(
            src_env=args.env,
            src_role_name=args.role,
            tgt_env=args.env2,
            tgt_role_name=args.role2,
            show_all=args.show_all,
            fmt=args.format,
            output_dir=args.output_dir,
        )
