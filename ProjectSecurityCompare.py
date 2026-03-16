"""
ProjectSecurityCompare.py — Compare project-level security role assignments and
security filter assignments between two MicroStrategy projects.

Subcommands
───────────
  roles    — Compare which users/groups are assigned to which security roles
             in each project.  Shows members that are in one project but not
             the other, or that have a different role assignment.

  filters  — Compare which users/groups have which security filters applied
             in each project.  Shows filter+member pairs present in one project
             but not the other.

Usage
─────
  python ProjectSecurityCompare.py roles   <env> <project> <env2> <project2>
                                           [--format csv|json] [--output-dir PATH]

  python ProjectSecurityCompare.py filters <env> <project> <env2> <project2>
                                           [--format csv|json] [--output-dir PATH]

Examples
────────
  # Compare security role assignments between two projects on QA
  python ProjectSecurityCompare.py roles qa "Finance" qa "Finance UAT"

  # Compare security role assignments across environments
  python ProjectSecurityCompare.py roles dev "Analytics" prod "Analytics"

  # Compare security filter assignments between two projects on prod
  python ProjectSecurityCompare.py filters prod "Finance" prod "Finance UAT"

  # JSON output
  python ProjectSecurityCompare.py roles dev "Project A" qa "Project A" --format json
"""

import argparse
import json as _json
from pathlib import Path

from loguru import logger
from mstrio.access_and_security.security_role import (
    SecurityRole,
    list_security_roles,
)
from mstrio.modeling.security_filter import (
    SecurityFilter,
    list_security_filters,
)
from mstrio.server import Project
from mstrio.users_and_groups import User, UserGroup

from mstrio_core import MstrConfig, get_mstrio_connection, write_csv
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]

_ROLES_CSV_COLS = [
    "member_id",
    "member_name",
    "member_type",
    "source_role_name",
    "source_role_id",
    "target_role_name",
    "target_role_id",
    "source_project",
    "source_env",
    "target_project",
    "target_env",
    "status",
]

_FILTERS_CSV_COLS = [
    "filter_name",
    "filter_id",
    "member_id",
    "member_name",
    "member_type",
    "source_project",
    "source_env",
    "target_project",
    "target_env",
    "status",
]


# ── Internal helpers ──────────────────────────────────────────────────────────


def _make_config(env: str) -> MstrConfig:
    return MstrConfig(environment=MstrEnvironment(env))


def _out_dir(config: MstrConfig, output_dir: Path | None) -> Path:
    d = output_dir or config.output_dir
    d.mkdir(parents=True, exist_ok=True)
    return d


def _dicts_to_rows(dicts: list[dict], columns: list[str]) -> list[list]:
    return [[d.get(c, "") for c in columns] for d in dicts]


def _member_type(member) -> str:
    """Return 'User' or 'UserGroup' for a parsed member object."""
    if isinstance(member, User):
        return "User"
    if isinstance(member, UserGroup):
        return "UserGroup"
    # Fallback — check subtype attribute
    st = getattr(member, "subtype", None)
    if st is not None:
        st_val = st if isinstance(st, int) else getattr(st, "value", st)
        # subtype 8704 = User, 8705 = UserGroup
        if st_val == 8705:
            return "UserGroup"
    return "User"


def _member_name(member) -> str:
    """Best-effort display name."""
    return getattr(member, "name", None) or getattr(member, "username", "") or str(member.id)


def _resolve_project_name(conn, project_name: str) -> str:
    """Resolve and validate a project name.  Returns the canonical name."""
    try:
        proj = Project(conn, name=project_name)
        return proj.name
    except Exception as exc:
        raise ValueError(
            f"Project {project_name!r} not found: {exc}"
        ) from exc


# ── Security Role comparison ─────────────────────────────────────────────────


def _build_role_map(conn, project_name: str) -> dict[str, dict]:
    """
    Build a mapping of member_id → {member_name, member_type, role_name, role_id}
    for every security role in the given project.

    When a member appears in multiple roles, the *last* role encountered wins.
    This mirrors MicroStrategy behaviour where a principal has one effective
    project-level role.
    """
    roles = list_security_roles(conn)
    logger.info(
        "Fetched {n} security roles from server",
        n=len(roles),
    )

    member_map: dict[str, dict] = {}
    for role in roles:
        try:
            members = role.list_members(project_name=project_name)
        except Exception:
            # Role may have no members in this project
            continue

        if not members:
            continue

        logger.debug(
            "Role {role!r}: {n} member(s) in project {proj!r}",
            role=role.name,
            n=len(members),
            proj=project_name,
        )

        for m in members:
            mid = m.id
            member_map[mid] = {
                "member_name": _member_name(m),
                "member_type": _member_type(m),
                "role_name": role.name,
                "role_id": role.id,
            }

    logger.info(
        "Project {proj!r}: {n} member(s) across all roles",
        proj=project_name,
        n=len(member_map),
    )
    return member_map


def _compare_role_maps(
    src_map: dict,
    tgt_map: dict,
    src_project: str,
    src_env: str,
    tgt_project: str,
    tgt_env: str,
) -> list[dict]:
    """
    Diff two role maps.  Returns rows for members that differ.

    Statuses:
      source_only  — member assigned a role in source but not target
      target_only  — member assigned a role in target but not source
      role_differs — member in both but with different security roles
    """
    all_ids = set(src_map.keys()) | set(tgt_map.keys())
    rows = []

    for mid in sorted(all_ids):
        s = src_map.get(mid)
        t = tgt_map.get(mid)

        if s and not t:
            rows.append(
                {
                    "member_id": mid,
                    "member_name": s["member_name"],
                    "member_type": s["member_type"],
                    "source_role_name": s["role_name"],
                    "source_role_id": s["role_id"],
                    "target_role_name": "",
                    "target_role_id": "",
                    "source_project": src_project,
                    "source_env": src_env,
                    "target_project": tgt_project,
                    "target_env": tgt_env,
                    "status": "source_only",
                }
            )
        elif t and not s:
            rows.append(
                {
                    "member_id": mid,
                    "member_name": t["member_name"],
                    "member_type": t["member_type"],
                    "source_role_name": "",
                    "source_role_id": "",
                    "target_role_name": t["role_name"],
                    "target_role_id": t["role_id"],
                    "source_project": src_project,
                    "source_env": src_env,
                    "target_project": tgt_project,
                    "target_env": tgt_env,
                    "status": "target_only",
                }
            )
        elif s["role_id"] != t["role_id"]:
            rows.append(
                {
                    "member_id": mid,
                    "member_name": s["member_name"],
                    "member_type": s["member_type"],
                    "source_role_name": s["role_name"],
                    "source_role_id": s["role_id"],
                    "target_role_name": t["role_name"],
                    "target_role_id": t["role_id"],
                    "source_project": src_project,
                    "source_env": src_env,
                    "target_project": tgt_project,
                    "target_env": tgt_env,
                    "status": "role_differs",
                }
            )

    rows.sort(key=lambda r: (r["status"], r["member_name"].lower()))
    return rows


# ── Security Filter comparison ───────────────────────────────────────────────


def _build_filter_map(conn, project_name: str) -> dict[tuple[str, str], dict]:
    """
    Build a mapping of (filter_name, member_id) → {filter_id, member_name, member_type}
    for every security filter in the given project.
    """
    filters = list_security_filters(conn, project_name=project_name)
    logger.info(
        "Fetched {n} security filters from project {proj!r}",
        n=len(filters),
        proj=project_name,
    )

    fmap: dict[tuple[str, str], dict] = {}
    for sf in filters:
        try:
            members = sf.members or []
        except Exception:
            logger.warning(
                "Could not read members for filter {f!r}",
                f=sf.name,
            )
            continue

        logger.debug(
            "Filter {f!r}: {n} member(s)",
            f=sf.name,
            n=len(members),
        )

        for m in members:
            mid = m.get("id") if isinstance(m, dict) else getattr(m, "id", None)
            if mid is None:
                continue
            mname = (
                m.get("name") if isinstance(m, dict) else getattr(m, "name", "")
            ) or ""
            mtype = _filter_member_type(m)

            fmap[(sf.name, mid)] = {
                "filter_id": sf.id,
                "member_name": mname,
                "member_type": mtype,
            }

    logger.info(
        "Project {proj!r}: {n} filter-member assignment(s)",
        proj=project_name,
        n=len(fmap),
    )
    return fmap


def _filter_member_type(m) -> str:
    """Determine member type from a filter member (dict or object)."""
    if isinstance(m, User):
        return "User"
    if isinstance(m, UserGroup):
        return "UserGroup"
    if isinstance(m, dict):
        # Check type/subType fields
        t = m.get("type") or m.get("subType") or m.get("subtype")
        if t is not None:
            t_val = int(t) if isinstance(t, (int, str)) and str(t).isdigit() else t
            if t_val in (34, 8704):
                return "User"
            if t_val in (34, 8705):
                return "UserGroup"
        # Fallback: presence of 'username' field suggests User
        if "username" in m:
            return "User"
    return "User"


def _compare_filter_maps(
    src_map: dict[tuple[str, str], dict],
    tgt_map: dict[tuple[str, str], dict],
    src_project: str,
    src_env: str,
    tgt_project: str,
    tgt_env: str,
) -> list[dict]:
    """
    Diff two filter maps.  Matches by (filter_name, member_id).

    Statuses:
      source_only — filter+member assignment exists in source only
      target_only — filter+member assignment exists in target only
    """
    all_keys = set(src_map.keys()) | set(tgt_map.keys())
    rows = []

    for key in sorted(all_keys):
        fname, mid = key
        s = src_map.get(key)
        t = tgt_map.get(key)

        if s and not t:
            rows.append(
                {
                    "filter_name": fname,
                    "filter_id": s["filter_id"],
                    "member_id": mid,
                    "member_name": s["member_name"],
                    "member_type": s["member_type"],
                    "source_project": src_project,
                    "source_env": src_env,
                    "target_project": tgt_project,
                    "target_env": tgt_env,
                    "status": "source_only",
                }
            )
        elif t and not s:
            rows.append(
                {
                    "filter_name": fname,
                    "filter_id": t["filter_id"],
                    "member_id": mid,
                    "member_name": t["member_name"],
                    "member_type": t["member_type"],
                    "source_project": src_project,
                    "source_env": src_env,
                    "target_project": tgt_project,
                    "target_env": tgt_env,
                    "status": "target_only",
                }
            )
        # If both exist with the same key, they match — no row needed

    rows.sort(key=lambda r: (r["status"], r["filter_name"].lower(), r["member_name"].lower()))
    return rows


# ── Operations ────────────────────────────────────────────────────────────────


def compare_roles(
    src_env: str,
    src_project: str,
    tgt_env: str,
    tgt_project: str,
    fmt: str = "csv",
    output_dir: Path | None = None,
) -> None:
    """Compare security role assignments between two projects."""
    same_env = src_env == tgt_env

    # ── Source ─────────────────────────────────────────────────────────────
    src_config = _make_config(src_env)
    src_conn = get_mstrio_connection(config=src_config)
    try:
        src_project_name = _resolve_project_name(src_conn, src_project)
        logger.info(
            "Building role map for {proj!r} on {env} ...",
            proj=src_project_name,
            env=src_env,
        )
        src_map = _build_role_map(src_conn, src_project_name)

        if same_env:
            tgt_project_name = _resolve_project_name(src_conn, tgt_project)
            logger.info(
                "Building role map for {proj!r} on {env} ...",
                proj=tgt_project_name,
                env=tgt_env,
            )
            tgt_map = _build_role_map(src_conn, tgt_project_name)
    finally:
        src_conn.close()

    # ── Target (cross-environment) ────────────────────────────────────────
    if not same_env:
        tgt_config = _make_config(tgt_env)
        tgt_conn = get_mstrio_connection(config=tgt_config)
        try:
            tgt_project_name = _resolve_project_name(tgt_conn, tgt_project)
            logger.info(
                "Building role map for {proj!r} on {env} ...",
                proj=tgt_project_name,
                env=tgt_env,
            )
            tgt_map = _build_role_map(tgt_conn, tgt_project_name)
        finally:
            tgt_conn.close()

    # ── Compare & write ───────────────────────────────────────────────────
    rows = _compare_role_maps(
        src_map, tgt_map,
        src_project_name, src_env,
        tgt_project_name, tgt_env,
    )

    if not rows:
        logger.info(
            "No differences — security role assignments are identical "
            "between {src} ({src_env}) and {tgt} ({tgt_env}).",
            src=src_project_name,
            src_env=src_env,
            tgt=tgt_project_name,
            tgt_env=tgt_env,
        )
        return

    n_src = sum(1 for r in rows if r["status"] == "source_only")
    n_tgt = sum(1 for r in rows if r["status"] == "target_only")
    n_diff = sum(1 for r in rows if r["status"] == "role_differs")
    logger.info(
        "{total} difference(s): {src_only} source-only, {tgt_only} target-only, "
        "{role_diff} role-differs",
        total=len(rows),
        src_only=n_src,
        tgt_only=n_tgt,
        role_diff=n_diff,
    )

    out = _out_dir(src_config, output_dir)
    src_safe = src_project_name.replace(" ", "_")
    tgt_safe = tgt_project_name.replace(" ", "_")

    if fmt == "csv":
        path = out / f"project_roles_diff_{src_safe}_{src_env}_vs_{tgt_safe}_{tgt_env}.csv"
        write_csv(
            _dicts_to_rows(rows, _ROLES_CSV_COLS),
            columns=_ROLES_CSV_COLS,
            path=path,
        )
    elif fmt == "json":
        path = out / f"project_roles_diff_{src_safe}_{src_env}_vs_{tgt_safe}_{tgt_env}.json"
        path.write_text(_json.dumps(rows, indent=2, default=str), encoding="utf-8")
    else:
        raise ValueError(f"Unsupported format {fmt!r}. Use 'csv' or 'json'.")

    logger.success("Diff ({n} rows) written → {path}", n=len(rows), path=path)


def compare_filters(
    src_env: str,
    src_project: str,
    tgt_env: str,
    tgt_project: str,
    fmt: str = "csv",
    output_dir: Path | None = None,
) -> None:
    """Compare security filter assignments between two projects."""
    same_env = src_env == tgt_env

    # ── Source ─────────────────────────────────────────────────────────────
    src_config = _make_config(src_env)
    src_conn = get_mstrio_connection(config=src_config)
    try:
        src_project_name = _resolve_project_name(src_conn, src_project)
        logger.info(
            "Building filter map for {proj!r} on {env} ...",
            proj=src_project_name,
            env=src_env,
        )
        src_map = _build_filter_map(src_conn, src_project_name)

        if same_env:
            tgt_project_name = _resolve_project_name(src_conn, tgt_project)
            logger.info(
                "Building filter map for {proj!r} on {env} ...",
                proj=tgt_project_name,
                env=tgt_env,
            )
            tgt_map = _build_filter_map(src_conn, tgt_project_name)
    finally:
        src_conn.close()

    # ── Target (cross-environment) ────────────────────────────────────────
    if not same_env:
        tgt_config = _make_config(tgt_env)
        tgt_conn = get_mstrio_connection(config=tgt_config)
        try:
            tgt_project_name = _resolve_project_name(tgt_conn, tgt_project)
            logger.info(
                "Building filter map for {proj!r} on {env} ...",
                proj=tgt_project_name,
                env=tgt_env,
            )
            tgt_map = _build_filter_map(tgt_conn, tgt_project_name)
        finally:
            tgt_conn.close()

    # ── Compare & write ───────────────────────────────────────────────────
    rows = _compare_filter_maps(
        src_map, tgt_map,
        src_project_name, src_env,
        tgt_project_name, tgt_env,
    )

    if not rows:
        logger.info(
            "No differences — security filter assignments are identical "
            "between {src} ({src_env}) and {tgt} ({tgt_env}).",
            src=src_project_name,
            src_env=src_env,
            tgt=tgt_project_name,
            tgt_env=tgt_env,
        )
        return

    n_src = sum(1 for r in rows if r["status"] == "source_only")
    n_tgt = sum(1 for r in rows if r["status"] == "target_only")
    logger.info(
        "{total} difference(s): {src_only} source-only, {tgt_only} target-only",
        total=len(rows),
        src_only=n_src,
        tgt_only=n_tgt,
    )

    out = _out_dir(src_config, output_dir)
    src_safe = src_project_name.replace(" ", "_")
    tgt_safe = tgt_project_name.replace(" ", "_")

    if fmt == "csv":
        path = out / f"project_filters_diff_{src_safe}_{src_env}_vs_{tgt_safe}_{tgt_env}.csv"
        write_csv(
            _dicts_to_rows(rows, _FILTERS_CSV_COLS),
            columns=_FILTERS_CSV_COLS,
            path=path,
        )
    elif fmt == "json":
        path = out / f"project_filters_diff_{src_safe}_{src_env}_vs_{tgt_safe}_{tgt_env}.json"
        path.write_text(_json.dumps(rows, indent=2, default=str), encoding="utf-8")
    else:
        raise ValueError(f"Unsupported format {fmt!r}. Use 'csv' or 'json'.")

    logger.success("Diff ({n} rows) written → {path}", n=len(rows), path=path)


# ── CLI ───────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Compare project-level security role assignments and "
            "security filter assignments between two MicroStrategy projects."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            '  python ProjectSecurityCompare.py roles   qa "Finance" qa "Finance UAT"\n'
            '  python ProjectSecurityCompare.py roles   dev "Analytics" prod "Analytics"\n'
            '  python ProjectSecurityCompare.py filters prod "Finance" prod "Finance UAT"\n'
            '  python ProjectSecurityCompare.py filters dev "Project A" qa "Project A" --format json\n'
        ),
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # ── roles ─────────────────────────────────────────────────────────────
    r_parser = sub.add_parser(
        "roles",
        help="Compare security role assignments between two projects.",
    )
    r_parser.add_argument("env", choices=ENVS, help="Source environment.")
    r_parser.add_argument("project", help="Source project name.")
    r_parser.add_argument("env2", choices=ENVS, help="Target environment.")
    r_parser.add_argument("project2", help="Target project name.")
    r_parser.add_argument(
        "--format", choices=["csv", "json"], default="csv",
        help="Output format (default: csv).",
    )
    r_parser.add_argument("--output-dir", type=Path, default=None, metavar="PATH")

    # ── filters ───────────────────────────────────────────────────────────
    f_parser = sub.add_parser(
        "filters",
        help="Compare security filter assignments between two projects.",
    )
    f_parser.add_argument("env", choices=ENVS, help="Source environment.")
    f_parser.add_argument("project", help="Source project name.")
    f_parser.add_argument("env2", choices=ENVS, help="Target environment.")
    f_parser.add_argument("project2", help="Target project name.")
    f_parser.add_argument(
        "--format", choices=["csv", "json"], default="csv",
        help="Output format (default: csv).",
    )
    f_parser.add_argument("--output-dir", type=Path, default=None, metavar="PATH")

    # ── Dispatch ──────────────────────────────────────────────────────────
    args = parser.parse_args()

    if args.command == "roles":
        compare_roles(
            src_env=args.env,
            src_project=args.project,
            tgt_env=args.env2,
            tgt_project=args.project2,
            fmt=args.format,
            output_dir=args.output_dir,
        )
    elif args.command == "filters":
        compare_filters(
            src_env=args.env,
            src_project=args.project,
            tgt_env=args.env2,
            tgt_project=args.project2,
            fmt=args.format,
            output_dir=args.output_dir,
        )
