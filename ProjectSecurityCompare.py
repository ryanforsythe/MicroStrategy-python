"""
ProjectSecurityCompare.py — Compare project-level security role assignments and
security filter assignments between two MicroStrategy projects, then optionally
apply changes to the target.

Subcommands
───────────
  roles         — Compare which users/groups are assigned to which security
                  roles in each project.  Shows members in one project but not
                  the other, or with a different role assignment.  Outputs a CSV
                  with a target_action column (Apply / Remove).

  filters       — Compare which users/groups have which security filters in
                  each project.  Outputs a CSV with a target_action column.

  apply-roles   — Read a roles diff CSV (from the roles subcommand), then
                  grant or revoke security role assignments on the target
                  project according to the target_action column.

  apply-filters — Read a filters diff CSV (from the filters subcommand), then
                  apply or revoke security filter assignments on the target
                  project according to the target_action column.

Usage
─────
  # Single project pair (different project names)
  python ProjectSecurityCompare.py roles   <env> <env2> <project> [<project2>]
                                           [--format csv|json] [--output-dir PATH]

  python ProjectSecurityCompare.py filters <env> <env2> <project> [<project2>]
                                           [--format csv|json] [--output-dir PATH]

  # Batch — iterate over a project list file (one project name per line)
  python ProjectSecurityCompare.py roles   <env> <env2> --projects-file FILE
                                           [--format csv|json] [--output-dir PATH]

  python ProjectSecurityCompare.py filters <env> <env2> --projects-file FILE
                                           [--format csv|json] [--output-dir PATH]

  # Apply from diff CSV
  python ProjectSecurityCompare.py apply-roles   <csv-file>  [--apply]
  python ProjectSecurityCompare.py apply-filters <csv-file>  [--apply]

  Note: when project2 is omitted the source project name is used for both.

Examples
────────
  # 1. Compare roles — different project names on same environment
  python ProjectSecurityCompare.py roles qa qa "Finance" "Finance UAT"

  # 2. Compare roles — same project across environments (project2 omitted)
  python ProjectSecurityCompare.py roles dev prod "Analytics"

  # 3. Review CSV → apply
  python ProjectSecurityCompare.py apply-roles c:/tmp/project_roles_diff_....csv
  python ProjectSecurityCompare.py apply-roles c:/tmp/project_roles_diff_....csv --apply

  # 4. Compare filters → review → apply
  python ProjectSecurityCompare.py filters prod prod "Finance" "Finance UAT"
  python ProjectSecurityCompare.py apply-filters c:/tmp/project_filters_diff_....csv --apply

  # 5. Batch — compare roles for multiple projects between dev and prod
  #    projects.txt contains one project name per line:
  #      Finance
  #      HR Analytics
  #      Marketing
  python ProjectSecurityCompare.py roles dev prod --projects-file projects.txt

  # 6. Batch — compare filters for multiple projects on same environment
  python ProjectSecurityCompare.py filters qa qa --projects-file projects.txt
"""

import argparse
import csv
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
    "target_action",
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
    "target_action",
]

# Register the same CSV dialect used by mstrio_core.write_csv
csv.register_dialect(
    "mstr_csv_read",
    delimiter=";",
    quoting=csv.QUOTE_NONNUMERIC,
    lineterminator="\n",
)


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


def _read_projects_file(path: Path) -> list[str]:
    """
    Read a project list file — one project name per line.

    Blank lines and lines starting with '#' are ignored.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Projects file not found: {p}")
    names = []
    for line in p.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            names.append(stripped)
    if not names:
        raise ValueError(f"Projects file is empty: {p}")
    logger.info("Loaded {n} project(s) from {path}", n=len(names), path=p)
    return names


def _target_action(status: str) -> str:
    """
    Default target_action based on diff status.

    source_only  → Apply  (member should be granted the role/filter on target)
    target_only  → Remove (member should be revoked from the role/filter on target)
    role_differs → Apply  (member's role on target should match the source)
    """
    if status == "source_only":
        return "Apply"
    if status == "target_only":
        return "Remove"
    if status == "role_differs":
        return "Apply"
    return ""


def _read_diff_csv(csv_path: Path) -> list[dict]:
    """
    Read a semicolon-delimited diff CSV back into a list of dicts.

    Handles the mstr_csv dialect (semicolon delimiter, QUOTE_NONNUMERIC).
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    rows: list[dict] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, dialect="mstr_csv_read")
        for row in reader:
            rows.append(dict(row))

    logger.info("Read {n} rows from {path}", n=len(rows), path=path)
    return rows


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
            status = "source_only"
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
                    "status": status,
                    "target_action": _target_action(status),
                }
            )
        elif t and not s:
            status = "target_only"
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
                    "status": status,
                    "target_action": _target_action(status),
                }
            )
        elif s["role_id"] != t["role_id"]:
            status = "role_differs"
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
                    "status": status,
                    "target_action": _target_action(status),
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
            status = "source_only"
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
                    "status": status,
                    "target_action": _target_action(status),
                }
            )
        elif t and not s:
            status = "target_only"
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
                    "status": status,
                    "target_action": _target_action(status),
                }
            )
        # If both exist with the same key, they match — no row needed

    rows.sort(key=lambda r: (r["status"], r["filter_name"].lower(), r["member_name"].lower()))
    return rows


# ── Operations: Compare ──────────────────────────────────────────────────────


def _diff_roles(
    src_env: str,
    src_project: str,
    tgt_env: str,
    tgt_project: str,
) -> list[dict]:
    """
    Connect, build role maps, and return diff rows for one project pair.

    Returns an empty list when assignments are identical.
    """
    same_env = src_env == tgt_env

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
    else:
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
    return rows


def _diff_filters(
    src_env: str,
    src_project: str,
    tgt_env: str,
    tgt_project: str,
) -> list[dict]:
    """
    Connect, build filter maps, and return diff rows for one project pair.

    Returns an empty list when assignments are identical.
    """
    same_env = src_env == tgt_env

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
    else:
        n_src = sum(1 for r in rows if r["status"] == "source_only")
        n_tgt = sum(1 for r in rows if r["status"] == "target_only")
        logger.info(
            "{total} difference(s): {src_only} source-only, {tgt_only} target-only",
            total=len(rows),
            src_only=n_src,
            tgt_only=n_tgt,
        )
    return rows


def _write_output(
    rows: list[dict],
    columns: list[str],
    fmt: str,
    path_stem: str,
    out_dir: Path,
) -> None:
    """Write diff rows to CSV or JSON."""
    if fmt == "csv":
        path = out_dir / f"{path_stem}.csv"
        write_csv(_dicts_to_rows(rows, columns), columns=columns, path=path)
    elif fmt == "json":
        path = out_dir / f"{path_stem}.json"
        path.write_text(_json.dumps(rows, indent=2, default=str), encoding="utf-8")
    else:
        raise ValueError(f"Unsupported format {fmt!r}. Use 'csv' or 'json'.")
    logger.success("Diff ({n} rows) written → {path}", n=len(rows), path=path)


def compare_roles(
    src_env: str,
    src_project: str,
    tgt_env: str,
    tgt_project: str,
    fmt: str = "csv",
    output_dir: Path | None = None,
) -> None:
    """Compare security role assignments between two projects."""
    rows = _diff_roles(src_env, src_project, tgt_env, tgt_project)
    if not rows:
        return

    out = _out_dir(_make_config(src_env), output_dir)
    src_safe = src_project.replace(" ", "_")
    tgt_safe = tgt_project.replace(" ", "_")
    stem = f"project_roles_diff_{src_safe}_{src_env}_vs_{tgt_safe}_{tgt_env}"
    _write_output(rows, _ROLES_CSV_COLS, fmt, stem, out)


def compare_filters(
    src_env: str,
    src_project: str,
    tgt_env: str,
    tgt_project: str,
    fmt: str = "csv",
    output_dir: Path | None = None,
) -> None:
    """Compare security filter assignments between two projects."""
    rows = _diff_filters(src_env, src_project, tgt_env, tgt_project)
    if not rows:
        return

    out = _out_dir(_make_config(src_env), output_dir)
    src_safe = src_project.replace(" ", "_")
    tgt_safe = tgt_project.replace(" ", "_")
    stem = f"project_filters_diff_{src_safe}_{src_env}_vs_{tgt_safe}_{tgt_env}"
    _write_output(rows, _FILTERS_CSV_COLS, fmt, stem, out)


# ── Operations: Batch ────────────────────────────────────────────────────────


def batch_compare_roles(
    src_env: str,
    tgt_env: str,
    projects_file: Path,
    fmt: str = "csv",
    output_dir: Path | None = None,
) -> None:
    """
    Compare security role assignments for every project listed in a file.

    All results are collected into a single output file.
    Each project name is used as both source and target project name
    (compared across the two environments).
    """
    projects = _read_projects_file(projects_file)
    total = len(projects)
    all_rows: list[dict] = []

    for i, proj_name in enumerate(projects, 1):
        logger.info(
            "── [{i}/{total}] Roles: {proj!r}  ({src} → {tgt}) ──",
            i=i,
            total=total,
            proj=proj_name,
            src=src_env,
            tgt=tgt_env,
        )
        try:
            rows = _diff_roles(src_env, proj_name, tgt_env, proj_name)
            all_rows.extend(rows)
        except Exception as exc:
            logger.error(
                "Failed for project {proj!r}: {exc}",
                proj=proj_name,
                exc=exc,
            )

    logger.info(
        "Batch complete: {n} project(s) processed, {rows} total difference(s)",
        n=total,
        rows=len(all_rows),
    )

    if not all_rows:
        logger.info("No differences found across any project.")
        return

    out = _out_dir(_make_config(src_env), output_dir)
    stem = f"project_roles_diff_{src_env}_vs_{tgt_env}_batch"
    _write_output(all_rows, _ROLES_CSV_COLS, fmt, stem, out)


def batch_compare_filters(
    src_env: str,
    tgt_env: str,
    projects_file: Path,
    fmt: str = "csv",
    output_dir: Path | None = None,
) -> None:
    """
    Compare security filter assignments for every project listed in a file.

    All results are collected into a single output file.
    """
    projects = _read_projects_file(projects_file)
    total = len(projects)
    all_rows: list[dict] = []

    for i, proj_name in enumerate(projects, 1):
        logger.info(
            "── [{i}/{total}] Filters: {proj!r}  ({src} → {tgt}) ──",
            i=i,
            total=total,
            proj=proj_name,
            src=src_env,
            tgt=tgt_env,
        )
        try:
            rows = _diff_filters(src_env, proj_name, tgt_env, proj_name)
            all_rows.extend(rows)
        except Exception as exc:
            logger.error(
                "Failed for project {proj!r}: {exc}",
                proj=proj_name,
                exc=exc,
            )

    logger.info(
        "Batch complete: {n} project(s) processed, {rows} total difference(s)",
        n=total,
        rows=len(all_rows),
    )

    if not all_rows:
        logger.info("No differences found across any project.")
        return

    out = _out_dir(_make_config(src_env), output_dir)
    stem = f"project_filters_diff_{src_env}_vs_{tgt_env}_batch"
    _write_output(all_rows, _FILTERS_CSV_COLS, fmt, stem, out)


# ── Operations: Apply ────────────────────────────────────────────────────────


def apply_roles(csv_path: Path, dry_run: bool = True) -> None:
    """
    Read a roles diff CSV and grant/revoke security role assignments on the
    target project.

    The CSV must have been produced by the ``roles`` subcommand (or manually
    edited to the same schema).  Only rows whose ``target_action`` column is
    "Apply" or "Remove" are processed; blank/empty rows are skipped.

    Apply logic:
      Apply  + source_only  → grant source_role to member on target project
      Apply  + role_differs → revoke target_role, then grant source_role
      Remove + target_only  → revoke target_role from member on target project
      Remove + role_differs → revoke target_role (do not grant source_role)
    """
    rows = _read_diff_csv(csv_path)
    actionable = [
        r for r in rows if r.get("target_action", "").strip() in ("Apply", "Remove")
    ]

    if not actionable:
        logger.info("No actionable rows (target_action = Apply or Remove). Nothing to do.")
        return

    n_apply = sum(1 for r in actionable if r["target_action"].strip() == "Apply")
    n_remove = sum(1 for r in actionable if r["target_action"].strip() == "Remove")
    logger.info(
        "{n} actionable row(s): {a} Apply, {r} Remove",
        n=len(actionable),
        a=n_apply,
        r=n_remove,
    )

    if dry_run:
        logger.info("DRY RUN — no changes will be made.  Pass --apply to execute.")
        for r in actionable:
            action = r["target_action"].strip()
            status = r.get("status", "")
            member = r.get("member_name", r.get("member_id", "?"))
            if action == "Apply":
                role = r.get("source_role_name", "?")
                logger.info(
                    "  Would GRANT role {role!r} to {member!r} on {proj!r} ({env})",
                    role=role,
                    member=member,
                    proj=r.get("target_project", "?"),
                    env=r.get("target_env", "?"),
                )
                if status == "role_differs":
                    old_role = r.get("target_role_name", "?")
                    logger.info(
                        "    (first revoke current role {old!r})",
                        old=old_role,
                    )
            elif action == "Remove":
                role = r.get("target_role_name", "?")
                logger.info(
                    "  Would REVOKE role {role!r} from {member!r} on {proj!r} ({env})",
                    role=role,
                    member=member,
                    proj=r.get("target_project", "?"),
                    env=r.get("target_env", "?"),
                )
        return

    # ── Group rows by target environment + project ────────────────────────
    # All rows in a single CSV share the same target_env and target_project,
    # but we group defensively in case a CSV is manually composed.
    by_target: dict[tuple[str, str], list[dict]] = {}
    for r in actionable:
        key = (r["target_env"].strip(), r["target_project"].strip())
        by_target.setdefault(key, []).append(r)

    for (tgt_env, tgt_project), target_rows in by_target.items():
        config = _make_config(tgt_env)
        conn = get_mstrio_connection(config=config)
        try:
            _resolve_project_name(conn, tgt_project)

            # Cache role lookups
            role_cache: dict[str, SecurityRole] = {}

            for r in target_rows:
                action = r["target_action"].strip()
                status = r.get("status", "")
                member_id = r["member_id"].strip()
                member_name = r.get("member_name", member_id)
                member_type = r.get("member_type", "User").strip()

                # Resolve member
                if member_type == "UserGroup":
                    member = UserGroup(conn, id=member_id)
                else:
                    member = User(conn, id=member_id)

                if action == "Apply":
                    # Role to grant is the source role
                    role_name = r.get("source_role_name", "").strip()
                    role_id = r.get("source_role_id", "").strip()

                    # Revoke existing role first if role_differs
                    if status == "role_differs":
                        old_role_id = r.get("target_role_id", "").strip()
                        if old_role_id:
                            old_role = _get_or_cache_role(
                                conn, role_cache, old_role_id,
                                r.get("target_role_name", ""),
                            )
                            try:
                                old_role.revoke_from(
                                    [member], project=Project(conn, name=tgt_project)
                                )
                                logger.info(
                                    "Revoked role {role!r} from {member!r}",
                                    role=old_role.name,
                                    member=member_name,
                                )
                            except Exception as exc:
                                logger.error(
                                    "Failed to revoke role {role!r} from {member!r}: {exc}",
                                    role=old_role.name,
                                    member=member_name,
                                    exc=exc,
                                )
                                continue

                    # Grant the source role
                    if role_id:
                        role = _get_or_cache_role(conn, role_cache, role_id, role_name)
                        try:
                            role.grant_to(
                                [member], project=Project(conn, name=tgt_project)
                            )
                            logger.success(
                                "Granted role {role!r} to {member!r} on {proj!r}",
                                role=role.name,
                                member=member_name,
                                proj=tgt_project,
                            )
                        except Exception as exc:
                            logger.error(
                                "Failed to grant role {role!r} to {member!r}: {exc}",
                                role=role.name,
                                member=member_name,
                                exc=exc,
                            )

                elif action == "Remove":
                    # Revoke the target role
                    role_name = r.get("target_role_name", "").strip()
                    role_id = r.get("target_role_id", "").strip()
                    if role_id:
                        role = _get_or_cache_role(conn, role_cache, role_id, role_name)
                        try:
                            role.revoke_from(
                                [member], project=Project(conn, name=tgt_project)
                            )
                            logger.success(
                                "Revoked role {role!r} from {member!r} on {proj!r}",
                                role=role.name,
                                member=member_name,
                                proj=tgt_project,
                            )
                        except Exception as exc:
                            logger.error(
                                "Failed to revoke role {role!r} from {member!r}: {exc}",
                                role=role.name,
                                member=member_name,
                                exc=exc,
                            )
        finally:
            conn.close()


def apply_filters(csv_path: Path, dry_run: bool = True) -> None:
    """
    Read a filters diff CSV and apply/revoke security filter assignments on
    the target project.

    Apply logic:
      Apply  + source_only → apply the security filter to the member on target
      Remove + target_only → revoke the security filter from the member on target
    """
    rows = _read_diff_csv(csv_path)
    actionable = [
        r for r in rows if r.get("target_action", "").strip() in ("Apply", "Remove")
    ]

    if not actionable:
        logger.info("No actionable rows (target_action = Apply or Remove). Nothing to do.")
        return

    n_apply = sum(1 for r in actionable if r["target_action"].strip() == "Apply")
    n_remove = sum(1 for r in actionable if r["target_action"].strip() == "Remove")
    logger.info(
        "{n} actionable row(s): {a} Apply, {r} Remove",
        n=len(actionable),
        a=n_apply,
        r=n_remove,
    )

    if dry_run:
        logger.info("DRY RUN — no changes will be made.  Pass --apply to execute.")
        for r in actionable:
            action = r["target_action"].strip()
            member = r.get("member_name", r.get("member_id", "?"))
            fname = r.get("filter_name", "?")
            if action == "Apply":
                logger.info(
                    "  Would APPLY filter {filter!r} to {member!r} on {proj!r} ({env})",
                    filter=fname,
                    member=member,
                    proj=r.get("target_project", "?"),
                    env=r.get("target_env", "?"),
                )
            elif action == "Remove":
                logger.info(
                    "  Would REVOKE filter {filter!r} from {member!r} on {proj!r} ({env})",
                    filter=fname,
                    member=member,
                    proj=r.get("target_project", "?"),
                    env=r.get("target_env", "?"),
                )
        return

    # ── Group by target environment + project ─────────────────────────────
    by_target: dict[tuple[str, str], list[dict]] = {}
    for r in actionable:
        key = (r["target_env"].strip(), r["target_project"].strip())
        by_target.setdefault(key, []).append(r)

    for (tgt_env, tgt_project), target_rows in by_target.items():
        config = _make_config(tgt_env)
        conn = get_mstrio_connection(config=config)
        try:
            _resolve_project_name(conn, tgt_project)

            # Cache filter lookups
            filter_cache: dict[str, SecurityFilter] = {}

            for r in target_rows:
                action = r["target_action"].strip()
                member_id = r["member_id"].strip()
                member_name = r.get("member_name", member_id)
                member_type = r.get("member_type", "User").strip()
                filter_id = r.get("filter_id", "").strip()
                filter_name = r.get("filter_name", "?")

                if not filter_id:
                    logger.warning(
                        "Skipping row — no filter_id for filter {f!r}",
                        f=filter_name,
                    )
                    continue

                # Resolve member
                if member_type == "UserGroup":
                    member = UserGroup(conn, id=member_id)
                else:
                    member = User(conn, id=member_id)

                # Resolve filter
                sf = _get_or_cache_filter(conn, filter_cache, filter_id, filter_name)

                if action == "Apply":
                    try:
                        sf.apply([member])
                        logger.success(
                            "Applied filter {filter!r} to {member!r} on {proj!r}",
                            filter=sf.name,
                            member=member_name,
                            proj=tgt_project,
                        )
                    except Exception as exc:
                        logger.error(
                            "Failed to apply filter {filter!r} to {member!r}: {exc}",
                            filter=sf.name,
                            member=member_name,
                            exc=exc,
                        )

                elif action == "Remove":
                    try:
                        sf.revoke([member])
                        logger.success(
                            "Revoked filter {filter!r} from {member!r} on {proj!r}",
                            filter=sf.name,
                            member=member_name,
                            proj=tgt_project,
                        )
                    except Exception as exc:
                        logger.error(
                            "Failed to revoke filter {filter!r} from {member!r}: {exc}",
                            filter=sf.name,
                            member=member_name,
                            exc=exc,
                        )
        finally:
            conn.close()


# ── Cache helpers ─────────────────────────────────────────────────────────────


def _get_or_cache_role(
    conn, cache: dict[str, SecurityRole], role_id: str, role_name: str
) -> SecurityRole:
    """Retrieve a SecurityRole from cache or fetch it by ID."""
    if role_id not in cache:
        cache[role_id] = SecurityRole(conn, id=role_id)
        logger.debug("Cached role: {name} ({id})", name=role_name, id=role_id)
    return cache[role_id]


def _get_or_cache_filter(
    conn, cache: dict[str, SecurityFilter], filter_id: str, filter_name: str
) -> SecurityFilter:
    """Retrieve a SecurityFilter from cache or fetch it by ID."""
    if filter_id not in cache:
        cache[filter_id] = SecurityFilter(conn, id=filter_id)
        logger.debug("Cached filter: {name} ({id})", name=filter_name, id=filter_id)
    return cache[filter_id]


# ── CLI ───────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Compare project-level security between two MicroStrategy projects, "
            "then optionally apply changes to the target."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            '  python ProjectSecurityCompare.py roles   qa qa "Finance" "Finance UAT"\n'
            '  python ProjectSecurityCompare.py roles   dev prod "Analytics"\n'
            '  python ProjectSecurityCompare.py roles   dev prod --projects-file projects.txt\n'
            '  python ProjectSecurityCompare.py filters prod prod "Finance" "Finance UAT"\n'
            '  python ProjectSecurityCompare.py apply-roles   c:/tmp/project_roles_diff.csv\n'
            '  python ProjectSecurityCompare.py apply-roles   c:/tmp/project_roles_diff.csv --apply\n'
            '  python ProjectSecurityCompare.py apply-filters c:/tmp/project_filters_diff.csv --apply\n'
        ),
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # ── roles ─────────────────────────────────────────────────────────────
    r_parser = sub.add_parser(
        "roles",
        help="Compare security role assignments between two projects.",
        description=(
            "Compare security role assignments.  Provide two project names\n"
            "for a single comparison, or use --projects-file to iterate over\n"
            "a list (same project name compared on both environments)."
        ),
    )
    r_parser.add_argument("env", choices=ENVS, help="Source environment.")
    r_parser.add_argument("env2", choices=ENVS, help="Target environment.")
    r_parser.add_argument(
        "project", nargs="?", default=None,
        help="Source project name (required unless --projects-file).",
    )
    r_parser.add_argument(
        "project2", nargs="?", default=None,
        help="Target project name (defaults to source project name if omitted).",
    )
    r_parser.add_argument(
        "--projects-file", type=Path, default=None, metavar="FILE",
        help="Text file with project names (one per line). "
             "Each project is compared across the two environments.",
    )
    r_parser.add_argument(
        "--format", choices=["csv", "json"], default="csv",
        help="Output format (default: csv).",
    )
    r_parser.add_argument("--output-dir", type=Path, default=None, metavar="PATH")

    # ── filters ───────────────────────────────────────────────────────────
    f_parser = sub.add_parser(
        "filters",
        help="Compare security filter assignments between two projects.",
        description=(
            "Compare security filter assignments.  Provide two project names\n"
            "for a single comparison, or use --projects-file to iterate over\n"
            "a list (same project name compared on both environments)."
        ),
    )
    f_parser.add_argument("env", choices=ENVS, help="Source environment.")
    f_parser.add_argument("env2", choices=ENVS, help="Target environment.")
    f_parser.add_argument(
        "project", nargs="?", default=None,
        help="Source project name (required unless --projects-file).",
    )
    f_parser.add_argument(
        "project2", nargs="?", default=None,
        help="Target project name (defaults to source project name if omitted).",
    )
    f_parser.add_argument(
        "--projects-file", type=Path, default=None, metavar="FILE",
        help="Text file with project names (one per line). "
             "Each project is compared across the two environments.",
    )
    f_parser.add_argument(
        "--format", choices=["csv", "json"], default="csv",
        help="Output format (default: csv).",
    )
    f_parser.add_argument("--output-dir", type=Path, default=None, metavar="PATH")

    # ── apply-roles ───────────────────────────────────────────────────────
    ar_parser = sub.add_parser(
        "apply-roles",
        help="Apply role changes from a diff CSV to the target project.",
    )
    ar_parser.add_argument(
        "csv_file", type=Path,
        help="Path to the roles diff CSV (from the roles subcommand).",
    )
    ar_parser.add_argument(
        "--apply",
        dest="dry_run",
        action="store_false",
        default=True,
        help="Execute changes (default: dry run).",
    )

    # ── apply-filters ─────────────────────────────────────────────────────
    af_parser = sub.add_parser(
        "apply-filters",
        help="Apply filter changes from a diff CSV to the target project.",
    )
    af_parser.add_argument(
        "csv_file", type=Path,
        help="Path to the filters diff CSV (from the filters subcommand).",
    )
    af_parser.add_argument(
        "--apply",
        dest="dry_run",
        action="store_false",
        default=True,
        help="Execute changes (default: dry run).",
    )

    # ── Dispatch ──────────────────────────────────────────────────────────
    args = parser.parse_args()

    if args.command == "roles":
        if args.projects_file:
            if args.project:
                r_parser.error(
                    "Do not provide project names when using --projects-file."
                )
            batch_compare_roles(
                src_env=args.env,
                tgt_env=args.env2,
                projects_file=args.projects_file,
                fmt=args.format,
                output_dir=args.output_dir,
            )
        else:
            if not args.project:
                r_parser.error(
                    "project is required unless --projects-file is given."
                )
            compare_roles(
                src_env=args.env,
                src_project=args.project,
                tgt_env=args.env2,
                tgt_project=args.project2 or args.project,
                fmt=args.format,
                output_dir=args.output_dir,
            )
    elif args.command == "filters":
        if args.projects_file:
            if args.project:
                f_parser.error(
                    "Do not provide project names when using --projects-file."
                )
            batch_compare_filters(
                src_env=args.env,
                tgt_env=args.env2,
                projects_file=args.projects_file,
                fmt=args.format,
                output_dir=args.output_dir,
            )
        else:
            if not args.project:
                f_parser.error(
                    "project is required unless --projects-file is given."
                )
            compare_filters(
                src_env=args.env,
                src_project=args.project,
                tgt_env=args.env2,
                tgt_project=args.project2 or args.project,
                fmt=args.format,
                output_dir=args.output_dir,
            )
    elif args.command == "apply-roles":
        apply_roles(csv_path=args.csv_file, dry_run=args.dry_run)
    elif args.command == "apply-filters":
        apply_filters(csv_path=args.csv_file, dry_run=args.dry_run)
