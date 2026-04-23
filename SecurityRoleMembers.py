"""
SecurityRoleMembers.py — Export and manage security role member assignments
across MicroStrategy projects.

Subcommands
───────────
  export      — List all security role member assignments.  Outputs one row
                per (role, project, member) triple.  Optionally filter by
                --role-id and/or --project-id.

  remove-all  — Revoke every member from the specified security role(s) on
                the specified project(s).  With no filters, removes ALL
                members from ALL roles on ALL loaded projects (use with care).

  add         — Grant security role assignments from a CSV or Excel file.
                Required columns: role_id, project_id, plus a member
                identifier (see Member type handling below).

  remove      — Revoke security role assignments from a CSV or Excel file.
                Same column rules as ``add``.

Member type handling
────────────────────
  The input file identifies members via one of three column strategies
  (checked per-row in priority order):

  1. ``user_id`` / ``userid``
     Value is always resolved as a User.

  2. ``usergroupid`` / ``user_group_id`` / ``user_groupid``
     Value is always resolved as a UserGroup.

  3. ``member_id`` / ``memberid`` / ``id``  +  ``is_group``
     Generic — the ``is_group`` column (True/False/1/0/yes/no/UserGroup)
     determines whether the ID is a User or UserGroup.

  A file may contain columns for more than one strategy.  Each row uses
  whichever column has a non-empty value (user_id checked first, then
  user-group, then generic member).  This lets you export, filter, and
  feed back the same CSV without modification.

Output columns (export & manage):
    role_id       – Security role GUID
    role_name     – Security role name
    project_id    – Project GUID
    project_name  – Project name
    member_id     – User or UserGroup GUID
    member_name   – Display name
    is_group      – True if the member is a user group, False if a user
    action        – "export" / "add" / "remove" / "remove-all"
    status        – "ok" (export) / "pending" / "success" / "error: ..."

Usage:
    python SecurityRoleMembers.py export     <env> [--role-id ID ...] [--project-id ID ...]
                                             [--format csv|json] [--output-dir PATH]

    python SecurityRoleMembers.py remove-all <env> [--role-id ID ...] [--project-id ID ...]
                                             [--apply] [--output-dir PATH]

    python SecurityRoleMembers.py add        <env> --csv PATH   [--apply] [--output-dir PATH]
    python SecurityRoleMembers.py add        <env> --excel PATH [--apply] [--output-dir PATH]

    python SecurityRoleMembers.py remove     <env> --csv PATH   [--apply] [--output-dir PATH]
    python SecurityRoleMembers.py remove     <env> --excel PATH [--apply] [--output-dir PATH]

Examples:
    # Export all role assignments across all projects
    python SecurityRoleMembers.py export dev

    # Export for specific roles and projects
    python SecurityRoleMembers.py export prod --role-id ABC123 DEF456 --project-id 789012

    # Preview removing ALL members from a single role on all projects
    python SecurityRoleMembers.py remove-all prod --role-id ABC123

    # Apply — remove all members from a role on specific projects
    python SecurityRoleMembers.py remove-all prod --role-id ABC123 --project-id 789012 --apply

    # Add members from a CSV file
    python SecurityRoleMembers.py add prod --csv assignments.csv --apply

    # Remove members listed in an Excel file
    python SecurityRoleMembers.py remove qa --excel removals.xlsx --apply

Run without --apply first to review the CSV output, then re-run with
--apply to commit the changes.
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
from mstrio.server import Environment, Project
from mstrio.users_and_groups import User, UserGroup

from mstrio_core import MstrConfig, get_mstrio_connection, read_excel, write_csv
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]
OUTPUT_FILENAME = "security_role_members.csv"

COLUMNS = [
    "role_id",
    "role_name",
    "project_id",
    "project_name",
    "member_id",
    "member_name",
    "is_group",
    "action",
    "status",
]

# CSV/Excel column aliases for the file-based add/remove subcommands
_ROLE_COL_ALIASES = {"role_id", "security_role_id", "securityroleid"}
_PROJECT_COL_ALIASES = {"project_id", "projectid"}

# Three ways to identify a member — checked in priority order:
#   1. Explicit user column     → always User
#   2. Explicit usergroup column → always UserGroup
#   3. Generic member column    → requires is_group to disambiguate
_USER_COL_ALIASES = {"user_id", "userid"}
_USERGROUP_COL_ALIASES = {"usergroupid", "user_group_id", "user_groupid"}
_MEMBER_COL_ALIASES = {"member_id", "memberid", "id"}
_ISGROUP_COL_ALIASES = {"is_group", "isgroup", "is_user_group", "member_type"}

_TRUTHY = {"true", "1", "yes", "usergroup"}


# ── Helpers ───────────────────────────────────────────────────────────────────


def _member_type(member) -> bool:
    """Return True if *member* is a UserGroup, False otherwise."""
    if isinstance(member, UserGroup):
        return True
    if isinstance(member, User):
        return False
    # Fallback — check subtype attribute (8705 = UserGroup)
    st = getattr(member, "subtype", None)
    if st is not None:
        st_val = st if isinstance(st, int) else getattr(st, "value", st)
        if st_val == 8705:
            return True
    return False


def _member_name(member) -> str:
    """Best-effort display name."""
    return (
        getattr(member, "name", None)
        or getattr(member, "username", "")
        or str(member.id)
    )


def _list_loaded_projects(conn) -> list:
    """Return all loaded (active) projects on the server."""
    env_obj = Environment(conn)
    projects = env_obj.list_projects()

    loaded = []
    for p in projects:
        status = getattr(p, "status", None)
        if status is not None:
            status_val = (
                status if isinstance(status, int)
                else getattr(status, "value", status)
            )
            if isinstance(status_val, int) and status_val != 0:
                logger.debug(
                    "Skipping unloaded project: {name} ({id}, status={s})",
                    name=p.name,
                    id=p.id,
                    s=status,
                )
                continue
        loaded.append(p)

    return loaded


def _resolve_roles(conn, role_ids: list[str] | None) -> list[SecurityRole]:
    """
    Resolve security roles.  If *role_ids* is provided, fetch those specific
    roles; otherwise return all roles on the server.
    """
    if role_ids:
        roles = []
        for rid in role_ids:
            try:
                roles.append(SecurityRole(conn, id=rid))
            except Exception as exc:
                logger.error(
                    "Could not resolve security role {id}: {exc}",
                    id=rid,
                    exc=exc,
                )
        return roles
    return list_security_roles(conn)


def _resolve_projects(conn, project_ids: list[str] | None) -> list:
    """
    Resolve projects.  If *project_ids* is provided, fetch those specific
    projects; otherwise return all loaded projects.
    """
    if project_ids:
        projects = []
        for pid in project_ids:
            try:
                projects.append(Project(conn, id=pid))
            except Exception as exc:
                logger.error(
                    "Could not resolve project {id}: {exc}",
                    id=pid,
                    exc=exc,
                )
        return projects
    return _list_loaded_projects(conn)


def _is_group_from_str(value: str) -> bool:
    """Parse an is_group cell value to a boolean."""
    return value.strip().lower() in _TRUTHY


def _find_column(headers: list[str], aliases: set[str]) -> str | None:
    """Find the first header matching an alias (case-insensitive)."""
    for h in headers:
        if h.strip().lower() in aliases:
            return h
    return None


# ── File readers ─────────────────────────────────────────────────────────────


def _read_file_records(
    csv_path: Path | None,
    excel_path: Path | None,
) -> list[dict]:
    """
    Read assignment records from a CSV or Excel file.

    Each record is a dict with keys: role_id, project_id, member_id, is_group.
    """
    if excel_path:
        return _read_excel_records(excel_path)
    if csv_path:
        return _read_csv_records(csv_path)
    raise ValueError("No input file specified.")


def _read_csv_records(csv_path: Path) -> list[dict]:
    """Read assignment records from a CSV file."""
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        sample = fh.read(4096)
        fh.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
        except csv.Error:
            logger.debug(
                "csv.Sniffer could not detect delimiter — defaulting to comma."
            )
            dialect = "excel"
        reader = csv.DictReader(fh, dialect=dialect)
        return _parse_records(list(reader), reader.fieldnames or [], csv_path)


def _read_excel_records(excel_path: Path) -> list[dict]:
    """Read assignment records from an Excel file."""
    df = read_excel(excel_path)
    # Convert DataFrame to list of dicts, casting all values to strings
    raw_rows = []
    for _, row in df.iterrows():
        raw_rows.append({col: str(val) for col, val in row.items()})
    return _parse_records(raw_rows, list(df.columns), excel_path)


def _parse_records(
    raw_rows: list[dict],
    headers: list[str],
    source_path: Path,
) -> list[dict]:
    """
    Validate columns and parse raw rows into normalised assignment records.

    Member identification supports three column strategies (checked per-row
    in priority order):

    1. **user_id / userid**  — value is always treated as a User.
    2. **usergroupid / user_group_id / user_groupid**  — value is always
       treated as a UserGroup.
    3. **member_id / memberid / id**  + **is_group** — generic; the
       ``is_group`` flag disambiguates.

    A file may contain columns for more than one strategy.  Each row uses
    whichever column has a non-empty value (user_id checked first, then
    user-group, then generic member).
    """
    role_col = _find_column(headers, _ROLE_COL_ALIASES)
    proj_col = _find_column(headers, _PROJECT_COL_ALIASES)
    user_col = _find_column(headers, _USER_COL_ALIASES)
    usergroup_col = _find_column(headers, _USERGROUP_COL_ALIASES)
    member_col = _find_column(headers, _MEMBER_COL_ALIASES)
    isgroup_col = _find_column(headers, _ISGROUP_COL_ALIASES)

    # ── Validate required columns ───────────────────────────────────────────
    missing = []
    if not role_col:
        missing.append(f"role_id ({', '.join(sorted(_ROLE_COL_ALIASES))})")
    if not proj_col:
        missing.append(f"project_id ({', '.join(sorted(_PROJECT_COL_ALIASES))})")

    has_any_member_col = user_col or usergroup_col or member_col
    if not has_any_member_col:
        missing.append(
            "member identifier — provide one of: "
            f"user_id ({', '.join(sorted(_USER_COL_ALIASES))}), "
            f"user_group_id ({', '.join(sorted(_USERGROUP_COL_ALIASES))}), "
            f"or member_id ({', '.join(sorted(_MEMBER_COL_ALIASES))}) "
            f"with is_group ({', '.join(sorted(_ISGROUP_COL_ALIASES))})"
        )

    # member_col without is_group is only valid if user_col or usergroup_col
    # is also present (those rows would use the typed column instead).
    if member_col and not isgroup_col and not user_col and not usergroup_col:
        missing.append(
            f"is_group ({', '.join(sorted(_ISGROUP_COL_ALIASES))}) — "
            "required when using a generic member_id column without "
            "user_id or user_group_id columns"
        )

    if missing:
        raise ValueError(
            f"Input file {source_path} is missing required column(s): "
            f"{'; '.join(missing)}.  Found columns: {headers}"
        )

    # Log which strategy the file uses
    strategies = []
    if user_col:
        strategies.append(f"user_id column ({user_col!r})")
    if usergroup_col:
        strategies.append(f"user_group_id column ({usergroup_col!r})")
    if member_col:
        strategies.append(f"member_id column ({member_col!r})")
    logger.info(
        "Member columns detected: {s}", s=", ".join(strategies) or "none",
    )

    # ── Parse rows ──────────────────────────────────────────────────────────
    records: list[dict] = []
    skipped = 0
    for row in raw_rows:
        role_id = (row.get(role_col) or "").strip()
        proj_id = (row.get(proj_col) or "").strip()

        if not role_id or not proj_id:
            skipped += 1
            continue
        if role_id.lower() == "nan":
            skipped += 1
            continue

        # Determine member_id and is_group from the first non-empty column
        mid: str = ""
        is_grp: bool = False

        # Priority 1: explicit user column
        if user_col:
            val = (row.get(user_col) or "").strip()
            if val and val.lower() != "nan":
                mid = val
                is_grp = False

        # Priority 2: explicit usergroup column
        if not mid and usergroup_col:
            val = (row.get(usergroup_col) or "").strip()
            if val and val.lower() != "nan":
                mid = val
                is_grp = True

        # Priority 3: generic member column + is_group
        if not mid and member_col:
            val = (row.get(member_col) or "").strip()
            if val and val.lower() != "nan":
                mid = val
                is_group_str = (row.get(isgroup_col) or "").strip() if isgroup_col else ""
                is_grp = _is_group_from_str(is_group_str)

        if not mid:
            skipped += 1
            continue

        records.append({
            "role_id": role_id,
            "project_id": proj_id,
            "member_id": mid,
            "is_group": is_grp,
        })

    if skipped:
        logger.debug(
            "Skipped {n} row(s) with missing/empty key values.",
            n=skipped,
        )
    logger.info(
        "Read {n} assignment record(s) from {path}.",
        n=len(records),
        path=source_path,
    )
    return records


# ── Subcommand: export ───────────────────────────────────────────────────────


def cmd_export(
    env: str,
    role_ids: list[str] | None = None,
    project_ids: list[str] | None = None,
    fmt: str = "csv",
    output_dir: Path | None = None,
) -> None:
    """Export security role member assignments."""
    config = MstrConfig(environment=MstrEnvironment(env))
    conn = get_mstrio_connection(config=config)
    out_dir = output_dir or config.output_dir
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    try:
        roles = _resolve_roles(conn, role_ids)
        projects = _resolve_projects(conn, project_ids)

        logger.info(
            "Scanning {r} role(s) across {p} project(s) ...",
            r=len(roles),
            p=len(projects),
        )

        rows: list[list] = []
        for proj in projects:
            for role in roles:
                try:
                    members = role.list_members(project_name=proj.name)
                except Exception as exc:
                    logger.debug(
                        "Could not list members for role {role!r} on "
                        "project {proj!r}: {exc}",
                        role=role.name,
                        proj=proj.name,
                        exc=exc,
                    )
                    continue

                if not members:
                    continue

                for m in members:
                    is_grp = _member_type(m)
                    rows.append([
                        role.id,
                        role.name,
                        proj.id,
                        proj.name,
                        m.id,
                        _member_name(m),
                        str(is_grp),
                        "export",
                        "ok",
                    ])

        logger.info(
            "Found {n} member assignment(s) across {p} project(s).",
            n=len(rows),
            p=len(projects),
        )

        if fmt == "json":
            out_path = Path(out_dir) / OUTPUT_FILENAME.replace(".csv", ".json")
            data = [dict(zip(COLUMNS, r)) for r in rows]
            out_path.write_text(
                _json.dumps(data, indent=2), encoding="utf-8"
            )
        else:
            out_path = Path(out_dir) / OUTPUT_FILENAME
            write_csv(rows, columns=COLUMNS, path=out_path)

        logger.success("Export written -> {p}", p=out_path)

    finally:
        conn.close()


# ── Subcommand: remove-all ───────────────────────────────────────────────────


def cmd_remove_all(
    env: str,
    role_ids: list[str] | None = None,
    project_ids: list[str] | None = None,
    dry_run: bool = True,
    output_dir: Path | None = None,
) -> None:
    """Revoke every member from specified role(s) on specified project(s)."""
    config = MstrConfig(environment=MstrEnvironment(env))
    conn = get_mstrio_connection(config=config)
    out_dir = output_dir or config.output_dir
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    try:
        roles = _resolve_roles(conn, role_ids)
        projects = _resolve_projects(conn, project_ids)

        logger.info(
            "Scanning {r} role(s) across {p} project(s) for members to remove ...",
            r=len(roles),
            p=len(projects),
        )

        # Build the list of (role, project, member, is_group) to revoke
        targets: list[tuple[SecurityRole, Project, object, bool]] = []
        for proj in projects:
            for role in roles:
                try:
                    members = role.list_members(project_name=proj.name)
                except Exception:
                    continue
                if not members:
                    continue
                for m in members:
                    targets.append((role, proj, m, _member_type(m)))

        logger.info(
            "Found {n} member assignment(s) to remove.",
            n=len(targets),
        )

        if not targets:
            logger.info("Nothing to remove.")
            return

        # Execute or preview
        rows: list[list] = []
        revoked = 0
        errors = 0

        for role, proj, member, is_grp in targets:
            status = "pending"
            if not dry_run:
                try:
                    role.revoke_from([member], project=proj)
                    status = "success"
                    revoked += 1
                    logger.info(
                        "Revoked {member!r} from role {role!r} on {proj!r}",
                        member=_member_name(member),
                        role=role.name,
                        proj=proj.name,
                    )
                except Exception as exc:
                    status = f"error: {exc}"
                    errors += 1
                    logger.error(
                        "Failed to revoke {member!r} from role {role!r} on "
                        "{proj!r}: {exc}",
                        member=_member_name(member),
                        role=role.name,
                        proj=proj.name,
                        exc=exc,
                    )

            rows.append([
                role.id,
                role.name,
                proj.id,
                proj.name,
                member.id,
                _member_name(member),
                str(is_grp),
                "remove-all",
                status,
            ])

        out_path = Path(out_dir) / OUTPUT_FILENAME
        write_csv(rows, columns=COLUMNS, path=out_path)

        if dry_run:
            logger.success("Preview written -> {p}", p=out_path)
            logger.info(
                "Dry run — no changes applied.  "
                "Review {p} then re-run with --apply to proceed.",
                p=out_path,
            )
        else:
            logger.success(
                "Done.  Revoked {rev}/{total} | Errors {err}.  "
                "Results -> {path}",
                rev=revoked,
                total=len(targets),
                err=errors,
                path=out_path,
            )

    finally:
        conn.close()


# ── Subcommands: add / remove (file-based) ──────────────────────────────────


def cmd_file_action(
    action: str,
    env: str,
    csv_path: Path | None = None,
    excel_path: Path | None = None,
    dry_run: bool = True,
    output_dir: Path | None = None,
) -> None:
    """
    Grant or revoke security role assignments from a CSV or Excel file.

    Args:
        action:      ``"add"`` or ``"remove"``.
        env:         Environment to connect to.
        csv_path:    Path to a CSV file.
        excel_path:  Path to an Excel file.
        dry_run:     Preview only (default).
        output_dir:  Output directory for the results CSV.
    """
    records = _read_file_records(csv_path, excel_path)
    if not records:
        logger.warning("No records to process — nothing to do.")
        return

    config = MstrConfig(environment=MstrEnvironment(env))
    conn = get_mstrio_connection(config=config)
    out_dir = output_dir or config.output_dir
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    try:
        # Cache lookups to avoid redundant API calls
        role_cache: dict[str, SecurityRole | None] = {}
        project_cache: dict[str, Project | None] = {}

        rows: list[list] = []
        successes = 0
        errors = 0

        for rec in records:
            role_id = rec["role_id"]
            proj_id = rec["project_id"]
            member_id = rec["member_id"]
            is_grp = rec["is_group"]

            # ── Resolve role ────────────────────────────────────────────────
            if role_id not in role_cache:
                try:
                    role_cache[role_id] = SecurityRole(conn, id=role_id)
                except Exception as exc:
                    logger.error(
                        "Could not resolve role {id}: {exc}",
                        id=role_id,
                        exc=exc,
                    )
                    role_cache[role_id] = None

            role = role_cache[role_id]
            role_name = role.name if role else ""

            # ── Resolve project ─────────────────────────────────────────────
            if proj_id not in project_cache:
                try:
                    project_cache[proj_id] = Project(conn, id=proj_id)
                except Exception as exc:
                    logger.error(
                        "Could not resolve project {id}: {exc}",
                        id=proj_id,
                        exc=exc,
                    )
                    project_cache[proj_id] = None

            proj = project_cache[proj_id]
            proj_name = proj.name if proj else ""

            # ── Resolve member ──────────────────────────────────────────────
            member = None
            member_name = ""
            try:
                if is_grp:
                    member = UserGroup(conn, id=member_id)
                else:
                    member = User(conn, id=member_id)
                member_name = _member_name(member)
            except Exception as exc:
                logger.error(
                    "Could not resolve member {id} (is_group={g}): {exc}",
                    id=member_id,
                    g=is_grp,
                    exc=exc,
                )

            # ── Check resolution ────────────────────────────────────────────
            if not role or not proj or not member:
                status = "error: unresolved "
                if not role:
                    status += "role "
                if not proj:
                    status += "project "
                if not member:
                    status += "member"
                status = status.strip()
                rows.append([
                    role_id, role_name, proj_id, proj_name,
                    member_id, member_name, str(is_grp),
                    action, status,
                ])
                errors += 1
                continue

            # ── Execute or preview ──────────────────────────────────────────
            if dry_run:
                status = "pending"
            else:
                try:
                    if action == "add":
                        role.grant_to([member], project=proj)
                    else:
                        role.revoke_from([member], project=proj)
                    status = "success"
                    successes += 1
                    logger.info(
                        "{action} {member!r} ({type}) {prep} role {role!r} on {proj!r}",
                        action=action.capitalize(),
                        member=member_name,
                        type="group" if is_grp else "user",
                        prep="to" if action == "add" else "from",
                        role=role_name,
                        proj=proj_name,
                    )
                except Exception as exc:
                    status = f"error: {exc}"
                    errors += 1
                    logger.error(
                        "Failed to {action} {member!r} {prep} role {role!r} "
                        "on {proj!r}: {exc}",
                        action=action,
                        member=member_name,
                        prep="to" if action == "add" else "from",
                        role=role_name,
                        proj=proj_name,
                        exc=exc,
                    )

            rows.append([
                role_id, role_name, proj_id, proj_name,
                member_id, member_name, str(is_grp),
                action, status,
            ])

        out_path = Path(out_dir) / OUTPUT_FILENAME
        write_csv(rows, columns=COLUMNS, path=out_path)

        if dry_run:
            logger.success("Preview written -> {p}", p=out_path)
            logger.info(
                "Dry run — no changes applied.  "
                "Review {p} then re-run with --apply to proceed.",
                p=out_path,
            )
        else:
            logger.success(
                "Done.  {action}: {ok} succeeded | {err} errors.  "
                "Results -> {path}",
                action=action.capitalize(),
                ok=successes,
                err=errors,
                path=out_path,
            )

    finally:
        conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Export and manage security role member assignments across "
            "MicroStrategy projects."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ── export ──────────────────────────────────────────────────────────────
    sp_export = subparsers.add_parser(
        "export",
        help="Export security role member assignments.",
    )
    sp_export.add_argument(
        "env", choices=ENVS, help="Environment to process.",
    )
    sp_export.add_argument(
        "--role-id", nargs="+", metavar="ID", default=None,
        help="One or more security role GUIDs to include (default: all).",
    )
    sp_export.add_argument(
        "--project-id", nargs="+", metavar="ID", default=None,
        help="One or more project GUIDs to include (default: all loaded).",
    )
    sp_export.add_argument(
        "--format", dest="fmt", choices=["csv", "json"], default="csv",
        help="Output format (default: csv).",
    )
    sp_export.add_argument(
        "--output-dir", type=Path, default=None, metavar="PATH",
        help="Output directory (default: MSTR_OUTPUT_DIR or c:/tmp).",
    )

    # ── remove-all ──────────────────────────────────────────────────────────
    sp_removeall = subparsers.add_parser(
        "remove-all",
        help="Remove ALL members from security role(s) on project(s).",
    )
    sp_removeall.add_argument(
        "env", choices=ENVS, help="Environment to process.",
    )
    sp_removeall.add_argument(
        "--role-id", nargs="+", metavar="ID", default=None,
        help="Security role GUIDs to clear (default: all roles).",
    )
    sp_removeall.add_argument(
        "--project-id", nargs="+", metavar="ID", default=None,
        help="Project GUIDs to target (default: all loaded projects).",
    )
    sp_removeall.add_argument(
        "--apply", dest="dry_run", action="store_false", default=True,
        help="Apply changes (default: dry run).",
    )
    sp_removeall.add_argument(
        "--output-dir", type=Path, default=None, metavar="PATH",
        help="Output directory (default: MSTR_OUTPUT_DIR or c:/tmp).",
    )

    # ── add / remove ────────────────────────────────────────────────────────
    for action_name, prep in [("add", "to"), ("remove", "from")]:
        sp = subparsers.add_parser(
            action_name,
            help=f"{action_name.capitalize()} members {prep} security roles from a file.",
        )
        sp.add_argument(
            "env", choices=ENVS, help="Environment to process.",
        )

        file_group = sp.add_mutually_exclusive_group(required=True)
        file_group.add_argument(
            "--csv", type=Path, metavar="PATH",
            help=(
                "CSV file with columns: role_id, project_id, and a member "
                "identifier.  Use user_id for users, user_group_id for "
                "groups, or member_id + is_group for mixed.  "
                "Works with Excel 'Save As CSV' files."
            ),
        )
        file_group.add_argument(
            "--excel", type=Path, metavar="PATH",
            help=(
                "Excel (.xlsx) file.  Same column rules as --csv."
            ),
        )

        sp.add_argument(
            "--apply", dest="dry_run", action="store_false", default=True,
            help="Apply changes (default: dry run).",
        )
        sp.add_argument(
            "--output-dir", type=Path, default=None, metavar="PATH",
            help="Output directory (default: MSTR_OUTPUT_DIR or c:/tmp).",
        )

    args = parser.parse_args()

    if args.command == "export":
        cmd_export(
            env=args.env,
            role_ids=args.role_id,
            project_ids=args.project_id,
            fmt=args.fmt,
            output_dir=args.output_dir,
        )

    elif args.command == "remove-all":
        cmd_remove_all(
            env=args.env,
            role_ids=args.role_id,
            project_ids=args.project_id,
            dry_run=args.dry_run,
            output_dir=args.output_dir,
        )

    elif args.command in ("add", "remove"):
        cmd_file_action(
            action=args.command,
            env=args.env,
            csv_path=getattr(args, "csv", None),
            excel_path=getattr(args, "excel", None),
            dry_run=args.dry_run,
            output_dir=args.output_dir,
        )
