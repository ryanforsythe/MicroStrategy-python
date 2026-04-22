"""
SecurityRoleEveryoneRemove.py — Remove a user group (default: "Everyone")
from all security role assignments across every loaded project.

Scans every project on the Intelligence Server, and for each security role
checks whether the specified user group is assigned.  When found, the group
is revoked from that role on that project.

Output columns:
    project_name   – Project name
    project_id     – Project GUID
    role_name      – Security role name
    role_id        – Security role GUID
    group_name     – User group name
    group_id       – User group GUID
    action         – "revoke" (planned or executed)
    status         – "pending" (dry run) / "success" / "error: ..."

Usage:
    python SecurityRoleEveryoneRemove.py <env>  [--apply]  [--group NAME]
                                                [--output-dir PATH]

    python SecurityRoleEveryoneRemove.py dev               # dry run — preview
    python SecurityRoleEveryoneRemove.py prod --apply       # apply changes
    python SecurityRoleEveryoneRemove.py dev --group "Public / Guest"

Run without --apply first to review the CSV output, then re-run with
--apply to commit the changes.
"""

import argparse
from pathlib import Path

from loguru import logger
from mstrio.access_and_security.security_role import (
    SecurityRole,
    list_security_roles,
)
from mstrio.server import Environment, Project
from mstrio.users_and_groups import UserGroup

from mstrio_core import MstrConfig, get_mstrio_connection, write_csv
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]
DEFAULT_GROUP_NAME = "Everyone"
OUTPUT_FILENAME = "everyone_role_removal.csv"

COLUMNS = [
    "project_name",
    "project_id",
    "role_name",
    "role_id",
    "group_name",
    "group_id",
    "action",
    "status",
]


# ── Helpers ───────────────────────────────────────────────────────────────────


def _resolve_group(conn, group_name: str) -> UserGroup:
    """
    Look up a user group by name.

    Raises ValueError if the group does not exist.
    """
    try:
        group = UserGroup(conn, name=group_name)
        logger.info(
            "Resolved user group: {name}  (ID: {id})",
            name=group.name,
            id=group.id,
        )
        return group
    except Exception as exc:
        raise ValueError(
            f"User group {group_name!r} not found: {exc}"
        ) from exc


def _list_loaded_projects(conn) -> list:
    """
    Return all loaded (active) projects on the server.

    Projects whose status indicates they are not loaded are skipped with a
    debug-level log message.
    """
    env_obj = Environment(conn)
    projects = env_obj.list_projects()

    loaded = []
    for p in projects:
        status = getattr(p, "status", None)
        if status is not None:
            # status may be an int or an enum; loaded is typically 0
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


def _find_group_in_role(role: SecurityRole, project_name: str, group_id: str) -> bool:
    """
    Check whether a user group (by ID) is assigned to *role* on *project_name*.

    Returns True if the group is found in the role's member list for that
    project, False otherwise.  Returns False on any API error (e.g. the
    project is unloaded or the role has no members).
    """
    try:
        members = role.list_members(project_name=project_name)
    except Exception as exc:
        logger.debug(
            "Could not list members for role {role!r} on project {proj!r}: {exc}",
            role=role.name,
            proj=project_name,
            exc=exc,
        )
        return False

    if not members:
        return False

    return any(m.id == group_id for m in members)


# ── Main ──────────────────────────────────────────────────────────────────────


def main(
    env: str,
    group_name: str = DEFAULT_GROUP_NAME,
    dry_run: bool = True,
    output_dir: Path | None = None,
) -> None:
    """
    Scan every project for the user group and revoke any security role
    assignments.

    Args:
        env:         Environment to connect to ("dev", "qa", or "prod").
        group_name:  User group to look for (default: "Everyone").
        dry_run:     When True (default), write the preview CSV but make no
                     server changes.  Pass False to apply.
        output_dir:  Directory for the output CSV.  Defaults to
                     MstrConfig.output_dir (MSTR_OUTPUT_DIR env var, c:/tmp).
    """
    config = MstrConfig(environment=MstrEnvironment(env))
    conn = get_mstrio_connection(config=config)

    try:
        # ── 1. Resolve user group ────────────────────────────────────────────
        group = _resolve_group(conn, group_name)

        # ── 2. List security roles ───────────────────────────────────────────
        roles = list_security_roles(conn)
        logger.info("Found {n} security role(s) on the server.", n=len(roles))

        # ── 3. List loaded projects ──────────────────────────────────────────
        projects = _list_loaded_projects(conn)
        logger.info("Found {n} loaded project(s) to scan.", n=len(projects))

        # ── 4. Scan each project + role for the group ────────────────────────
        findings: list[tuple[Project, SecurityRole]] = []

        for proj in projects:
            logger.debug("Scanning project: {name} ({id})", name=proj.name, id=proj.id)

            for role in roles:
                if _find_group_in_role(role, proj.name, group.id):
                    findings.append((proj, role))
                    logger.info(
                        "  Found {group!r} in role {role!r} on project {proj!r}",
                        group=group.name,
                        role=role.name,
                        proj=proj.name,
                    )

        logger.info(
            "Scan complete: {n} role assignment(s) found for {group!r} "
            "across {p} project(s).",
            n=len(findings),
            group=group.name,
            p=len(projects),
        )

        # ── 5. Write preview CSV ─────────────────────────────────────────────
        out_dir_path = output_dir or config.output_dir
        Path(out_dir_path).mkdir(parents=True, exist_ok=True)
        out_path = Path(out_dir_path) / OUTPUT_FILENAME

        if not findings:
            logger.info(
                "No security role assignments found for {group!r} — nothing to do.",
                group=group.name,
            )
            return

        if dry_run:
            rows = [
                [
                    proj.name,
                    proj.id,
                    role.name,
                    role.id,
                    group.name,
                    group.id,
                    "revoke",
                    "pending",
                ]
                for proj, role in findings
            ]
            write_csv(rows, columns=COLUMNS, path=out_path)
            logger.success("Preview written → {p}", p=out_path)
            logger.info(
                "Dry run — no changes applied.  "
                "Review {p} then re-run with --apply to proceed.",
                p=out_path,
            )
            return

        # ── 6. Apply — revoke the group from each role on each project ───────
        revoked = 0
        errors = 0
        result_rows: list[list] = []

        for proj, role in findings:
            try:
                role.revoke_from([group], project=proj)
                logger.success(
                    "Revoked {group!r} from role {role!r} on project {proj!r}",
                    group=group.name,
                    role=role.name,
                    proj=proj.name,
                )
                status = "success"
                revoked += 1
            except Exception as exc:
                logger.error(
                    "Failed to revoke {group!r} from role {role!r} on "
                    "{proj!r}: {exc}",
                    group=group.name,
                    role=role.name,
                    proj=proj.name,
                    exc=exc,
                )
                status = f"error: {exc}"
                errors += 1

            result_rows.append([
                proj.name,
                proj.id,
                role.name,
                role.id,
                group.name,
                group.id,
                "revoke",
                status,
            ])

        write_csv(result_rows, columns=COLUMNS, path=out_path)
        logger.success(
            "Done.  Revoked {rev}/{total} assignment(s) | Errors: {err}.  "
            "Results → {path}",
            rev=revoked,
            total=len(findings),
            err=errors,
            path=out_path,
        )

    finally:
        conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Remove a user group from all security role assignments across "
            "every loaded project.  Defaults to the 'Everyone' group."
        ),
    )
    parser.add_argument(
        "env",
        choices=ENVS,
        help="Environment to process.",
    )
    parser.add_argument(
        "--group",
        default=DEFAULT_GROUP_NAME,
        metavar="NAME",
        help=(
            f"User group name to remove (default: {DEFAULT_GROUP_NAME!r}).  "
            "Use quotes if the name contains spaces."
        ),
    )
    parser.add_argument(
        "--apply",
        dest="dry_run",
        action="store_false",
        default=True,
        help="Apply changes to the server (default: dry run — preview only).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        metavar="PATH",
        help="Output directory for the CSV (default: MSTR_OUTPUT_DIR or c:/tmp).",
    )

    args = parser.parse_args()
    main(
        env=args.env,
        group_name=args.group,
        dry_run=args.dry_run,
        output_dir=args.output_dir,
    )
