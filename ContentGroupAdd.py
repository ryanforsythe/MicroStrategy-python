"""
ContentGroupAdd.py — Add objects to a MicroStrategy content group.

Supports two input modes:
  1. CSV file with a "GUID" column (additional columns are ignored)
  2. Folder GUID — adds all non-hidden, non-folder contents; resolves shortcuts
     to their target objects

Usage
─────
  python ContentGroupAdd.py csv    <env> --content-group <name-or-id> --csv <path>
                                   [--project <name>] [--apply] [--output-dir PATH]

  python ContentGroupAdd.py folder <env> --content-group <name-or-id> --folder <guid>
                                   [--project <name>] [--apply] [--output-dir PATH]

Examples
────────
  # Dry-run: preview what would be added from a CSV
  python ContentGroupAdd.py csv dev --content-group "My Content Group" --csv guids.csv

  # Add objects from a CSV to a content group
  python ContentGroupAdd.py csv prod --content-group "My Content Group" --csv guids.csv --apply

  # Add all non-hidden objects from a folder
  python ContentGroupAdd.py folder qa --content-group ABC123 --folder DEF456 --apply

  # Specify a project (overrides MSTR_PROJECT_NAME from env)
  python ContentGroupAdd.py folder prod --content-group "Dashboards" --folder ABC123 \\
      --project "MicroStrategy Tutorial" --apply
"""

import argparse
import csv
import json
from pathlib import Path

from loguru import logger

from mstrio_core import (
    MstrConfig,
    MstrRestSession,
    OBJECT_TYPE_ID_MAP,
    folder_contents,
    get_object_type_info,
    write_csv,
)
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]

_SHORTCUT_TYPE = 18
_FOLDER_TYPE = 8

_OUTPUT_COLS = [
    "GUID", "Name", "Type", "TypeID", "Source", "Status",
]


# ── Helpers ───────────────────────────────────────────────────────────────────


def _resolve_content_group(session, name_or_id):
    """
    Resolve a content group by name or GUID.

    Returns (id, name) tuple.
    """
    # Try by ID first (32-char hex)
    if len(name_or_id) == 32 and name_or_id.isalnum():
        r = session.get(f"/contentGroups/{name_or_id}", scope="server")
        if r.ok:
            data = r.json()
            return data["id"], data["name"]
        logger.debug("Lookup by ID failed, trying as name.")

    # List all and match by name
    r = session.get("/contentGroups", scope="server")
    r.raise_for_status()
    for cg in r.json().get("contentGroups", r.json() if isinstance(r.json(), list) else []):
        if cg.get("name") == name_or_id or cg.get("id") == name_or_id:
            return cg["id"], cg["name"]

    raise ValueError(f"Content group not found: {name_or_id!r}")


def _read_guids_from_csv(csv_path):
    """
    Read a CSV file and extract values from the 'GUID' column.

    Supports both comma and semicolon delimiters. Returns a list of
    non-empty GUID strings.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    text = path.read_text(encoding="utf-8")

    # Detect delimiter
    if ";" in text.splitlines()[0]:
        delimiter = ";"
    else:
        delimiter = ","

    guids = []
    reader = csv.DictReader(text.splitlines(), delimiter=delimiter)

    if "GUID" not in (reader.fieldnames or []):
        raise ValueError(
            f"CSV file must contain a 'GUID' column. "
            f"Found columns: {reader.fieldnames}"
        )

    for row in reader:
        guid = row["GUID"].strip().strip('"').strip("'")
        if guid:
            guids.append(guid)

    logger.info("Read {n} GUID(s) from {path}", n=len(guids), path=path)
    return guids


def _get_folder_objects(session, folder_id):
    """
    Get all non-hidden, non-folder objects from a folder.

    Shortcuts (type 18) are resolved to their target objects.
    Returns a list of dicts with 'id', 'name', 'type' keys.
    """
    items = folder_contents(session, folder_id=folder_id)
    logger.info(
        "Folder {fid}: {n} total item(s)",
        fid=folder_id, n=len(items),
    )

    results = []
    skipped_hidden = 0
    skipped_folders = 0
    resolved_shortcuts = 0

    for item in items:
        # Skip hidden objects
        if item.get("hidden", False):
            skipped_hidden += 1
            logger.debug("Skipping hidden: {name} ({id})", name=item.get("name"), id=item.get("id"))
            continue

        obj_type = item.get("type", 0)

        # Skip folders
        if obj_type == _FOLDER_TYPE:
            skipped_folders += 1
            logger.debug("Skipping folder: {name} ({id})", name=item.get("name"), id=item.get("id"))
            continue

        # Resolve shortcuts to their target
        if obj_type == _SHORTCUT_TYPE:
            target = _resolve_shortcut(session, item["id"])
            if target:
                resolved_shortcuts += 1
                logger.debug(
                    "Resolved shortcut '{name}' → '{tname}' ({tid}, type={ttype})",
                    name=item.get("name"),
                    tname=target.get("name"),
                    tid=target.get("id"),
                    ttype=target.get("type"),
                )
                results.append(target)
            else:
                logger.warning(
                    "Could not resolve shortcut: {name} ({id})",
                    name=item.get("name"), id=item.get("id"),
                )
            continue

        results.append({
            "id": item["id"],
            "name": item.get("name", ""),
            "type": obj_type,
        })

    logger.info(
        "Folder results: {n} object(s) to add, "
        "{hidden} hidden skipped, {folders} folders skipped, "
        "{shortcuts} shortcuts resolved",
        n=len(results),
        hidden=skipped_hidden,
        folders=skipped_folders,
        shortcuts=resolved_shortcuts,
    )
    return results


def _resolve_shortcut(session, shortcut_id):
    """
    Resolve a shortcut to its target object.

    Returns a dict with 'id', 'name', 'type' of the target, or None on failure.
    """
    r = session.get(f"/objects/{shortcut_id}?type={_SHORTCUT_TYPE}")
    if not r.ok:
        return None

    data = r.json()
    target = data.get("target")
    if target:
        return {
            "id": target["id"],
            "name": target.get("name", ""),
            "type": target.get("type", 0),
        }
    return None


def _resolve_object_types(session, guids):
    """
    For a list of GUIDs (from CSV), resolve each to get name and type.

    Returns a list of dicts with 'id', 'name', 'type' keys.
    """
    results = []
    for guid in guids:
        info = get_object_type_info(session, guid)
        if info["status_code"] == 200:
            results.append({
                "id": guid,
                "name": info["object_name"],
                "type": info["object_type_id"],
            })
        else:
            logger.warning(
                "Could not resolve GUID {guid}: {msg}",
                guid=guid, msg=info.get("status_exception_comment", "unknown error"),
            )
    return results


def _add_to_content_group(session, cg_id, project_id, objects, apply):
    """
    Add objects to a content group via REST API PATCH.

    Args:
        session:    Authenticated MstrRestSession.
        cg_id:      Content group GUID.
        project_id: Project GUID for the content path.
        objects:    List of dicts with 'id' and 'type' keys.
        apply:      If False, dry-run only (no API call).

    Returns:
        List of result dicts for reporting.
    """
    results = []

    if not apply:
        for obj in objects:
            type_name = OBJECT_TYPE_ID_MAP.get(obj["type"], str(obj["type"]))
            results.append({
                "id": obj["id"],
                "name": obj.get("name", ""),
                "type_name": type_name,
                "type_id": obj["type"],
                "status": "DRY-RUN",
            })
        return results

    # Build the PATCH payload — batch all objects in one call
    value_list = [{"id": obj["id"], "type": obj["type"]} for obj in objects]

    payload = {
        "operationList": [
            {
                "op": "add",
                "path": f"/{project_id}",
                "value": value_list,
                "id": 1,
            }
        ]
    }

    r = session.patch(
        f"/contentGroups/{cg_id}/contents",
        scope="server",
        json=payload,
    )

    if r.ok:
        logger.success(
            "Added {n} object(s) to content group {cg}",
            n=len(objects), cg=cg_id,
        )
        status = "ADDED"
    else:
        logger.error(
            "Failed to add objects: HTTP {status} — {body}",
            status=r.status_code, body=r.text[:500],
        )
        status = f"FAILED (HTTP {r.status_code})"

    for obj in objects:
        type_name = OBJECT_TYPE_ID_MAP.get(obj["type"], str(obj["type"]))
        results.append({
            "id": obj["id"],
            "name": obj.get("name", ""),
            "type_name": type_name,
            "type_id": obj["type"],
            "status": status,
        })

    return results


# ── Subcommand: csv ──────────────────────────────────────────────────────────


def cmd_csv(
    env,
    content_group,
    csv_path,
    project=None,
    apply=False,
    output_dir=None,
):
    """Add objects to a content group from a CSV file with a GUID column."""
    config = MstrConfig(environment=MstrEnvironment(env))
    out = output_dir or config.output_dir
    Path(out).mkdir(parents=True, exist_ok=True)

    guids = _read_guids_from_csv(csv_path)
    if not guids:
        logger.warning("No GUIDs found in CSV. Nothing to do.")
        return

    with MstrRestSession(config) as session:
        # Set project
        if project:
            session.set_project(name=project)
        elif config.project_name:
            session.set_project(name=config.project_name)
        elif config.project_id:
            session.set_project(project_id=config.project_id)
        else:
            raise ValueError(
                "No project specified. Use --project or set MSTR_PROJECT_NAME / MSTR_PROJECT_ID."
            )

        project_id = session.project_id

        # Resolve content group
        cg_id, cg_name = _resolve_content_group(session, content_group)
        logger.info(
            "Content group: {name} ({id})", name=cg_name, id=cg_id,
        )

        # Resolve each GUID to get type info
        logger.info("Resolving {n} GUID(s) from CSV ...", n=len(guids))
        objects = _resolve_object_types(session, guids)

        if not objects:
            logger.warning("No valid objects resolved. Nothing to add.")
            return

        logger.info(
            "Resolved {n} of {total} GUID(s)",
            n=len(objects), total=len(guids),
        )

        # Add to content group
        mode = "APPLY" if apply else "DRY-RUN"
        logger.info("Mode: {mode}", mode=mode)

        results = _add_to_content_group(session, cg_id, project_id, objects, apply)

        # Write output
        rows = [
            [r["id"], r["name"], r["type_name"], r["type_id"], "CSV", r["status"]]
            for r in results
        ]
        out_path = Path(out) / f"content_group_add_{env}.csv"
        write_csv(rows, columns=_OUTPUT_COLS, path=out_path)


# ── Subcommand: folder ───────────────────────────────────────────────────────


def cmd_folder(
    env,
    content_group,
    folder_id,
    project=None,
    apply=False,
    output_dir=None,
):
    """Add all non-hidden, non-folder objects from a folder to a content group."""
    config = MstrConfig(environment=MstrEnvironment(env))
    out = output_dir or config.output_dir
    Path(out).mkdir(parents=True, exist_ok=True)

    with MstrRestSession(config) as session:
        # Set project
        if project:
            session.set_project(name=project)
        elif config.project_name:
            session.set_project(name=config.project_name)
        elif config.project_id:
            session.set_project(project_id=config.project_id)
        else:
            raise ValueError(
                "No project specified. Use --project or set MSTR_PROJECT_NAME / MSTR_PROJECT_ID."
            )

        project_id = session.project_id

        # Resolve content group
        cg_id, cg_name = _resolve_content_group(session, content_group)
        logger.info(
            "Content group: {name} ({id})", name=cg_name, id=cg_id,
        )

        # Get folder contents
        objects = _get_folder_objects(session, folder_id)

        if not objects:
            logger.warning("No eligible objects found in folder. Nothing to add.")
            return

        # Add to content group
        mode = "APPLY" if apply else "DRY-RUN"
        logger.info("Mode: {mode}", mode=mode)

        results = _add_to_content_group(session, cg_id, project_id, objects, apply)

        # Write output
        rows = [
            [r["id"], r["name"], r["type_name"], r["type_id"], "Folder", r["status"]]
            for r in results
        ]
        out_path = Path(out) / f"content_group_add_{env}.csv"
        write_csv(rows, columns=_OUTPUT_COLS, path=out_path)


# ── CLI ───────────────────────────────────────────────────────────────────────


def _add_common_args(sub):
    """Add arguments shared by all subcommands."""
    sub.add_argument("env", choices=ENVS, help="Environment (dev, qa, prod).")
    sub.add_argument(
        "--content-group",
        required=True,
        metavar="NAME_OR_ID",
        help="Content group name or GUID.",
    )
    sub.add_argument(
        "--project",
        default=None,
        metavar="NAME",
        help="Project name (overrides MSTR_PROJECT_NAME env var).",
    )
    sub.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="Execute changes. Without this flag, runs in dry-run mode.",
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
        description="Add objects to a MicroStrategy content group.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # ── csv ───────────────────────────────────────────────────────────────
    p_csv = subparsers.add_parser(
        "csv",
        help="Add objects from a CSV file with a GUID column.",
    )
    _add_common_args(p_csv)
    p_csv.add_argument(
        "--csv",
        required=True,
        dest="csv_path",
        metavar="PATH",
        help="Path to CSV file. Must contain a 'GUID' column.",
    )

    # ── folder ────────────────────────────────────────────────────────────
    p_folder = subparsers.add_parser(
        "folder",
        help="Add all non-hidden objects from a folder (shortcuts resolved).",
    )
    _add_common_args(p_folder)
    p_folder.add_argument(
        "--folder",
        required=True,
        dest="folder_id",
        metavar="GUID",
        help="Folder GUID to read objects from.",
    )

    # ── Dispatch ──────────────────────────────────────────────────────────
    args = parser.parse_args()

    if args.subcommand == "csv":
        cmd_csv(
            env=args.env,
            content_group=args.content_group,
            csv_path=args.csv_path,
            project=args.project,
            apply=args.apply,
            output_dir=args.output_dir,
        )
    elif args.subcommand == "folder":
        cmd_folder(
            env=args.env,
            content_group=args.content_group,
            folder_id=args.folder_id,
            project=args.project,
            apply=args.apply,
            output_dir=args.output_dir,
        )
