"""
ContentGroupAdd.py — Add objects to a MicroStrategy content group.

Supports two input modes:
  1. CSV file with a "GUID" column (additional columns are ignored)
  2. Folder GUID — adds all non-hidden, non-folder contents; resolves shortcuts
     to their target objects

Content groups accept: Dashboard, Document, Report.

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
from pathlib import Path

from loguru import logger
from mstrio.project_objects import Dashboard, Document
from mstrio.project_objects.content_group import ContentGroup, list_content_groups

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

# Object types that ContentGroup accepts, mapped to mstrio-py classes.
# Type 55 = Document/Dashboard
_CONTENT_TYPE_MAP = {
    55: Dashboard,   # Dashboard is the modern form of type 55 (Dossier)
}

_OUTPUT_COLS = [
    "GUID", "Name", "Type", "TypeID", "Source", "Status",
]


# ── Helpers ───────────────────────────────────────────────────────────────────


def _set_project(session, config, project_name=None):
    """Set the active project on the session."""
    if project_name:
        session.set_project(name=project_name)
    elif config.project_name:
        session.set_project(name=config.project_name)
    elif config.project_id:
        session.set_project(project_id=config.project_id)
    else:
        raise ValueError(
            "No project specified. Use --project or set MSTR_PROJECT_NAME / MSTR_PROJECT_ID."
        )


def _resolve_content_group_sdk(conn, name_or_id):
    """
    Resolve a content group by name or GUID via mstrio-py SDK.

    Returns (id, name) tuple.
    """
    # Try by ID first (32-char hex)
    if len(name_or_id) == 32 and name_or_id.isalnum():
        try:
            cg = ContentGroup(connection=conn, id=name_or_id)
            return cg.id, cg.name
        except Exception:
            logger.debug("Lookup by ID failed, trying as name.")

    # Try by name
    try:
        cg = ContentGroup(connection=conn, name=name_or_id)
        return cg.id, cg.name
    except Exception:
        pass

    # Fall back to listing all and matching
    all_cgs = list_content_groups(connection=conn)
    for cg in all_cgs:
        if cg.name == name_or_id or cg.id == name_or_id:
            return cg.id, cg.name

    raise ValueError(f"Content group not found: {name_or_id!r}")


def _make_content_object(conn, obj_id, obj_type):
    """
    Instantiate the appropriate mstrio-py object for ContentGroup.update_contents().

    Returns the mstrio object, or None if the type is not supported by content groups.
    """
    cls = _CONTENT_TYPE_MAP.get(obj_type)
    if cls is None:
        return None
    return cls(connection=conn, id=obj_id)


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


def _add_to_content_group(conn, cg_id, cg_name, objects, apply):
    """
    Add objects to a content group via mstrio-py ContentGroup.update_contents().

    Args:
        conn:       mstrio-py Connection (session.mstrio_conn).
        cg_id:      Content group GUID.
        cg_name:    Content group name (for logging).
        objects:    List of dicts with 'id', 'name', and 'type' keys.
        apply:      If False, dry-run only (no SDK call).

    Returns:
        List of result dicts for reporting.
    """
    results = []
    content_to_add = []
    skipped = 0

    for obj in objects:
        type_name = OBJECT_TYPE_ID_MAP.get(obj["type"], str(obj["type"]))

        mstrio_obj = _make_content_object(conn, obj["id"], obj["type"])
        if mstrio_obj is None:
            logger.warning(
                "Skipping unsupported type for content group: "
                "{name} ({id}, type={type_name}). "
                "Content groups accept: Dashboard, Document, Report.",
                name=obj.get("name", ""), id=obj["id"], type_name=type_name,
            )
            results.append({
                "id": obj["id"],
                "name": obj.get("name", ""),
                "type_name": type_name,
                "type_id": obj["type"],
                "status": "SKIPPED (unsupported type)",
            })
            skipped += 1
            continue

        if not apply:
            results.append({
                "id": obj["id"],
                "name": obj.get("name", ""),
                "type_name": type_name,
                "type_id": obj["type"],
                "status": "DRY-RUN",
            })
        else:
            content_to_add.append(mstrio_obj)
            results.append({
                "id": obj["id"],
                "name": obj.get("name", ""),
                "type_name": type_name,
                "type_id": obj["type"],
                "status": "PENDING",
            })

    if not apply:
        if skipped:
            logger.info(
                "Dry-run: {n} object(s) would be added, {s} skipped (unsupported type)",
                n=len(results) - skipped, s=skipped,
            )
        return results

    if not content_to_add:
        logger.warning("No supported objects to add after filtering.")
        return results

    # Add via mstrio-py SDK
    cg = ContentGroup(connection=conn, id=cg_id)
    try:
        cg.update_contents(content_to_add=content_to_add)
        logger.success(
            "Added {n} object(s) to content group '{name}' ({id})",
            n=len(content_to_add), name=cg_name, id=cg_id,
        )
        for r in results:
            if r["status"] == "PENDING":
                r["status"] = "ADDED"
    except Exception as exc:
        logger.error(
            "Failed to add objects to content group '{name}': {exc}",
            name=cg_name, exc=exc,
        )
        for r in results:
            if r["status"] == "PENDING":
                r["status"] = f"FAILED ({exc})"

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
        _set_project(session, config, project)
        conn = session.mstrio_conn

        # Resolve content group
        cg_id, cg_name = _resolve_content_group_sdk(conn, content_group)
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

        results = _add_to_content_group(conn, cg_id, cg_name, objects, apply)

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
        _set_project(session, config, project)
        conn = session.mstrio_conn

        # Resolve content group
        cg_id, cg_name = _resolve_content_group_sdk(conn, content_group)
        logger.info(
            "Content group: {name} ({id})", name=cg_name, id=cg_id,
        )

        # Get folder contents (REST API via mstrio_core)
        objects = _get_folder_objects(session, folder_id)

        if not objects:
            logger.warning("No eligible objects found in folder. Nothing to add.")
            return

        # Add to content group
        mode = "APPLY" if apply else "DRY-RUN"
        logger.info("Mode: {mode}", mode=mode)

        results = _add_to_content_group(conn, cg_id, cg_name, objects, apply)

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
