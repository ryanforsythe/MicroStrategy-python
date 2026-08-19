"""
ContentGroupAdd.py — Add objects to (or export objects from) a MicroStrategy
content group.

Supports four modes:
  1. csv    — add objects from a CSV file with a "GUID" column (single project)
  2. folder — add all non-hidden, non-folder contents of a folder (shortcuts
              resolved to their targets)
  3. json   — add objects from a project-keyed JSON file ({project_id: [objects]}).
              The target projects come FROM the file (not --project); each
              project's objects are added under that project's context. This is
              the multi-project re-import counterpart to `export --format json`.
  4. export — read the objects currently in a content group and write them out.
              --format csv  → single project, flat rows, feeds the `csv` mode.
              --format json → ALL loaded projects, keyed by project id, empty
                              projects omitted; feeds the `json` mode. This is the
                              cross-env replication path (object GUIDs are
                              preserved across environments).

Content groups accept: Dashboard, Document, Report.

Usage
─────
  python ContentGroupAdd.py csv    <env> --content-group <name-or-id> --csv <path>
                                   [--project <name>] [--apply] [--output-dir PATH]

  python ContentGroupAdd.py folder <env> --content-group <name-or-id> --folder <guid>
                                   [--project <name>] [--apply] [--output-dir PATH]

  python ContentGroupAdd.py json   <env> --content-group <name-or-id> --json <path>
                                   [--apply] [--output-dir PATH]

  python ContentGroupAdd.py export <env> --content-group <name-or-id>
                                   [--format csv|json] [--project <name>] [--output-dir PATH]

Examples
────────
  # Dry-run: preview what would be added from a CSV
  python ContentGroupAdd.py csv dev --content-group "My Content Group" --csv guids.csv

  # Add objects from a CSV to a content group
  python ContentGroupAdd.py csv prod --content-group "My Content Group" --csv guids.csv --apply

  # Add all non-hidden objects from a folder
  python ContentGroupAdd.py folder qa --content-group ABC123 --folder DEF456 --apply

  # Export a content group's contents (single project → CSV; all projects → JSON)
  python ContentGroupAdd.py export dev --content-group "My Content Group"
  python ContentGroupAdd.py export dev --content-group "My Content Group" --format json

  # Migrate a content group dev -> prod via JSON (multi-project, projects from file)
  python ContentGroupAdd.py export dev  --content-group "My Content Group" --format json
  python ContentGroupAdd.py json   prod --content-group "My Content Group" \\
      --json c:/tmp/content_group_export_dev.json --apply

  # Specify a project (overrides MSTR_PROJECT_NAME from env)
  python ContentGroupAdd.py folder prod --content-group "Dashboards" --folder ABC123 \\
      --project "MicroStrategy Tutorial" --apply
"""

import argparse
import csv
import json
from pathlib import Path

from loguru import logger
from mstrio.project_objects import Dashboard, Document, Report
from mstrio.project_objects.content_group import ContentGroup, list_content_groups
from mstrio.server import Environment

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
# update_contents() sends only {id, type} where type = class._OBJECT_TYPE.value,
# so Dashboard covers both Documents and Dashboards (both metadata type 55).
_CONTENT_TYPE_MAP = {
    3: Report,       # Report definition
    55: Dashboard,   # Document / Dashboard (Dossier); both are type 55
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
            target = _resolve_shortcut(session, item["id"], item.get("name", ""))
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
            # _resolve_shortcut logs the specific reason on failure.
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


def _resolve_shortcut(session, shortcut_id, fallback_name=""):
    """
    Resolve a shortcut (type 18) to its target object.

    ``GET /objects/{id}?type=18`` returns the shortcut's object info; the
    target's metadata lives under the ``targetInfo`` key (mstrio maps this to
    ``target_info``). Some server versions may instead use ``target`` — both
    are handled.

    Returns a dict with 'id', 'name', 'type' of the target, or None when the
    lookup fails or no target is present (e.g. a dangling shortcut whose target
    was deleted). The reason is logged so folder runs are diagnosable.
    """
    r = session.get(f"/objects/{shortcut_id}?type={_SHORTCUT_TYPE}")
    if not r.ok:
        logger.warning(
            "Could not resolve shortcut '{name}' ({id}): "
            "object lookup failed HTTP {status} — {body}",
            name=fallback_name, id=shortcut_id,
            status=r.status_code, body=r.text[:300],
        )
        return None

    data = r.json()
    target = data.get("targetInfo") or data.get("target")
    if not target or not target.get("id"):
        logger.warning(
            "Could not resolve shortcut '{name}' ({id}): no target in response "
            "(response keys: {keys}) — possibly a dangling shortcut",
            name=fallback_name, id=shortcut_id, keys=sorted(data.keys()),
        )
        return None

    return {
        "id": target["id"],
        "name": target.get("name") or fallback_name,
        "type": target.get("type", 0),
    }


def _read_contents_json(json_path):
    """
    Read a project-keyed contents JSON file:
        {project_id: [ {id, type, name, ...}, ... ], ...}

    This is the same shape produced by `export --format json`. Projects with an
    empty array are ignored. Returns {project_id: [objects]} for populated
    projects only.
    """
    path = Path(json_path)
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(
            "JSON must be an object keyed by project id: "
            "{project_id: [ {id, type, ...}, ... ]}"
        )

    populated = {pid: objs for pid, objs in data.items() if objs}
    logger.info(
        "Read {n} object(s) across {p} project(s) from {path}",
        n=sum(len(v) for v in populated.values()),
        p=len(populated), path=path,
    )
    return populated


def _get_content_group_contents_raw(session, cg_id, project_ids):
    """
    Raw GET /contentGroups/{id}/contents response, keyed by project id:
        {project_id: [ {id, name, type, subtype, ...}, ... ], ...}

    `project_ids` may be a single id string or a list of ids (the API accepts
    repeated projectId params and returns a key for each, including empty
    arrays for projects with no content).

    Uses REST rather than the SDK get_contents() because the latter
    re-instantiates each object (per-object fetches) and silently drops types
    other than Report/Document/Dashboard/Agent.
    """
    r = session.get(
        f"/contentGroups/{cg_id}/contents",
        params={"projectId": project_ids},
    )
    if not r.ok:
        logger.error(
            "HTTP {status} retrieving content group contents: {text}",
            status=r.status_code, text=r.text,
        )
        return {}
    return r.json() or {}


def _flatten_contents(raw):
    """Flatten {project_id: [objects]} → list of {'id','name','type'} dicts."""
    results = []
    for _proj_key, contents in (raw or {}).items():
        for c in contents or []:
            results.append({
                "id": c.get("id"),
                "name": c.get("name", ""),
                "type": c.get("type", 0),
            })
    return results


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


# ── Subcommand: json ─────────────────────────────────────────────────────────


def cmd_json(
    env,
    content_group,
    json_path,
    apply=False,
    output_dir=None,
):
    """Add objects to a content group from a project-keyed JSON file.

    The JSON (same shape as `export --format json`) is keyed by project id, so
    the target projects come from the file itself — no --project/--folder is
    used. Each project's objects are added under that project's context, because
    ContentGroup.update_contents() derives the add path from the object's
    connection project.
    """
    config = MstrConfig(environment=MstrEnvironment(env))
    out = output_dir or config.output_dir
    Path(out).mkdir(parents=True, exist_ok=True)

    project_map = _read_contents_json(json_path)
    if not project_map:
        logger.warning("No populated projects in JSON. Nothing to add.")
        return

    with MstrRestSession(config) as session:
        conn = session.mstrio_conn

        # Resolve the content group once (config-level object). Select the first
        # project so the connection is project-scoped for the lookup.
        first_pid = next(iter(project_map))
        session.set_project(project_id=first_pid)
        cg_id, cg_name = _resolve_content_group_sdk(conn, content_group)
        logger.info("Content group: {name} ({id})", name=cg_name, id=cg_id)

        mode = "APPLY" if apply else "DRY-RUN"
        logger.info("Mode: {mode}", mode=mode)

        all_results = []
        for pid, objs in project_map.items():
            # Must select the project BEFORE building objects — update_contents()
            # uses content.connection.project_id for the add path. One project's
            # objects per update_contents() call (all share the one connection).
            session.set_project(project_id=pid)
            objects = [
                {"id": o.get("id"), "name": o.get("name", ""), "type": o.get("type", 0)}
                for o in objs if o.get("id")
            ]
            logger.info(
                "Project {pid}: {n} object(s)", pid=pid, n=len(objects),
            )
            results = _add_to_content_group(conn, cg_id, cg_name, objects, apply)
            for r in results:
                r["project_id"] = pid
            all_results.extend(results)

        # Write output — same schema as the add-input CSV; Source carries project id
        rows = [
            [r["id"], r["name"], r["type_name"], r["type_id"],
             f"JSON:{r.get('project_id', '')}", r["status"]]
            for r in all_results
        ]
        out_path = Path(out) / f"content_group_add_{env}.csv"
        write_csv(rows, columns=_OUTPUT_COLS, path=out_path)


# ── Subcommand: export ───────────────────────────────────────────────────────


def cmd_export(
    env,
    content_group,
    project=None,
    output_dir=None,
    fmt="csv",
):
    """Export a content group's current objects to a re-importable file.

    csv  — single project (--project / env default), flat rows. Feeds the `csv`
           subcommand on another environment (GUID column drives re-import).
    json — comprehensive: queries ALL loaded projects, output is keyed by project
           id ({project_id: [objects]}) with empty projects omitted. Feeds the
           `json` subcommand, which replicates every project's membership.
    """
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

        if fmt == "json":
            # Comprehensive: query every loaded project, keep only populated ones
            project_ids = [p.id for p in Environment(conn).list_loaded_projects()]
            raw = _get_content_group_contents_raw(session, cg_id, project_ids)
            populated = {pid: objs for pid, objs in raw.items() if objs}
            if not populated:
                logger.warning(
                    "Content group '{name}' has no contents in any loaded "
                    "project. Nothing to export.", name=cg_name,
                )
                return
            out_path = Path(out) / f"content_group_export_{env}.json"
            out_path.write_text(json.dumps(populated, indent=2), encoding="utf-8")
            total = sum(len(v) for v in populated.values())
            logger.success(
                "Exported {n} object(s) across {p} project(s) from content "
                "group '{name}' to {path}",
                n=total, p=len(populated), name=cg_name, path=out_path,
            )
            return

        # csv — single project
        raw = _get_content_group_contents_raw(session, cg_id, session.project_id)
        objects = _flatten_contents(raw)
        if not objects:
            logger.warning(
                "Content group '{name}' has no contents in project {pid}. "
                "Nothing to export.",
                name=cg_name, pid=session.project_id,
            )
            return

        rows = []
        for obj in objects:
            type_name = OBJECT_TYPE_ID_MAP.get(obj["type"], str(obj["type"]))
            rows.append([
                obj["id"], obj["name"], type_name, obj["type"],
                "ContentGroup", "EXPORTED",
            ])
        out_path = Path(out) / f"content_group_export_{env}.csv"
        write_csv(rows, columns=_OUTPUT_COLS, path=out_path)
        logger.success(
            "Exported {n} object(s) from content group '{name}' to {path}",
            n=len(rows), name=cg_name, path=out_path,
        )


# ── CLI ───────────────────────────────────────────────────────────────────────


def _add_common_args(sub, include_apply=True):
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
    if include_apply:
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

    # ── json ──────────────────────────────────────────────────────────────
    p_json = subparsers.add_parser(
        "json",
        help="Add objects from a project-keyed JSON file (projects from the file).",
    )
    _add_common_args(p_json)
    p_json.add_argument(
        "--json",
        required=True,
        dest="json_path",
        metavar="PATH",
        help="Path to a project-keyed JSON file ({project_id: [objects]}).",
    )

    # ── export ────────────────────────────────────────────────────────────
    p_export = subparsers.add_parser(
        "export",
        help="Export a content group's contents to a re-importable CSV or JSON.",
    )
    _add_common_args(p_export, include_apply=False)  # read-only, no --apply
    p_export.add_argument(
        "--format",
        choices=["csv", "json"],
        default="csv",
        dest="fmt",
        help="Output format. csv = single project (flat); "
             "json = all loaded projects, keyed by project id (default: csv).",
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
    elif args.subcommand == "json":
        cmd_json(
            env=args.env,
            content_group=args.content_group,
            json_path=args.json_path,
            apply=args.apply,
            output_dir=args.output_dir,
        )
    elif args.subcommand == "export":
        cmd_export(
            env=args.env,
            content_group=args.content_group,
            project=args.project,
            output_dir=args.output_dir,
            fmt=args.fmt,
        )
