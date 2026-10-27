# Synchronize Content Groups from "Public Objects/Content Groups/<sub-folder>" folders (Workstation).
"""ContentGroupSynchronize_Workstation — keep Content Groups in step with folders.

MicroStrategy Workstation script. Uses the injected `connectionData` session.

Convention
----------
In a project, the folder  Public Objects / Content Groups  holds one sub-folder
per content group. Each sub-folder's DESCRIPTION names its content group:

    ContentGroupGUID: 0A1B2C3D4E5F60718293A4B5C6D7E8F9

and the sub-folder holds the dashboards (or, more usually, shortcuts to them)
that belong in that group for this project. Documents and reports work too.

Setting a project up
--------------------
When a project has no "Content Groups" folder, the script creates it under
Public Objects (PredefinedFolders.PUBLIC_OBJECTS) with a single template
sub-folder:

    ContentGroupName    description: ContentGroupGUID: <id>

Rename that folder to the content group, put the group's GUID in the
description, add the shortcuts, and the next run syncs it. The template folder
itself is skipped on every run — its description has no real GUID — and a
project that already has the "Content Groups" folder is never seeded.

For every such sub-folder the script:
  1. resolves each item — a shortcut to its target, a dashboard/document/report
     as itself; other object types, sub-folders and dangling shortcuts are
     reported and skipped;
  2. reads the content group's current contents for this project;
  3. ADDS items that are in the folder but not in the group;
  4. with remove_missing_content, REMOVES dashboards/documents/reports that
     are in the group for this project but in none of its sub-folders. Off by
     default.
     Agents and other types in the group are never removed.

Several sub-folders may point to the same content group; their items are
combined before comparing, so they never remove each other's items. Content
group membership is per project, so each project is compared on its own.

By default (apply_changes = false) nothing is changed and the final output
lists only the changes that would be made. With apply_changes = true they are
applied, then the group is read back and each change is reported as applied
or not.

mstrio-py throughout
--------------------
    projects          Environment.list_loaded_projects()
    folders           get_predefined_folder_contents(PUBLIC_OBJECTS),
                      Folder.get_subfolders() / .get_contents() / .description,
                      Folder.create() + folders.get_predefined_folder_id()
    shortcut targets  Shortcut(...).target_info
    content group     ContentGroup(...).name, .update_contents(add, remove)
    group contents    mstrio.api.content_groups.get_content_group_contents
The last is mstrio-py's own REST wrapper rather than ContentGroup.get_contents():
that method fetches every item separately and raises on any bot-subtype
document in the group, which would stop the run.

Inputs
------
    projects                list  Project names or GUIDs, e.g. ['Cultiva',
                                  'Cultiva Library Conversion'], or ['All'] for
                                  every loaded project (required). A text value
                                  also works: names separated by commas,
                                  semicolons or new lines. Projects run in the
                                  order given; duplicates are ignored.
    apply_changes           text  false (default) = dry run; true = make the
                                  changes
    remove_missing_content  text  false (default) / true — also remove group
                                  items that are not in any sub-folder

A Workstation list variable arrives as a Python list literal, e.g.
['Cultiva', 'Cultiva Library Conversion'].

Workstation "unset" tokens (case-insensitive): '', '-', 'null', 'blank', 'empty'.
"""

import json
import re
import sys
from datetime import datetime

from mstrio.api import content_groups as cg_api
from mstrio.api import folders as folders_api
from mstrio.connection import get_connection
from mstrio.object_management import PredefinedFolders
from mstrio.object_management.folder import Folder, get_predefined_folder_contents
from mstrio.object_management.shortcut import Shortcut
from mstrio.project_objects import Dashboard, Report
from mstrio.project_objects.content_group import ContentGroup
from mstrio.server import Environment

# --- Editable configuration --------------------------------------------------
CONTENT_GROUPS_FOLDER = "Content Groups"       # folder under Public Objects
DESCRIPTION_PATTERN = re.compile(r"ContentGroupGUID\s*:\s*([0-9A-Fa-f]{32})")
TEMPLATE_FOLDER_NAME = "ContentGroupName"      # the example folder created with it
TEMPLATE_DESCRIPTION = "ContentGroupGUID: <id>"
# ----------------------------------------------------------------------------

FOLDER_TYPE = 8
SHORTCUT_TYPE = 18
# Types a content group accepts that a folder can hold: report, document/dashboard.
CONTENT_TYPES = {3: "Report", 55: "Dashboard/Document"}

_EMPTY_SENTINELS = {"", "-", "null", "blank", "empty"}


def _blank(value, default=None):
    if value is None:
        return default
    if isinstance(value, str) and value.strip().lower() in _EMPTY_SENTINELS:
        return default
    return value


def _as_bool(value, name):
    text = str(value).strip().lower()
    if text in ("true", "yes", "y", "1"):
        return True
    if text in ("false", "no", "n", "0"):
        return False
    raise ValueError(f"{name} must be true or false, not {value!r}.")


# --- Substituted by Workstation (text -> 'quoted') --------------------------
PROJECTS = $projects                                     # noqa: F821 (list variable)
APPLY_CHANGES = _as_bool(_blank($apply_changes, "false"), "apply_changes")  # noqa: F821
REMOVE_MISSING = _as_bool(_blank($remove_missing_content, "false"), "remove_missing_content")  # noqa: F821
# ----------------------------------------------------------------------------


# --- Logging (loguru, with a same-format fallback) ----------------------------------
LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS ZZ} | {level: <8} | {message}"

try:
    from loguru import logger
    logger.remove()
    logger.add(lambda message: sys.stdout.write(message), format=LOG_FORMAT,
               level="INFO", colorize=False)
except ImportError:  # loguru isn't installed in this runtime
    class _TimestampLogger:
        def __getattr__(self, level):
            def log(message, *args, **kwargs):
                if args or kwargs:
                    message = message.format(*args, **kwargs)
                now = datetime.now().astimezone()
                print(f"{now:%Y-%m-%d %H:%M:%S}.{now.microsecond // 1000:03d} {now:%z} | "
                      f"{level.upper():<8} | {message}", flush=True)
            return log

    logger = _TimestampLogger()


# --- Projects -------------------------------------------------------------------------


def project_list(value):
    """Project entries from the `projects` variable, in order, blanks dropped.

    A Workstation list variable arrives as a list, e.g. ['Cultiva', 'Cultiva
    Library Conversion']: each item is one project, used as is (a name may
    contain a comma). A text value is split on commas, semicolons and new lines.
    """
    if isinstance(value, (list, tuple)):
        parts = [str(item) for item in value if item is not None]
    else:
        parts = re.split(r"[,;\r\n]+", str(value or ""))
    return [part.strip() for part in parts
            if part.strip() and part.strip().lower() not in _EMPTY_SENTINELS]


def select_projects(conn, wanted):
    """[{'id', 'name'}] for "All", or for each project named/identified in `wanted`.

    Every entry must match a loaded project by name or GUID (case-insensitive);
    unknown entries are all reported together before anything runs.
    """
    loaded = Environment(conn).list_loaded_projects(to_dictionary=True)
    projects = [{"id": p["id"], "name": p.get("name")} for p in loaded]
    entries = project_list(wanted)
    if not entries:
        raise ValueError("projects is required: project names or GUIDs, or ['All'].")
    if any(e.lower() == "all" for e in entries):
        if len(entries) > 1:
            raise ValueError('"All" cannot be combined with other project names.')
        return sorted(projects, key=lambda p: str(p["name"]).lower())

    chosen, missing = [], []
    for entry in entries:
        match = next((p for p in projects
                      if entry.lower() in (str(p["id"]).lower(), str(p["name"]).lower())), None)
        if match is None:
            missing.append(entry)
        elif match not in chosen:
            chosen.append(match)
    if missing:
        raise ValueError(f"Not loaded: {', '.join(repr(m) for m in missing)}. Loaded projects: "
                         + ", ".join(sorted(str(p["name"]) for p in projects)))
    return chosen


# --- Folders ---------------------------------------------------------------------------


def _type_of(item):
    value = item.get("type")
    return getattr(value, "value", value)


def content_group_folder(conn, project_id):
    """The 'Content Groups' folder dict under Public Objects, or None."""
    items = get_predefined_folder_contents(
        conn, folder_type=PredefinedFolders.PUBLIC_OBJECTS, project_id=project_id,
        to_dictionary=True)
    for item in items:
        if (_type_of(item) == FOLDER_TYPE
                and str(item.get("name", "")).strip().lower() == CONTENT_GROUPS_FOLDER.lower()):
            return item
    return None


def resolve_shortcut(conn, project_id, shortcut):
    """Target {'id', 'name', 'type'} of a shortcut, or None (with the reason).
    Shortcut.target_info is the server's 'targetInfo' (not 'target')."""
    try:
        target = Shortcut(conn, id=shortcut["id"], project_id=project_id).target_info
    except Exception as exc:  # noqa: BLE001
        return None, f"shortcut lookup failed: {exc}"
    if not target or not target.get("id"):
        return None, "shortcut has no target (the target was probably deleted)"
    return {"id": target["id"], "name": target.get("name") or shortcut.get("name"),
            "type": _type_of(target)}, None


def folder_items(conn, project_id, folder_id, issues, where):
    """{target_id: {'id','name','type','via'}} for the content objects in a folder."""
    items = {}
    for obj in Folder(conn, id=folder_id).get_contents(to_dictionary=True):
        obj_type = _type_of(obj)
        via = "direct"
        if obj_type == SHORTCUT_TYPE:
            target, problem = resolve_shortcut(conn, project_id, obj)
            if problem:
                issues.append(f"{where}: shortcut '{obj.get('name')}' ({obj.get('id')}) — {problem}")
                continue
            obj, obj_type, via = target, target["type"], f"shortcut {obj.get('id')}"
        if obj_type not in CONTENT_TYPES:
            issues.append(f"{where}: '{obj.get('name')}' ({obj.get('id')}) is object type "
                          f"{obj_type}, not a dashboard/document/report — skipped")
            continue
        items.setdefault(obj["id"], {"id": obj["id"], "name": obj.get("name"),
                                     "type": obj_type, "via": via})
    return items


def public_objects_folder_id(conn, project_id):
    """GUID of the project's Public Objects folder."""
    response = folders_api.get_predefined_folder_id(
        conn, folder_type=PredefinedFolders.PUBLIC_OBJECTS.value, project_id=project_id)
    if not response.ok:
        raise RuntimeError(f"could not find Public Objects: HTTP {response.status_code} "
                           f"{response.text[:200]}")
    folder_id = (response.json() or {}).get("id")
    if not folder_id:
        raise RuntimeError("the Public Objects lookup returned no id")
    return folder_id


def seed_project(conn, project, do_apply):
    """Create the 'Content Groups' folder with one template sub-folder.

    Runs only when the project has no 'Content Groups' folder.
    Returns (root folder dict or None, change rows).
    """
    pid = project["id"]

    def row(object_name, via):
        return {"project_id": pid, "project_name": project["name"],
                "content_group_id": None, "content_group_name": None, "folders": "",
                "action": "create folder", "object_id": None, "object_name": object_name,
                "object_type": "Folder", "via": via, "status": "dry run"}

    changes = [row(f"{CONTENT_GROUPS_FOLDER} (under Public Objects)", "missing in this project"),
               row(TEMPLATE_FOLDER_NAME, f"template, description '{TEMPLATE_DESCRIPTION}'")]

    if not do_apply:
        logger.info("  no '{name}' folder: would create it with the '{template}' template "
                    "folder", name=CONTENT_GROUPS_FOLDER, template=TEMPLATE_FOLDER_NAME)
        return None, changes

    root = Folder.create(conn, name=CONTENT_GROUPS_FOLDER,
                         parent_id=public_objects_folder_id(conn, pid),
                         description="Folders that drive ContentGroupSynchronize")
    changes[0]["status"], changes[0]["object_id"] = "created", root.id
    logger.info("  created '{name}' ({id}) under Public Objects",
                name=CONTENT_GROUPS_FOLDER, id=root.id)

    template = Folder.create(conn, name=TEMPLATE_FOLDER_NAME, parent_id=root.id,
                             description=TEMPLATE_DESCRIPTION)
    changes[1]["status"], changes[1]["object_id"] = "created", template.id
    logger.info("  created the '{template}' template folder ({id}); rename it to the content "
                "group, put its GUID in the description, then add shortcuts",
                template=TEMPLATE_FOLDER_NAME, id=template.id)
    return {"id": root.id, "name": CONTENT_GROUPS_FOLDER}, changes


def desired_contents(conn, project, issues, root):
    """{content_group_id: {'folders': [...], 'items': {id: item}}} for one project."""
    pid = project["id"]
    desired = {}
    for sub in Folder(conn, id=root["id"]).get_subfolders(to_dictionary=True):
        where = f"{project['name']} / {CONTENT_GROUPS_FOLDER} / {sub.get('name')}"
        description = Folder(conn, id=sub["id"]).description or ""
        match = DESCRIPTION_PATTERN.search(description)
        if not match:
            if str(sub.get("name", "")).strip() == TEMPLATE_FOLDER_NAME:
                logger.info("  template folder '{name}' — skipped", name=TEMPLATE_FOLDER_NAME)
            else:
                issues.append(f"{where}: description has no 'ContentGroupGUID: <guid>' — skipped")
            continue
        cg_id = match.group(1).upper()
        entry = desired.setdefault(cg_id, {"folders": [], "items": {}})
        entry["folders"].append(sub.get("name"))
        for item_id, item in folder_items(conn, pid, sub["id"], issues, where).items():
            entry["items"].setdefault(item_id, item)
    return desired


# --- Content groups ------------------------------------------------------------------


def content_group_name(conn, cg_id):
    """The content group's name, or None when it doesn't exist / can't be read."""
    try:
        return ContentGroup(conn, id=cg_id).name
    except Exception:  # noqa: BLE001
        return None


def current_contents(conn, cg_id, project_id):
    """{object_id: {'id','name','type'}} in the content group for this project."""
    response = cg_api.get_content_group_contents(conn, id=cg_id, project_ids=[project_id])
    if not response.ok:
        raise RuntimeError(f"reading contents failed: HTTP {response.status_code} "
                           f"{response.text[:200]}")
    body = response.json() or {}          # {project_id: [ {id, name, type, ...} ]}
    rows = body.get(project_id)
    if rows is None:                      # key casing can differ from the request
        rows = next((v for k, v in body.items() if str(k).upper() == project_id.upper()), [])
    return {c["id"]: {"id": c["id"], "name": c.get("name"), "type": _type_of(c)}
            for c in rows or [] if c.get("id")}


def _content_object(conn, item):
    """The mstrio object update_contents() needs. It sends only the object's id
    and class type, so Dashboard stands in for documents too (both type 55)."""
    cls = Report if item["type"] == 3 else Dashboard
    return cls(connection=conn, id=item["id"])


def apply_changes(conn, cg_id, project_id, to_add, to_remove):
    """ContentGroup.update_contents() for this group and project.

    update_contents() builds each operation's path from the object's
    connection.project_id, so the objects are built while `conn` has this
    project selected (synchronize_project selects it first).
    """
    if conn.project_id != project_id:
        raise RuntimeError(f"connection has project {conn.project_id} selected, "
                           f"expected {project_id}")
    ContentGroup(conn, id=cg_id).update_contents(
        content_to_add=[_content_object(conn, i) for i in to_add] or None,
        content_to_remove=[_content_object(conn, i) for i in to_remove] or None,
    )


def synchronize_project(conn, project, do_apply, remove_missing, issues):
    """Compare (and apply) every content group referenced in this project.
    Returns the list of change rows."""
    pid = project["id"]
    conn.select_project(project_id=pid)
    changes = []

    root = content_group_folder(conn, pid)
    if root is None:                       # first run for this project: set it up
        root, changes = seed_project(conn, project, do_apply)
        if root is None:                   # dry run, or the folder couldn't be created
            return changes

    for cg_id, entry in desired_contents(conn, project, issues, root).items():
        folders = ", ".join(entry["folders"])
        cg_name = content_group_name(conn, cg_id)
        if cg_name is None:
            issues.append(f"{project['name']} / {folders}: content group {cg_id} "
                          f"not found — skipped")
            continue
        current = current_contents(conn, cg_id, pid)
        wanted = entry["items"]
        to_add = [wanted[i] for i in wanted if i not in current]
        to_remove = ([current[i] for i in current
                      if i not in wanted and current[i]["type"] in CONTENT_TYPES]
                     if remove_missing else [])
        logger.info("  {cg} ({id}) from folder(s) {folders}: {n_folder} in folder(s), "
                    "{n_group} in group, {add} to add, {remove} to remove",
                    cg=cg_name, id=cg_id, folders=folders, n_folder=len(wanted),
                    n_group=len(current), add=len(to_add), remove=len(to_remove))
        if not (to_add or to_remove):
            continue

        status = "dry run"
        if do_apply:
            try:
                apply_changes(conn, cg_id, pid, to_add, to_remove)
                after = current_contents(conn, cg_id, pid)
                status = None
            except Exception as exc:  # noqa: BLE001
                after, status = None, f"error: {exc}"
        for action, item in [("add", i) for i in to_add] + [("remove", i) for i in to_remove]:
            row_status = status
            if row_status is None:    # applied: confirm from the read-back
                present = item["id"] in after
                row_status = ("applied" if present == (action == "add")
                              else "not applied (read-back disagrees)")
            changes.append({
                "project_id": pid, "project_name": project["name"],
                "content_group_id": cg_id, "content_group_name": cg_name,
                "folders": folders, "action": action,
                "object_id": item["id"], "object_name": item.get("name"),
                "object_type": CONTENT_TYPES.get(item["type"], item["type"]),
                "via": item.get("via", "in content group"),
                "status": row_status,
            })
    return changes


# --- Main ------------------------------------------------------------------------------


def main():
    if not project_list(PROJECTS):
        raise ValueError("projects is required: project names or GUIDs, or ['All'].")
    mode = "APPLY" if APPLY_CHANGES else "DRY RUN — nothing will be changed"
    logger.info("Content group sync: projects={p}; {mode}; remove_missing_content={r}",
                p=project_list(PROJECTS), mode=mode, r=REMOVE_MISSING)

    conn = get_connection(connectionData)  # noqa: F821
    projects = select_projects(conn, PROJECTS)
    logger.info("{n} project(s): {names}", n=len(projects),
                names=", ".join(str(p["name"]) for p in projects))
    changes, issues, failed = [], [], []
    for project in projects:
        logger.info("Project {name} ({id})", name=project["name"], id=project["id"])
        try:
            changes += synchronize_project(conn, project, APPLY_CHANGES, REMOVE_MISSING, issues)
        except Exception as exc:  # noqa: BLE001
            failed.append(project["name"])
            logger.error("  project {name} failed: {error}", name=project["name"], error=exc)

    for issue in issues:
        logger.warning("{issue}", issue=issue)

    title = "Changes" if APPLY_CHANGES else "Changes that would be made (dry run)"
    logger.info("=== {title}: {count} ===", title=title, count=len(changes))
    for c in changes:
        if c["action"] == "create folder":
            logger.info("  CREATE FOLDER '{obj}' in {project} [{status}]",
                        obj=c["object_name"], project=c["project_name"], status=c["status"])
        else:
            logger.info("  {action:<6} {obj} ({obj_id}) {verb} '{cg}' in {project} [{status}]",
                        action=c["action"].upper(), obj=c["object_name"], obj_id=c["object_id"],
                        verb="to" if c["action"] == "add" else "from",
                        cg=c["content_group_name"], project=c["project_name"],
                        status=c["status"])
    if not changes:
        logger.info("  none — every content group already matches its folder(s)")

    errors = [c for c in changes if c["status"] not in ("dry run", "applied", "created")]
    if failed or errors:
        logger.error("{p} project(s) failed, {e} change(s) not applied.",
                     p=len(failed), e=len(errors))
    return changes, 1 if (failed or errors) else 0


# The host exec()s this script as a string, so the __main__ guard is unreliable.
result, _rc = main()
print(json.dumps(result, default=str))


def get_results():
    return result


if _rc:
    sys.exit(_rc)
