"""
MicroStrategy folder navigation and object lookup utilities.

Functions
─────────
folder_contents        — List the contents of a folder (by GUID or root).
folder_path_to_guid    — Resolve a backslash-delimited folder path to a GUID.
get_predefined_folder  — Return the GUID of a MicroStrategy predefined system folder.
get_object_type_info   — Resolve type, subtype, and extended-type metadata for any object GUID.

Constants / Enums
─────────────────
PredefinedFolder       — Enum of common MicroStrategy predefined folder type IDs.
OBJECT_TYPE_MAP        — Dict mapping lowercase object-type names to their integer IDs.
OBJECT_TYPE_ID_MAP     — Dict mapping integer type IDs to display names.
OBJECT_TYPE_CATEGORY   — Dict mapping integer type IDs to their category string.
"""

from __future__ import annotations

from enum import IntEnum
from typing import Union

from loguru import logger

from mstrio_core.connection import MstrRestSession


# ─────────────────────────────────────────────────────────────────────────────
# Predefined folder type IDs
# Reference: https://github.com/MicroStrategy/mstrio-py/blob/master/mstrio/object_management/predefined_folders.py
# ─────────────────────────────────────────────────────────────────────────────


class PredefinedFolder(IntEnum):
    """
    MicroStrategy predefined system folder type identifiers.

    Pass any member (or its string name / integer value) to
    ``get_predefined_folder()`` to retrieve the corresponding folder GUID
    from the live environment.

    Example::

        guid = get_predefined_folder(session, PredefinedFolder.PUBLIC_REPORTS)
        guid = get_predefined_folder(session, "PUBLIC_REPORTS")
        guid = get_predefined_folder(session, 7)
    """

    PUBLIC_OBJECTS = 1
    PUBLIC_FILTERS = 4
    PUBLIC_METRICS = 5
    PUBLIC_PROMPTS = 6
    PUBLIC_REPORTS = 7
    PUBLIC_TEMPLATES = 9
    SCHEMA_OBJECTS = 24
    SCHEMA_ATTRIBUTES = 26
    SCHEMA_FACTS = 29
    SCHEMA_HIERARCHIES = 31
    SCHEMA_TABLES = 35
    ROOT = 39
    SYSTEM_MD_SECURITY_FILTERS = 69


# ─────────────────────────────────────────────────────────────────────────────
# Object-type reference tables
# Reference: https://community.microstrategy.com/s/article/KB16048
# ─────────────────────────────────────────────────────────────────────────────

#: Maps lowercase object-type names to their integer type IDs.
OBJECT_TYPE_MAP: dict[str, int] = {
    "aggmetric": 7,
    "attribute": 12,
    "attributeform": 21,
    "autostyles": 6,
    "catalog": 24,
    "catalogdefn": 25,
    "column": 26,
    "configuration": 36,
    "consolidation": 47,
    "consolidationelement": 48,
    "dbconnection": 31,
    "dblogin": 30,
    "dbms": 57,
    "dbrole": 29,
    "dbtable": 53,
    "dimension": 14,
    "document": 55,
    "drillmap": 56,
    "fact": 13,
    "factgroup": 17,
    "filter": 1,
    "findobject": 23,
    "folder": 8,
    "function": 11,
    "functionpackagedefinition": 42,
    "inbox": 45,
    "inboxmsg": 46,
    "link": 52,
    "metric": 4,
    "project": 32,
    "prompt": 10,
    "propertygroup": 27,
    "propertyset": 28,
    "reportdefinition": 3,
    "request": 37,
    "reserved": 0,
    "resolution": 19,
    "role": 43,
    "scheduleevent": 49,
    "scheduleobject": 50,
    "scheduletrigger": 51,
    "schema": 22,
    "search": 39,
    "searchfolder": 40,
    "securityfilter": 58,
    "securityrole": 44,
    "serverdef": 33,
    "shortcut": 18,
    "table": 15,
    "tablesource": 54,
    "template": 2,
}

#: Maps integer type IDs to display names.
OBJECT_TYPE_ID_MAP: dict[int, str] = {
    0: "Reserved",
    1: "Filter",
    2: "Template",
    3: "Grid",
    4: "Metric",
    6: "AutoStyles",
    7: "AggMetric",
    8: "Folder",
    10: "Prompt",
    11: "Function",
    12: "Attribute",
    13: "Fact",
    14: "Dimension",
    15: "Logical Table",
    17: "FactGroup",
    18: "Shortcut",
    19: "Resolution",
    21: "AttributeForm",
    22: "Schema",
    23: "FindObject",
    24: "Catalog",
    25: "CatalogDefn",
    26: "Column",
    27: "PropertyGroup",
    28: "PropertySet",
    29: "DBRole",
    30: "DBLogin",
    31: "DBConnection",
    32: "Project",
    33: "ServerDef",
    36: "Configuration",
    37: "Request",
    39: "Search",
    40: "SearchFolder",
    42: "FunctionPackageDefinition",
    43: "Role",
    44: "SecurityRole",
    45: "Inbox",
    46: "InboxMsg",
    47: "Consolidation",
    48: "ConsolidationElement",
    49: "ScheduleEvent",
    50: "ScheduleObject",
    51: "ScheduleTrigger",
    52: "Link",
    53: "DBTable",
    54: "TableSource",
    55: "Document",
    56: "DrillMap",
    57: "DBMS",
    58: "SecurityFilter",
}

#: Maps integer type IDs to their category (PublicObject, SchemaObject, etc.).
OBJECT_TYPE_CATEGORY: dict[int, str] = {
    0: "ServerObject",           # reserved
    1: "PublicObject",           # filter
    2: "PublicObject",           # template
    3: "PublicObject",           # reportdefinition / grid
    4: "PublicObject",           # metric
    6: "ProjectObject",          # autostyles
    7: "PublicObject",           # aggmetric
    8: "ProjectObject",          # folder
    10: "PublicObject",          # prompt
    11: "SchemaObject",          # function
    12: "SchemaObject",          # attribute
    13: "SchemaObject",          # fact
    14: "SchemaObject",          # dimension
    15: "SchemaObject",          # table
    17: "SchemaObject",          # factgroup
    18: "PublicObject",          # shortcut
    19: "Other",                 # resolution
    21: "SchemaObject",          # attributeform
    22: "SchemaObject",          # schema
    23: "PublicObject",          # findobject
    24: "ProjectObject",         # catalog
    25: "ProjectObject",         # catalogdefn
    26: "SchemaObject",          # column
    27: "ProjectObject",         # propertygroup
    28: "ProjectObject",         # propertyset
    29: "ConfigurationObject",   # dbrole
    30: "ConfigurationObject",   # dblogin
    31: "ConfigurationObject",   # dbconnection
    32: "ProjectObject",         # project
    33: "ServerObject",          # serverdef
    36: "ProjectObject",         # configuration
    37: "ServerObject",          # request
    39: "PublicObject",          # search
    40: "PublicObject",          # searchfolder
    42: "SchemaObject",          # functionpackagedefinition
    43: "ConfigurationObject",   # role
    44: "ConfigurationObject",   # securityrole
    45: "ProjectObject",         # inbox
    46: "ProjectObject",         # inboxmsg
    47: "PublicObject",          # consolidation
    48: "PublicObject",          # consolidationelement
    49: "ConfigurationObject",   # scheduleevent
    50: "ConfigurationObject",   # scheduleobject
    51: "ConfigurationObject",   # scheduletrigger
    52: "ProjectObject",         # link
    53: "ConfigurationObject",   # dbtable
    54: "SchemaObject",          # tablesource
    55: "PublicObject",          # document
    56: "PublicObject",          # drillmap
    57: "ConfigurationObject",   # dbms
    58: "PublicObject",          # securityfilter
}


# ─────────────────────────────────────────────────────────────────────────────
# Folder utilities
# ─────────────────────────────────────────────────────────────────────────────


def folder_contents(
    session: MstrRestSession,
    folder_id: str = "",
    object_type: int | None = None,
    offset: int = 0,
    limit: int = -1,
) -> list[dict]:
    """
    Return the contents of a MicroStrategy folder.

    Calls ``GET /api/folders/{folder_id}`` (or ``GET /api/folders`` for the
    project root when ``folder_id`` is empty). Results are optionally filtered
    to a single ``object_type``.

    Args:
        session:     An authenticated ``MstrRestSession`` with a project set.
        folder_id:   GUID of the folder to list. Omit (or pass ``""``) for
                     the project root.
        object_type: Integer object-type ID to filter results (e.g. ``8`` for
                     folders, ``3`` for reports). ``None`` returns all types.
        offset:      Pagination start index. Default 0.
        limit:       Maximum items to return. ``-1`` means no limit.

    Returns:
        List of folder-entry dicts, each containing at least ``id``, ``name``,
        and ``type`` keys.

    Raises:
        requests.HTTPError: If the API call fails.

    Example::

        # List all sub-folders under a known folder
        entries = folder_contents(session, folder_id="ABC123", object_type=8)
        for e in entries:
            print(e["name"], e["id"])

        # List all objects in the project root (no type filter)
        root = folder_contents(session)
    """
    path = "/folders"
    if folder_id:
        path += f"/{folder_id}"

    qs_parts: list[str] = [f"offset={offset}", f"limit={limit}"]
    if object_type is not None:
        qs_parts.append(f"type={object_type}")
    full_path = path + "?" + "&".join(qs_parts)

    r = session.get(full_path)
    r.raise_for_status()

    data: list[dict] = r.json()
    logger.debug(
        "folder_contents: {count} items in folder '{fid}'",
        count=len(data),
        fid=folder_id or "(root)",
    )
    return data


def folder_path_to_guid(
    session: MstrRestSession,
    folder_path: str,
) -> str:
    """
    Resolve a backslash-delimited folder path to a GUID.

    Traverses the MicroStrategy folder hierarchy level-by-level, starting
    from the project root, until the deepest folder in the path is found.
    Only objects of type 8 (folder) are considered at each level.

    Args:
        session:     An authenticated ``MstrRestSession`` with a project set.
        folder_path: Backslash-delimited path, e.g.
                     ``r"Public Objects\\Reports\\Finance"``.
                     Leading and trailing backslashes are stripped.

    Returns:
        GUID string of the target folder.

    Raises:
        ValueError:         If any folder name along the path is not found at
                            the expected level.
        requests.HTTPError: If an API call fails.

    Example::

        guid = folder_path_to_guid(session, r"Public Objects\\Finance Reports")
        entries = folder_contents(session, folder_id=guid)
    """
    parts = [p for p in folder_path.split("\\") if p]
    if not parts:
        raise ValueError("folder_path must not be empty.")

    logger.debug("Resolving folder path: {path}", path=folder_path)

    # Start at the project root — list root-level folders (type 8)
    root_entries = folder_contents(session, object_type=8)
    current_guid: str | None = _match_folder_guid(root_entries, parts[0])
    if current_guid is None:
        available = [e["name"] for e in root_entries]
        raise ValueError(
            f"Folder '{parts[0]}' not found in the project root. "
            f"Available root folders: {available}"
        )

    for part in parts[1:]:
        parent_guid = current_guid
        child_entries = folder_contents(session, folder_id=current_guid, object_type=8)
        current_guid = _match_folder_guid(child_entries, part)
        if current_guid is None:
            available = [e["name"] for e in child_entries]
            raise ValueError(
                f"Subfolder '{part}' not found under folder '{parent_guid}'. "
                f"Available: {available}"
            )

    logger.info(
        "Resolved '{path}' → {guid}", path=folder_path, guid=current_guid
    )
    return current_guid  # type: ignore[return-value]  — guaranteed non-None by guards above


def _match_folder_guid(entries: list[dict], name: str) -> str | None:
    """Return the ``id`` of the first entry whose ``name`` equals ``name``."""
    for entry in entries:
        if entry.get("name") == name:
            return entry["id"]
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Predefined folder lookup
# ─────────────────────────────────────────────────────────────────────────────


def get_predefined_folder(
    session: MstrRestSession,
    predefined_folder: Union[PredefinedFolder, str, int],
    include_ancestors: bool = False,
    show_navigation_path: bool = False,
) -> str:
    """
    Return the GUID of a MicroStrategy predefined system folder.

    Calls ``GET /api/folders/preDefined?folderType={id}`` and extracts the
    first folder GUID from the response.

    Args:
        session:              An authenticated ``MstrRestSession`` with a
                              project set.
        predefined_folder:    ``PredefinedFolder`` enum member, its string name
                              (e.g. ``"PUBLIC_REPORTS"``), or its integer type
                              ID.
        include_ancestors:    Include ancestor metadata in the API response.
                              Default ``False``.
        show_navigation_path: Include the navigation path in the API response.
                              Default ``False``.

    Returns:
        GUID string of the predefined folder.

    Raises:
        ValueError:         If ``predefined_folder`` is not a recognised name,
                            or if the API returns no folder for that type.
        requests.HTTPError: If the API call fails.

    Example::

        from mstrio_core import get_predefined_folder, PredefinedFolder

        with MstrRestSession(config) as session:
            session.set_project(project_id=config.project_id)
            guid = get_predefined_folder(session, PredefinedFolder.PUBLIC_REPORTS)
            guid = get_predefined_folder(session, "PUBLIC_METRICS")
            guid = get_predefined_folder(session, 5)   # integer ID also accepted
    """
    folder_type_id = _resolve_predefined_folder(predefined_folder)
    path = (
        f"/folders/preDefined"
        f"?folderType={folder_type_id}"
        f"&includeAncestors={str(include_ancestors).lower()}"
        f"&showNavigationPath={str(show_navigation_path).lower()}"
    )
    r = session.get(path)
    r.raise_for_status()

    data = r.json()
    folder_list: list[dict] = data.get("preDefined", [])
    if not folder_list:
        raise ValueError(
            f"No predefined folder returned for type {folder_type_id} "
            f"({predefined_folder!r}). Check that the project is set correctly."
        )

    folder_guid: str = folder_list[0]["id"]
    logger.debug(
        "Predefined folder '{name}' (type={tid}) → {guid}",
        name=predefined_folder,
        tid=folder_type_id,
        guid=folder_guid,
    )
    return folder_guid


def _resolve_predefined_folder(
    predefined_folder: Union[PredefinedFolder, str, int],
) -> int:
    """Coerce any supported representation to the integer folder-type ID."""
    if isinstance(predefined_folder, PredefinedFolder):
        return int(predefined_folder)
    if isinstance(predefined_folder, int):
        return predefined_folder
    # String name — look up in the enum
    name = predefined_folder.upper()
    try:
        return int(PredefinedFolder[name])
    except KeyError:
        valid = [m.name for m in PredefinedFolder]
        raise ValueError(
            f"Unknown predefined folder '{predefined_folder}'. "
            f"Valid names: {valid}"
        ) from None


# ─────────────────────────────────────────────────────────────────────────────
# Object type resolution
# ─────────────────────────────────────────────────────────────────────────────


def get_object_type_info(
    session: MstrRestSession,
    object_id: str,
    project_id: str | None = None,
) -> dict:
    """
    Resolve the type, subtype, and extended-type metadata for any object GUID.

    Posts to ``POST /api/searches/objects`` with the object and project IDs,
    then maps the numeric type identifiers to human-readable names using the
    mstrio-py ``ObjectTypes``, ``ObjectSubTypes``, and ``ExtendedType`` enums.

    The returned dict always has the same set of keys — on failure only
    ``status_code`` and ``status_exception_comment`` differ from ``None``.

    Args:
        session:    An authenticated ``MstrRestSession``.
        object_id:  GUID of the MicroStrategy object to look up.
        project_id: Project GUID. Falls back to ``session.project_id`` when
                    omitted.

    Returns:
        Dict with the following keys::

            {
                "status_code":                    200,        # -5 = not found
                "object_id":                      "...",
                "object_name":                    "My Metric",
                "project_id":                     "...",
                "object_type_id":                 4,
                "object_type_class_name_full":    "ObjectTypes.METRIC",
                "object_type_name":               "METRIC",
                "object_subtype_id":              800,
                "object_subtype_class_name_full": "ObjectSubTypes.METRIC",
                "object_subtype_name":            "METRIC",
                "object_exttype_id":              0,
                "object_exttype_class_name_full": "ExtendedType.NONE",
                "object_exttype_name":            "NONE",
                "status_exception_comment":       None,
            }

    Raises:
        ImportError: If ``mstrio-py`` is not installed.

    Example::

        info = get_object_type_info(session, "42095C02184EDE59ECC1A882FC2FD54A")
        if info["status_code"] == 200:
            print(info["object_type_name"], info["object_name"])
    """
    try:
        from mstrio.types import ExtendedType, ObjectSubTypes, ObjectTypes
    except ImportError as exc:
        raise ImportError(
            "mstrio-py is not installed. Run: pip install mstrio-py"
        ) from exc

    pid = project_id or session.project_id
    result: dict = {
        "status_code": 0,
        "object_id": object_id,
        "object_name": None,
        "project_id": pid,
        "object_type_id": None,
        "object_type_class_name_full": None,
        "object_type_name": None,
        "object_subtype_id": None,
        "object_subtype_class_name_full": None,
        "object_subtype_name": None,
        "object_exttype_id": None,
        "object_exttype_class_name_full": None,
        "object_exttype_name": None,
        "status_exception_comment": None,
    }

    payload = {
        "projectIdAndObjectIds": [{"projectId": pid, "objectIds": [object_id]}]
    }

    try:
        r = session.post("/searches/objects", scope="server", json=payload)
        r.raise_for_status()
        data = r.json()

        total = data.get("totalItems", 0)
        if total == 0:
            raise LookupError(
                f"Object not found. Project: {pid}; Object ID: {object_id}"
            )

        obj = data["result"][0]
        type_id: int = obj.get("type")
        subtype_id: int = obj.get("subtype")
        exttype_id: int = obj.get("extType")

        type_full = str(ObjectTypes(type_id))
        subtype_full = str(ObjectSubTypes(subtype_id))
        exttype_full = str(ExtendedType(exttype_id))

        result.update(
            {
                "status_code": 200,
                "object_name": obj.get("name"),
                "object_type_id": type_id,
                "object_type_class_name_full": type_full,
                "object_type_name": type_full.split(".")[1],
                "object_subtype_id": subtype_id,
                "object_subtype_class_name_full": subtype_full,
                "object_subtype_name": subtype_full.split(".")[1],
                "object_exttype_id": exttype_id,
                "object_exttype_class_name_full": exttype_full,
                "object_exttype_name": exttype_full.split(".")[1],
            }
        )
        logger.debug(
            "Object {oid}: type={type}, subtype={sub}",
            oid=object_id,
            type=result["object_type_name"],
            sub=result["object_subtype_name"],
        )

    except LookupError as exc:
        comment = str(exc)
        logger.warning("{msg}", msg=comment)
        result.update({"status_code": -5, "status_exception_comment": comment})
    except Exception as exc:
        comment = (
            f"Error resolving object type — project: {pid}; "
            f"object ID: {object_id}: {exc}"
        )
        logger.error("{msg}", msg=comment)
        result.update({"status_code": -2100, "status_exception_comment": comment})

    return result
