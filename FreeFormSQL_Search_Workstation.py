# Find every Freeform SQL report / cube in a project -> location, DB instance, SQL.
"""FreeFormSQL_Search_Workstation — list Freeform SQL objects in a project.

MicroStrategy Workstation script. Uses the injected `connectionData` session.

How Freeform SQL is identified
------------------------------
Freeform SQL reports and cubes are report definitions (type 3) whose
extended type is CUSTOM_SQL_FREE_FORM (3). `full_search` returns `ext_type`
on every result, so one search over type 3 (hidden objects included) is
filtered client-side. Set INCLUDE_QUERY_BUILDER to also return Query Builder
(CUSTOM_SQL_WIZARD, ext_type 4) objects.

Location comes from the search result's `ancestors` list (project root
dropped). If a server omits it, the script falls back to
GET /api/objects/{id}?type=3&includeAncestors=true.

Definition details
------------------
Each object's definition is read from the modeling API
GET /api/model/reports/{id}?showExpressionAs=tree (cubes retry
GET /api/model/cubes/{id} if the report endpoint refuses them):

    dataSource.table.dataSource            -> database instance id / name
    dataSource.table.physicalTable         -> columns + sqlExpression.tree
    dataSource.table.attributes / metrics  -> mapped objects

The SQL tree is flattened back to text: string constants are concatenated,
`apply_optional` templates (#0, #1, ...) are filled from their arguments,
security-filter qualifications render as {SecurityFilter: Attr@Form = col},
and any other object reference renders as {name}.

When the definition has no database instance, the script falls back to a
component search (database instances used by the object).

Inputs
------
    project_name    text  Project to search (required)

Output: JSON list printed to stdout, sorted by location then name.
"""

import json
from concurrent.futures import ThreadPoolExecutor

from mstrio.connection import get_connection
from mstrio.datasources import DatasourceInstance
from mstrio.object_management import full_search
from mstrio.object_management.search_enums import SearchDomain
from mstrio.types import ExtendedType, ObjectSubTypes, ObjectTypes

INCLUDE_QUERY_BUILDER = False   # also return Query Builder (ext_type 4) objects
INCLUDE_HIDDEN = True
INCLUDE_SQL = True              # flattened SQL text in the output
CONCURRENCY = 10                # parallel definition fetches

_CUBE_SUBTYPES = {ObjectSubTypes.OLAP_CUBE.value, ObjectSubTypes.SUPER_CUBE.value}

_EMPTY_SENTINELS = {"", "-", "null", "blank", "empty"}


def _blank(value, default=None):
    if value is None:
        return default
    if isinstance(value, str) and value.strip().lower() in _EMPTY_SENTINELS:
        return default
    return value


# --- Substituted by Workstation (text -> 'quoted') --------------------------
PROJECT_NAME = _blank($project_name)                 # noqa: F821
# ----------------------------------------------------------------------------


def _val(x):
    """Enum or raw value -> raw value."""
    return getattr(x, "value", x)


def _location(ancestors):
    """Folder path from an ancestors list, project root excluded."""
    names = [a.get("name", "") for a in (ancestors or [])]
    # Ancestors run root-first; the first entry is the project itself.
    return "/" + "/".join(names[1:]) if len(names) > 1 else "/"


def _fetch_location(conn, project_id, object_id):
    r = conn.get(
        endpoint=f"/api/objects/{object_id}",
        headers={"X-MSTR-ProjectID": project_id},
        params={"type": ObjectTypes.REPORT_DEFINITION.value, "includeAncestors": "true"},
    )
    if not r.ok:
        return f"<lookup failed: HTTP {r.status_code}>"
    return _location(r.json().get("ancestors"))


# --- SQL expression tree -> text --------------------------------------------

def _ref_name(node):
    """Display name for an object-reference node (form shortcut, prompt, ...)."""
    attr = node.get("attribute") or {}
    form = node.get("form") or {}
    if attr:
        return f"{attr.get('name')}@{form.get('name')}" if form else attr.get("name")
    for key in ("prompt", "target", "metric", "object"):
        if isinstance(node.get(key), dict):
            return node[key].get("name")
    return node.get("name") or node.get("objectId") or node.get("type") or "?"


def _sql_text(node):
    if not isinstance(node, dict):
        return ""
    if node.get("type") == "constant":
        return str((node.get("variant") or {}).get("value", ""))

    children = node.get("children") or []
    func = node.get("function")

    if func == "apply_optional" and children:
        # children[0] is a template ("#0 ..."); the rest fill #0, #1, ...
        text = _sql_text(children[0])
        for i in reversed(range(len(children) - 1)):   # #10 before #1
            text = text.replace(f"#{i}", _sql_text(children[i + 1]))
        return text

    if func == "apply_cs_security_filter":
        # Pairs of (attribute form, column expression).
        pairs = [f"{_ref_name(children[i])} = {_sql_text(children[i + 1])}"
                 for i in range(0, len(children) - 1, 2)]
        return "{SecurityFilter: " + ", ".join(pairs) + "}"

    if children:
        return "".join(_sql_text(c) for c in children)

    return "{" + str(_ref_name(node)) + "}"


def _tree_has(node, func_name):
    if not isinstance(node, dict):
        return False
    if node.get("function") == func_name:
        return True
    return any(_tree_has(c, func_name) for c in node.get("children") or [])


# --- Definition lookups -----------------------------------------------------

def _fetch_definition(conn, project_id, object_id, is_cube):
    """Modeling-API definition JSON, or an error string."""
    endpoints = [f"/api/model/reports/{object_id}"]
    if is_cube:
        endpoints.append(f"/api/model/cubes/{object_id}")
    last = None
    for ep in endpoints:
        r = conn.get(
            endpoint=ep,
            headers={"X-MSTR-ProjectID": project_id},
            params={"showExpressionAs": "tree", "showAdvancedProperties": "false"},
        )
        if r.ok:
            return r.json()
        last = f"<definition failed: {ep} HTTP {r.status_code}>"
    return last


def _db_instances_by_search(conn, project_id, report_id):
    """Fallback: database instances the object uses -> list of {id, name}."""
    try:
        for domain in (SearchDomain.PROJECT, SearchDomain.CONFIGURATION):
            found = full_search(
                conn,
                project=project_id,
                domain=domain,
                object_types=ObjectTypes.DBROLE,
                used_by_object_id=report_id,
                used_by_object_type=ObjectTypes.REPORT_DEFINITION,
                to_dictionary=True,
            )
            if found:
                return [{"id": d.get("id"), "name": d.get("name")} for d in found]
    except Exception as exc:
        print(f"WARNING: component search failed for {report_id}: {exc}")
    return []


def _details(conn, project_id, obj):
    """Everything pulled from the definition for one object."""
    is_cube = _val(obj.get("subtype")) in _CUBE_SUBTYPES
    defn = _fetch_definition(conn, project_id, obj["id"], is_cube)
    out = {"definition_error": None, "source_type": None, "db_instances": [],
           "sql": None, "has_security_filter": None, "columns": [],
           "attributes": [], "metrics": []}

    if isinstance(defn, str):
        out["definition_error"] = defn
    else:
        out["source_type"] = defn.get("sourceType")
        table = (defn.get("dataSource") or {}).get("table") or {}
        ds = table.get("dataSource") or {}
        if ds.get("objectId"):
            out["db_instances"] = [{"id": ds["objectId"], "name": ds.get("name")}]
        physical = table.get("physicalTable") or {}
        tree = (physical.get("sqlExpression") or {}).get("tree")
        if tree:
            out["sql"] = _sql_text(tree)
            out["has_security_filter"] = _tree_has(tree, "apply_cs_security_filter")
        out["columns"] = [c.get("name") for c in physical.get("columns") or []]
        out["attributes"] = [a.get("name") for a in table.get("attributes") or []]
        out["metrics"] = [m.get("name") for m in table.get("metrics") or []]

    if not out["db_instances"]:
        out["db_instances"] = _db_instances_by_search(conn, project_id, obj["id"])
    return out


def _dbms_name(conn, instance_id):
    try:
        dbms = DatasourceInstance(conn, id=instance_id).dbms
        return getattr(dbms, "name", None)
    except Exception as exc:
        return f"<lookup failed: {exc}>"


def main():
    if not PROJECT_NAME:
        raise ValueError("project_name is required")

    conn = get_connection(connectionData, project=PROJECT_NAME)  # noqa: F821
    project_id = conn.project_id

    wanted = {ExtendedType.CUSTOM_SQL_FREE_FORM.value}
    if INCLUDE_QUERY_BUILDER:
        wanted.add(ExtendedType.CUSTOM_SQL_WIZARD.value)

    results = full_search(
        conn,
        project=project_id,
        object_types=ObjectTypes.REPORT_DEFINITION,
        include_hidden=INCLUDE_HIDDEN,
        to_dictionary=True,
    )
    print(f"Searched {len(results)} report definitions in '{PROJECT_NAME}' ({project_id})")

    ffsql = [o for o in results if _val(o.get("ext_type")) in wanted]
    print(f"Found {len(ffsql)} Freeform SQL object(s); reading definitions")

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        details = list(pool.map(lambda o: _details(conn, project_id, o), ffsql))

    instance_ids = {d["id"] for det in details for d in det["db_instances"]}
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        dbms_by_id = dict(zip(instance_ids,
                              pool.map(lambda i: _dbms_name(conn, i), instance_ids)))

    rows = []
    for obj, det in zip(ffsql, details):
        ancestors = obj.get("ancestors")
        location = (_location(ancestors) if ancestors
                    else _fetch_location(conn, project_id, obj["id"]))
        subtype = _val(obj.get("subtype"))
        owner = obj.get("owner") or {}
        dbs = det["db_instances"]
        row = {
            "name": obj.get("name"),
            "guid": obj.get("id"),
            "location": location,
            "object_kind": "Cube" if subtype in _CUBE_SUBTYPES else "Report",
            "subtype": subtype,
            "source_type": det["source_type"],
            "db_instance_id": " | ".join(d["id"] for d in dbs),
            "db_instance_name": " | ".join(d["name"] or "" for d in dbs),
            "dbms": " | ".join(str(dbms_by_id.get(d["id"]) or "") for d in dbs),
            "has_security_filter": det["has_security_filter"],
            "column_count": len(det["columns"]),
            "attributes": det["attributes"],
            "metrics": det["metrics"],
            "owner_id": owner.get("id") if isinstance(owner, dict) else None,
            "owner_name": owner.get("name") if isinstance(owner, dict) else owner,
            "date_created": obj.get("date_created"),
            "date_modified": obj.get("date_modified"),
            "hidden": obj.get("hidden"),
            "definition_error": det["definition_error"],
        }
        if INCLUDE_SQL:
            row["sql"] = det["sql"]
        rows.append(row)

    rows.sort(key=lambda r: (r["location"].lower(), (r["name"] or "").lower()))
    missing = sum(1 for r in rows if not r["db_instance_id"])
    failed = sum(1 for r in rows if r["definition_error"])
    if failed:
        print(f"WARNING: {failed} definition(s) could not be read")
    if missing:
        print(f"WARNING: {missing} object(s) with no database instance resolved")
    print(json.dumps(rows, indent=2, default=str))


# Workstation exec()s the script as a string, so don't rely on a __main__ guard.
main()
