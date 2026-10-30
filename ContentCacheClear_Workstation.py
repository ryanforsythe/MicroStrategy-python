# Invalidate or delete every content cache in a project, on every node (Workstation).
"""ContentCacheClear_Workstation — clear a project's content caches.

Connects with a URL + username/password (not get_connection), then works
node by node: list the project's content caches, then invalidate or delete them.

Why the REST API and not the ContentCache class
-----------------------------------------------
`ContentCache.list_caches(connection=conn)` raises
    ValueError: 'docreport' is not a valid CacheSource.Type
because it turns every cache into a ContentCache object, and the constructor
parses `source.type` through the CacheSource.Type enum, which doesn't know
every type a server can return. `to_dictionary=True` avoids that on the LIST,
but the actions are broken the same way: `invalidate_caches` / `delete_caches`
call `ContentCache(connection, cache_id)` per id (mstrio/utils/cache.py,
ContentCacheMixin.__alter_status), so one unknown-type cache kills the call —
and it costs one GET per cache. So both steps go through the monitors API:

    GET   /api/monitors/caches/contents?projectId=&clusterNode=&offset=&limit=
          (monitors.get_contents_caches — the docs' "getAllCaches")
    PATCH /api/v2/monitors/caches/contents?clusterNode=
          (monitors.update_contents_caches)

Those wrappers take project_id/node explicitly and hand back the raw response,
so no enum is involved. The mstrio-py equivalent is at the bottom, commented out.

Not updateProjectStatusOnAllNodes: that sets a PROJECT's status (loaded,
unloaded, idle) on every node. It has nothing to do with caches.

Caches are addressed by `combinedId` (cache + project + node), which is why the
work is per node: an id from one node is not valid on another.

Inputs
------
    base_url       text    Library URL, e.g.
                           https://agdata.cloud.strategy.com/MicroStrategyLibrarySTD
    user_name      text    Login to authenticate as
    password       secret  Password for user_name
    login_mode     num     1=Standard (default), 16=SAML, 64=LDAP
    project_guid   text    Project GUID (the only way to name the project)
    action         text    invalidate (default) or delete
    apply_changes  text    false (default) = count the caches and stop;
                           true = actually invalidate/delete them

Workstation "unset" tokens (case-insensitive): '', '-', 'null', 'blank', 'empty'.
"""

import json
import sys

from loguru import logger
from mstrio.api import monitors as monitors_api
from mstrio.connection import Connection
from mstrio.server import Cluster

PAGE_SIZE = 1000          # the listing endpoint's maximum

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


def _as_action(value):
    action = str(value).strip().lower()
    if action not in ("invalidate", "delete"):
        raise ValueError(f"action must be invalidate or delete, not {value!r}.")
    return action


# --- Substituted by Workstation (text -> 'quoted') --------------------------
BASE_URL = _blank($base_url)                          # noqa: F821
USER_NAME = _blank($user_name)                        # noqa: F821
PASSWORD = _blank($password)                          # noqa: F821 (secret var type)
LOGIN_MODE = int(_blank($login_mode, 1))              # noqa: F821
PROJECT_GUID = _blank($project_guid)                  # noqa: F821
ACTION = _as_action(_blank($action, "invalidate"))    # noqa: F821
APPLY_CHANGES = _as_bool(_blank($apply_changes, "false"), "apply_changes")  # noqa: F821
# ----------------------------------------------------------------------------

logger.remove()
logger.add(lambda message: sys.stdout.write(message), colorize=False, level="INFO",
           format="{time:YYYY-MM-DD HH:mm:ss.SSS ZZ} | {level: <8} | {message}")


def list_caches(conn, project_id, node):
    """Every content cache for this project on this node (paged, flattened).

    The endpoint returns contentCaches as a list of ONE-KEY objects whose key is
    the cache's combined id (base64 of "<cacheId>:<type>:<projectId>") and whose
    value is the cache detail:
        "contentCaches": [ { "OEU0QTA5...": {"id": "8E4A...", "status": {...}} } ]
    There is no combinedId field inside the detail, so the key is the only place
    that id exists — and it's what the PATCH path needs. Flattened to
    {**detail, "combined_id": key}, the same shape mstrio's own
    get_contents_caches_loop produces.
    """
    caches, offset = [], 0
    while True:
        body = monitors_api.get_contents_caches(
            conn, project_id=project_id, node=node, offset=offset, limit=PAGE_SIZE).json()
        page = body.get("contentCaches") or []
        caches += [{**detail, "combined_id": cache_id}
                   for wrapper in page for cache_id, detail in wrapper.items()]
        total = body.get("total")
        if not page or (total is not None and len(caches) >= total):
            return caches
        offset += PAGE_SIZE


def operation(cache, action):
    """One PATCH operation for a cache, or None when it has no combined id."""
    combined = cache.get("combined_id")
    if not combined:
        return None
    if action == "delete":
        return {"op": "remove", "path": f"/contentCaches/{combined}"}
    return {"op": "replace", "path": f"/contentCaches/{combined}/status/invalid",
            "value": True}


def still_valid(caches):
    """Caches not yet marked invalid (how invalidation is verified)."""
    return [c for c in caches if not (c.get("status") or {}).get("invalid")]


def main():
    for name, value in (("base_url", BASE_URL), ("user_name", USER_NAME),
                        ("password", PASSWORD), ("project_guid", PROJECT_GUID)):
        if not value:
            raise ValueError(f"{name} is required.")

    mode = "APPLY" if APPLY_CHANGES else "DRY RUN — nothing will be changed"
    logger.info("Content cache {action} for project {project}; {mode}",
                action=ACTION.upper(), project=PROJECT_GUID, mode=mode)

    conn = Connection(base_url=BASE_URL, username=USER_NAME, password=PASSWORD,
                      project_id=PROJECT_GUID, login_mode=LOGIN_MODE)
    nodes = [n.get("name") for n
             in Cluster(conn).list_nodes(project_id=PROJECT_GUID, to_dictionary=True) or []
             if n.get("name")]
    logger.info("{count} node(s): {nodes}", count=len(nodes), nodes=", ".join(nodes))

    result = {"project_id": PROJECT_GUID, "action": ACTION, "applied": APPLY_CHANGES,
              "nodes": []}
    for node in nodes:
        caches = list_caches(conn, PROJECT_GUID, node)
        operations = [op for op in (operation(c, ACTION) for c in caches) if op]
        row = {"node": node, "caches": len(caches), "operations": len(operations),
               "remaining": None, "status": "dry run"}
        logger.info("  {node}: {n} cache(s), {ops} to {action}", node=node,
                    n=len(caches), ops=len(operations), action=ACTION)

        if APPLY_CHANGES and operations:
            try:
                monitors_api.update_contents_caches(
                    conn, node, {"operationList": operations})
                after = list_caches(conn, PROJECT_GUID, node)
                left = len(after) if ACTION == "delete" else len(still_valid(after))
                row["remaining"] = left
                row["status"] = "done" if left == 0 else f"{left} left"
                log = logger.success if left == 0 else logger.error
                log("  {node}: {action}d {ops} cache(s); {left} still {what}",
                    node=node, action=ACTION, ops=len(operations), left=left,
                    what="present" if ACTION == "delete" else "valid")
            except Exception as exc:  # noqa: BLE001
                row["status"] = f"error: {exc}"
                logger.error("  {node}: {action} failed: {error}", node=node,
                             action=ACTION, error=exc)
        elif APPLY_CHANGES:
            row["status"] = "nothing to do"
        result["nodes"].append(row)

    conn.close()
    total_ops = sum(r["operations"] for r in result["nodes"])
    if not APPLY_CHANGES:
        logger.info("DRY RUN: set apply_changes to true to {action} {n} cache(s).",
                    action=ACTION, n=total_ops)
    return result


# --- mstrio-py alternative (commented out: it crashes on unknown cache types) -------
#
# from mstrio.project_objects.content_cache import ContentCache
#
# # list_caches needs to_dictionary=True; without it mstrio builds ContentCache
# # objects and raises "'docreport' is not a valid CacheSource.Type".
# caches = ContentCache.list_caches(conn, project_id=PROJECT_GUID, nodes=node,
#                                   to_dictionary=True)
# cache_ids = [c["id"] for c in caches]
# # Both of these still build a ContentCache per id internally, so they hit the
# # same enum error on a server that has a 'docreport' (or other unknown) cache:
# ContentCache.invalidate_caches(conn, cache_ids=cache_ids)
# ContentCache.delete_caches(conn, cache_ids=cache_ids, force=True)
# -----------------------------------------------------------------------------------


# The host exec()s this script as a string, so the __main__ guard is unreliable.
result = main()
print(json.dumps(result, default=str))


def get_results():
    return result


if any(r["status"].startswith("error") or r["status"].endswith("left")
       for r in result["nodes"]):
    sys.exit(1)
