# Strip a dashboard's datasets, save it, then delete it (Workstation).
"""CropCanadaDashboardCleanup_Workstation — delete a dashboard that won't delete.

MicroStrategy Workstation script. Uses the injected `connectionData` session.

Some dashboards can't be deleted while they still reference datasets. This
clears the datasets, saves the dashboard over itself, and then deletes it.
One dashboard per run for now; folder-wide cleanup comes later.

Steps (each verified before the next)
-------------------------------------
  1. Read the definition: name and dataset list.
  2. Create an instance of the dashboard.
  3. PATCH the instance's datasets away, then re-read the instance and confirm
     no datasets remain.
  4. Save the instance over the existing dashboard.
  5. Re-read the SAVED definition and confirm its datasets are gone — proof the
     save took, not just the instance.
  6. Delete the instance (tidy-up; a failure here is only logged).
  7. Delete the dashboard, then confirm its definition can no longer be read.
Nothing is deleted unless every check before it passed.

mstrio-py API wrappers
----------------------
    documents.get_dashboard_hierarchy    GET  /api/v2/dossiers/{id}/definition
    documents.create_dashboard_instance  POST /api/dossiers/{id}/instances
    objects.delete_object                DEL  /api/objects/{id}?type=55

Four calls do NOT use their wrapper, each for a reason:
  * PATCH /api/dossiers/{id}/instances/{iid}/datasets — mstrio has no wrapper.
  * GET  .../instances/{iid}/definition      (get_dashboard_hierarchy_from_instance)
  * POST .../instances/{iid}/save            (overwrite_document)
  * DEL  /api/documents/{id}/instances/{iid} (delete_document_instance)
    Those three wrappers hard-code headers={'X-MSTR-ProjectID': None}, which
    removes the header and the server answers ERR006 "The header parameter
    'X-MSTR-ProjectID' is required but not provided". They go through
    conn.get/post/delete with the project header instead.

Removing the datasets
---------------------
The documented enum for that PATCH only lists addElements and replace, with no
documented remove, so the script tries these bodies in order and keeps the first
one the server accepts AND that leaves no datasets behind:
  1. {"op": "remove", "path": "/datasets"}
  2. {"op": "remove", "path": "/datasets", "value": {}}
  3. {"op": "replace", "path": "/datasets", "value": []}
  4. one {"op": "remove", "path": "/datasets/<datasetId>"} per dataset
The log says which one worked, so the list can be trimmed later.

Inputs
------
    project_name      text  Project holding the dashboard (required)
    dashboard_id      text  Dashboard (dossier) GUID (required)
    apply_changes     text  false (default) = report the datasets and stop;
                            true = clear them and save
    delete_dashboard  text  true (default) = also delete the dashboard once the
                            save is verified; false = clear and save only

Workstation "unset" tokens (case-insensitive): '', '-', 'null', 'blank', 'empty'.
"""

import json
import re
import sys
from datetime import datetime

from mstrio.api import documents, objects
from mstrio.connection import get_connection
from mstrio.types import ObjectTypes

DASHBOARD_TYPE = ObjectTypes.DOCUMENT_DEFINITION.value      # 55

_EMPTY_SENTINELS = {"", "-", "null", "blank", "empty"}
_GUID = re.compile(r"[0-9A-Fa-f]{32}")


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
PROJECT_NAME = _blank($project_name)                       # noqa: F821
DASHBOARD_ID = _blank($dashboard_id)                       # noqa: F821
APPLY_CHANGES = _as_bool(_blank($apply_changes, "false"), "apply_changes")  # noqa: F821
DELETE_DASHBOARD = _as_bool(_blank($delete_dashboard, "true"), "delete_dashboard")  # noqa: F821
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


# --- Definition and datasets -----------------------------------------------------------


def _datasets(definition):
    """[{'id','name'}] from a dashboard definition."""
    return [{"id": d.get("id"), "name": d.get("name")}
            for d in (definition or {}).get("datasets") or [] if isinstance(d, dict)]


def _project_headers(conn):
    """The header the instance endpoints require (mstrio's wrappers send None)."""
    return {"X-MSTR-ProjectID": conn.project_id}


def _checked(response, what):
    """The response body, or an error naming the call and the server's answer."""
    if not response.ok:
        raise RuntimeError(f"{what} failed: HTTP {response.status_code} "
                           f"{response.text[:300]}")
    return response.json() if response.content else {}


def read_definition(conn, dashboard_id):
    """The saved dashboard definition."""
    return documents.get_dashboard_hierarchy(conn, id=dashboard_id).json() or {}


def read_instance_definition(conn, dashboard_id, instance_id):
    """The instance's definition — what the PATCH actually changed."""
    response = conn.get(
        endpoint=f"/api/v2/dossiers/{dashboard_id}/instances/{instance_id}/definition",
        headers=_project_headers(conn))
    return _checked(response, "reading the instance definition") or {}


def patch_bodies(dataset_ids):
    """The PATCH bodies to try, in order (see "Removing the datasets" above)."""
    return [
        ("remove /datasets",
         {"operationList": [{"op": "remove", "path": "/datasets"}]}),
        ("remove /datasets with empty value",
         {"operationList": [{"op": "remove", "path": "/datasets", "value": {}}]}),
        ("replace /datasets with []",
         {"operationList": [{"op": "replace", "path": "/datasets", "value": []}]}),
        ("remove each /datasets/<id>",
         {"operationList": [{"op": "remove", "path": f"/datasets/{ds_id}"}
                            for ds_id in dataset_ids]}),
    ]


def clear_datasets(conn, dashboard_id, instance_id, dataset_ids):
    """Try each body until the instance has no datasets left.

    Returns the description of the body that worked; raises with every attempt
    listed when none does. There is no mstrio wrapper for this PATCH.
    """
    endpoint = f"/api/dossiers/{dashboard_id}/instances/{instance_id}/datasets"
    attempts = []
    for label, body in patch_bodies(dataset_ids):
        response = conn.patch(endpoint=endpoint, json=body,
                              headers=_project_headers(conn))
        if not response.ok:
            attempts.append(f"{label}: HTTP {response.status_code} {response.text[:200]}")
            continue
        left = _datasets(read_instance_definition(conn, dashboard_id, instance_id))
        if not left:
            logger.info("  datasets cleared with: {label}", label=label)
            return label
        attempts.append(f"{label}: accepted but {len(left)} dataset(s) remain")
    raise RuntimeError("could not clear the datasets — " + " | ".join(attempts))


# --- Deleting ---------------------------------------------------------------------------


def save_instance(conn, dashboard_id, instance_id):
    """Save the instance over the existing dashboard."""
    response = conn.post(
        endpoint=f"/api/documents/{dashboard_id}/instances/{instance_id}/save",
        headers=_project_headers(conn))
    return _checked(response, "saving the dashboard")


def delete_instance(conn, dashboard_id, instance_id):
    """Tidy up the instance. Never raises: the dashboard is already saved."""
    try:
        response = conn.delete(
            endpoint=f"/api/documents/{dashboard_id}/instances/{instance_id}",
            headers=_project_headers(conn))
        _checked(response, "deleting the instance")
        logger.info("  deleted the instance {id}", id=instance_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("  could not delete the instance {id}: {error}",
                       id=instance_id, error=exc)


def delete_dashboard(conn, dashboard_id):
    """Delete the dashboard and confirm it can no longer be read."""
    objects.delete_object(conn, id=dashboard_id, object_type=DASHBOARD_TYPE,
                          project_id=conn.project_id)
    try:
        read_definition(conn, dashboard_id)
    except Exception:  # noqa: BLE001
        return True          # the expected outcome: it's gone
    return False


# --- Main ------------------------------------------------------------------------------


def main():
    if not PROJECT_NAME:
        raise ValueError("project_name is required.")
    if not DASHBOARD_ID or not _GUID.fullmatch(str(DASHBOARD_ID).strip()):
        raise ValueError(f"dashboard_id must be a 32-character GUID, not {DASHBOARD_ID!r}.")
    dashboard_id = str(DASHBOARD_ID).strip()

    conn = get_connection(connectionData, project_name=PROJECT_NAME)  # noqa: F821
    definition = read_definition(conn, dashboard_id)
    name = definition.get("name")
    datasets = _datasets(definition)
    mode = ("APPLY" if APPLY_CHANGES else "PREVIEW — nothing will be changed")
    logger.info("Dashboard '{name}' ({id}) in {project}; {mode}; delete_dashboard={d}",
                name=name, id=dashboard_id, project=PROJECT_NAME, mode=mode,
                d=DELETE_DASHBOARD)
    logger.info("{n} dataset(s):", n=len(datasets))
    for dataset in datasets:
        logger.info("  {name} ({id})", name=dataset["name"], id=dataset["id"])

    result = {"dashboard": {"id": dashboard_id, "name": name},
              "datasets": datasets, "applied": APPLY_CHANGES,
              "cleared_with": None, "saved": False, "deleted": False}
    if not APPLY_CHANGES:
        logger.info("PREVIEW: set apply_changes to true to clear these dataset(s), save, "
                    "and {what}.", what="delete the dashboard" if DELETE_DASHBOARD
                    else "stop before deleting")
        return result

    instance_id = (documents.create_dashboard_instance(
        conn, dashboard_id=dashboard_id, body={}).json() or {}).get("mid")
    if not instance_id:
        raise RuntimeError("creating the dashboard instance returned no instance id")
    logger.info("Created instance {id}", id=instance_id)

    try:
        if datasets:
            result["cleared_with"] = clear_datasets(
                conn, dashboard_id, instance_id, [d["id"] for d in datasets])
        else:
            logger.info("  no datasets to clear")

        save_instance(conn, dashboard_id, instance_id)
        still_there = _datasets(read_definition(conn, dashboard_id))
        if still_there:
            raise RuntimeError(f"saved, but the dashboard still lists {len(still_there)} "
                               f"dataset(s) — not deleting it")
        result["saved"] = True
        logger.success("Saved '{name}' with no datasets.", name=name)
    finally:
        delete_instance(conn, dashboard_id, instance_id)

    if not DELETE_DASHBOARD:
        logger.info("delete_dashboard is false — leaving '{name}' in place.", name=name)
        return result

    result["deleted"] = delete_dashboard(conn, dashboard_id)
    if result["deleted"]:
        logger.success("Deleted dashboard '{name}' ({id}).", name=name, id=dashboard_id)
    else:
        logger.error("Delete reported success but '{name}' ({id}) can still be read.",
                     name=name, id=dashboard_id)
    return result


# The host exec()s this script as a string, so the __main__ guard is unreliable.
result = main()
print(json.dumps(result, default=str))


def get_results():
    return result


if result["applied"] and not result["saved"]:
    sys.exit(1)
if result["applied"] and DELETE_DASHBOARD and not result["deleted"]:
    sys.exit(1)
