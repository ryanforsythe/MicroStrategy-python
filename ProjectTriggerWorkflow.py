"""ProjectTriggerWorkflow — run a configurable chain of project actions driven
by a Dashboard visualization's results.

MicroStrategy Workstation script — intended to run ON the Intelligence Server
(the `workstationData` object is injected automatically by the host). It uses
the official mstrio-py connection for authentication; the dashboard/datamart
steps are raw REST against the documented Library endpoints, and the workflow
steps use mstrio-py functionality directly. No custom libraries.

Runtime variables are modeled on the mstrio-py `psycopg2_all_actions_template.py`
convention: `$`-prefixed placeholders that Workstation substitutes at run time
(text -> quoted string). Define these in the script's variable panel:

    $project        text    Project GUID          e.g. 3614601D9F9144D384FD3ABBFE2AC0F2
    $dashboard_id   text    Dashboard GUID        e.g. 92D04B0F1140547CC9F5D9B64868B052
    $chapter        text    Chapter key           default K36
    $visualization  text    Visualization key     default K52
    $datamart_id    text    Datamart/Report GUID  e.g. 35FF9EB541DDF18C483275AAF3C87E8A

Overall flow:
    1. Create a Dashboard instance and read the target visualization's grid.
    2. If data.paging.total > 0, collect the LoadHistoryID element IDs.
    3. >>> Run the configurable WORKFLOW_CONFIG_JSON steps (sequentially). <<<
    4. Create a Datamart (report) instance, answer its prompt from the dashboard
       IDs, execute the job, poll to completion, then delete the instance.
    Steps 3 and 4 only run when total > 0.

>>> IMPORTANT: the row on the visualization MUST be named 'LoadHistoryID'.
    This script locates the values by that NAME, not by attribute GUID, so the
    attribute display name on the dashboard must not be changed.

--------------------------------------------------------------------------------
Configurable workflow (WORKFLOW_CONFIG_JSON)
--------------------------------------------------------------------------------
Read between the dashboard check and the datamart execution, and ONLY when the
visualization returned rows (total > 0). Steps run sequentially in list order.

The config is a JSON ARRAY (not an object) so the same process type can appear
multiple times and order is preserved — a JSON object cannot hold duplicate keys
like two "EventTrigger" entries. Each array element:

    {"type": "<ProcessType>", ...type-specific fields..., "waitSeconds": <int>}

Process types (all mstrio-py driven):

  EventTrigger
      module : mstrio.distribution_services.event.Event(conn, id=EVENTID).trigger()
      eventID     (required)  event GUID to trigger
      waitSeconds (optional)  seconds to pause AFTER triggering (default 0)

  ContentCacheInvalidate
      module : mstrio.project_objects.content_cache.ContentCache
               .list_caches(conn, project_id=PROJECTID, status=STATUS) to gather
               cache ids, then .invalidate_caches(conn, cache_ids=[...])
      projectID   (required)  project GUID whose caches to invalidate
      status      (optional)  cache status filter for list_caches (default 'ready')
      waitSeconds (optional)  seconds to pause AFTER invalidating (default 0)

  ContentCacheDelete
      module : mstrio.project_objects.content_cache.ContentCache
               .delete_all_caches(conn, project_id=PROJECTID, status=STATUS, force=True)
      projectID   (required)  project GUID whose caches to delete
      status      (optional)  cache status filter (default: all)
      waitSeconds (optional)  seconds to pause AFTER deleting (default 0)

Example WORKFLOW_CONFIG_JSON:
    [
      {"type": "EventTrigger",           "eventID": "1ABCDEF...", "waitSeconds": 0},
      {"type": "ContentCacheInvalidate", "projectID": "3DKEJD...", "waitSeconds": 50},
      {"type": "EventTrigger",           "eventID": "4HIJ...",     "waitSeconds": 30}
    ]
"""

import json
import time

from mstrio.connection import get_connection
from mstrio.distribution_services.event import Event
from mstrio.project_objects.content_cache import ContentCache

# --- Substituted by Workstation (text -> 'quoted') --------------------------
PROJECT_ID        = $project
DASHBOARD_ID      = $dashboard_id
CHAPTER_KEY       = $chapter
VISUALIZATION_KEY = $visualization
DATAMART_ID       = $datamart_id
# ----------------------------------------------------------------------------

# The attribute name on the visualization that carries the Load History IDs.
# Do NOT change this unless the dashboard's attribute display name changes.
LOAD_HISTORY_ATTR_NAME = 'LoadHistoryID'

# Datamart job polling. Instance status 5 = executing/running; 1 = complete.
# Adjust RUNNING_STATUSES if your server reports a different in-progress code.
RUNNING_STATUSES     = {5}
POLL_INTERVAL_SECONDS = 3
POLL_TIMEOUT_SECONDS  = 300

# Standalone toggle — set True to run ONLY the configured workflow steps and
# skip the dashboard/datamart entirely (useful for testing Event/Cache actions
# in isolation). When False, the normal dashboard -> workflow -> datamart flow
# runs and the workflow is gated on the visualization returning rows.
WORKFLOW_ONLY = False

# Configurable workflow steps — edit this JSON array. See the module docstring
# for the schema. Runs only when the dashboard visualization returns rows.
WORKFLOW_CONFIG_JSON = '''
[
  {"type": "EventTrigger",           "eventID": "REPLACE_WITH_EVENT_GUID",   "waitSeconds": 0},
  {"type": "ContentCacheInvalidate", "projectID": "REPLACE_WITH_PROJECT_GUID", "status": "ready", "waitSeconds": 50},
  {"type": "ContentCacheDelete",     "projectID": "REPLACE_WITH_PROJECT_GUID", "status": "ready", "waitSeconds": 0}
]
'''


# ---------------------------------------------------------------------------
# REST helpers (auth handled by the mstrio Connection; project header explicit)
# ---------------------------------------------------------------------------
def _call(method, endpoint, **kwargs):
    """Invoke a Connection REST method, forcing the project header, raise on error."""
    headers = kwargs.pop('headers', {}) or {}
    headers.setdefault('X-MSTR-ProjectID', PROJECT_ID)
    resp = method(endpoint=endpoint, headers=headers, **kwargs)
    if not resp.ok:
        raise RuntimeError(
            f'{resp.status_code} {resp.reason} on {endpoint}\n{resp.text}'
        )
    return resp


def _instance_id(body):
    """Instance id field differs by object type (reports: instanceId, dossiers: mid)."""
    for key in ('instanceId', 'mid', 'id'):
        if body.get(key):
            return body[key]
    raise RuntimeError(f'No instance id in response: {body}')


# ---------------------------------------------------------------------------
# Configurable workflow processes (mstrio-py driven)
# ---------------------------------------------------------------------------
def _cfg(step, *names, required=False, default=None):
    """Read the first present, non-empty field from a step config (alias-tolerant)."""
    for name in names:
        if step.get(name) not in (None, ''):
            return step[name]
    if required:
        raise ValueError(f'Missing required field {names[0]!r} in step: {step}')
    return default


def _process_event_trigger(conn, step):
    event_id = _cfg(step, 'eventID', 'event_id', required=True)
    Event(conn, id=event_id).trigger()
    return f'triggered event {event_id}'


def _process_cache_invalidate(conn, step):
    project_id = _cfg(step, 'projectID', 'project_id', required=True)
    status = _cfg(step, 'status')  # None -> list_caches default ('ready')
    list_kwargs = {'project_id': project_id}
    if status:
        list_kwargs['status'] = status
    caches = ContentCache.list_caches(conn, **list_kwargs)
    cache_ids = [c.id for c in caches]
    if cache_ids:
        ContentCache.invalidate_caches(conn, cache_ids=cache_ids)
    return f'invalidated {len(cache_ids)} cache(s) in project {project_id}'


def _process_cache_delete(conn, step):
    project_id = _cfg(step, 'projectID', 'project_id', required=True)
    status = _cfg(step, 'status')
    # project_id / status passed as **filters; force=True avoids a confirm prompt.
    del_kwargs = {'project_id': project_id, 'force': True}
    if status:
        del_kwargs['status'] = status
    ContentCache.delete_all_caches(conn, **del_kwargs)
    return f'deleted caches in project {project_id}'


_PROCESS_HANDLERS = {
    'EventTrigger': _process_event_trigger,
    'ContentCacheInvalidate': _process_cache_invalidate,
    'ContentCacheDelete': _process_cache_delete,
}


def run_workflow(conn, config_json):
    """Execute the configured workflow steps sequentially; return a per-step log."""
    steps = json.loads(config_json)
    if not isinstance(steps, list):
        raise ValueError('WORKFLOW_CONFIG_JSON must be a JSON array of step objects')

    results = []
    for i, step in enumerate(steps, start=1):
        ptype = step.get('type')
        handler = _PROCESS_HANDLERS.get(ptype)
        if handler is None:
            raise ValueError(
                f'Step {i}: unknown process type {ptype!r}. '
                f'Valid types: {sorted(_PROCESS_HANDLERS)}'
            )
        print(f'[workflow {i}/{len(steps)}] {ptype} ...')
        detail = handler(conn, step)
        wait = int(_cfg(step, 'waitSeconds', 'wait_seconds', default=0) or 0)
        print(f'    {detail}; waiting {wait}s before next step')
        if wait > 0:
            time.sleep(wait)
        results.append({'step': i, 'type': ptype, 'detail': detail, 'wait_seconds': wait})
    return results


# ---------------------------------------------------------------------------
# Dashboard instance -> visualization -> Load History IDs
# ---------------------------------------------------------------------------
def get_dashboard_load_history(conn):
    """Return (total, [element_ids]) from the LoadHistoryID row of the viz."""
    # Create a dashboard instance
    created = _call(
        conn.post,
        endpoint=f'/api/dossiers/{DASHBOARD_ID}/instances',
    ).json()
    instance_id = _instance_id(created)
    print(f'Dashboard instance: {instance_id} (status {created.get("status")})')

    # Get the visualization grid
    viz = _call(
        conn.get,
        endpoint=(
            f'/api/v2/dossiers/{DASHBOARD_ID}/instances/{instance_id}'
            f'/chapters/{CHAPTER_KEY}/visualizations/{VISUALIZATION_KEY}'
        ),
        params={'offset': 0, 'limit': 1000, 'columnOffset': -1, 'columnLimit': -1},
    ).json()

    # Check the paging total before doing any work
    total = viz.get('data', {}).get('paging', {}).get('total', 0)
    print(f'Visualization rows (data.paging.total): {total}')
    if total <= 0:
        return total, []

    # Locate the LoadHistoryID row BY NAME, collect its element IDs
    rows = viz.get('definition', {}).get('grid', {}).get('rows', [])
    load_row = next(
        (r for r in rows if r.get('name') == LOAD_HISTORY_ATTR_NAME), None
    )
    if load_row is None:
        raise RuntimeError(
            f"Row named '{LOAD_HISTORY_ATTR_NAME}' not found on the visualization"
        )

    element_ids = [el['id'] for el in load_row.get('elements', []) if el.get('id')]
    print(f'LoadHistoryID elements: {element_ids}')
    return total, element_ids


# ---------------------------------------------------------------------------
# Datamart resolve prompts -> answer -> execute -> poll -> delete
# ---------------------------------------------------------------------------
def _element_key(element_id):
    """The matching key is the element prefix before ';' (e.g. 'h232')."""
    return element_id.split(';', 1)[0]


def poll_datamart_job(conn, instance_id):
    """Poll the datamart instance until it leaves a running status or times out.

    Returns (final_status, timed_out). GETs the instance status; if your server
    exposes job status under a different path, adjust the endpoint here.
    """
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    while True:
        body = _call(
            conn.get,
            endpoint=f'/api/datamarts/{DATAMART_ID}/instances/{instance_id}',
        ).json()
        status = body.get('status')
        if status not in RUNNING_STATUSES:
            print(f'Datamart job finished with status {status}')
            return status, False
        if time.monotonic() >= deadline:
            print(f'Datamart job still running (status {status}) after '
                  f'{POLL_TIMEOUT_SECONDS}s — giving up on the wait')
            return status, True
        print(f'Datamart job running (status {status}); '
              f'polling again in {POLL_INTERVAL_SECONDS}s...')
        time.sleep(POLL_INTERVAL_SECONDS)


def run_datamart(conn, dashboard_element_ids):
    """Resolve prompts, answer from the dashboard IDs, execute, poll, and clean up."""
    # Create a report instance stopped at prompt resolution
    created = _call(
        conn.post,
        endpoint=f'/api/v2/reports/{DATAMART_ID}/instances',
        params={'offset': 0, 'limit': 1000, 'executionStage': 'resolve_prompts'},
    ).json()
    instance_id = _instance_id(created)
    print(f'Datamart instance: {instance_id} (status {created.get("status")})')

    # Read the prompt(s) on this instance
    prompts = _call(
        conn.get,
        endpoint=f'/api/reports/{DATAMART_ID}/instances/{instance_id}/prompts',
    ).json()
    if not prompts:
        raise RuntimeError('Datamart returned no prompts to answer')
    prompt = prompts[0]
    prompt_id, prompt_type = prompt['id'], prompt.get('type', 'ELEMENTS')

    # Read the available prompt elements and match to the dashboard IDs.
    # Attribute GUIDs differ between dashboard and datamart, so match on the
    # element key prefix (h232, h233, ...), not the full id.
    dash_keys = {_element_key(eid) for eid in dashboard_element_ids}
    elements = _call(
        conn.get,
        endpoint=(
            f'/api/reports/{DATAMART_ID}/instances/{instance_id}'
            f'/prompts/{prompt_id}/elements'
        ),
        params={'offset': 0, 'limit': 100},
    ).json().get('elements', [])

    answers = [{'id': el['id']} for el in elements if _element_key(el['id']) in dash_keys]
    matched_keys = {_element_key(el['id']) for el in elements if _element_key(el['id']) in dash_keys}
    unmatched = sorted(dash_keys - matched_keys)
    print(f'Matched {len(answers)} prompt element(s); unmatched dashboard keys: {unmatched}')
    if not answers:
        raise RuntimeError('No datamart prompt elements matched the dashboard IDs')

    # Submit the prompt answers
    _call(
        conn.put,
        endpoint=f'/api/reports/{DATAMART_ID}/instances/{instance_id}/prompts/answers',
        json={'prompts': [{'id': prompt_id, 'type': prompt_type, 'answers': answers}]},
    )

    # Execute the datamart job (note: 'datamarts' root, same instance id)
    job = _call(
        conn.post,
        endpoint=f'/api/datamarts/{DATAMART_ID}/instances/{instance_id}/execution/jobs',
    ).json()
    print(f'Datamart job: {job.get("jobId")} (status {job.get("status")})')

    # Wait for the job to finish before cleaning up the instance
    final_status, timed_out = poll_datamart_job(conn, instance_id)

    # Clean up the instance
    _call(
        conn.delete,
        endpoint=f'/api/datamarts/{DATAMART_ID}/instances/{instance_id}',
    )
    print('Datamart instance deleted.')

    return {
        'instance_id': instance_id,
        'prompt_id': prompt_id,
        'answers': [a['id'] for a in answers],
        'unmatched_dashboard_keys': unmatched,
        'job_id': job.get('jobId'),
        'submit_status': job.get('status'),
        'final_status': final_status,
        'timed_out': timed_out,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    conn = get_connection(workstationData)  # noqa: F821  (injected by Workstation)
    conn.select_project(project_id=PROJECT_ID)

    result = {'project_id': PROJECT_ID, 'dashboard_id': DASHBOARD_ID,
              'datamart_id': DATAMART_ID}
    try:
        if WORKFLOW_ONLY:
            # Standalone: just run the configured workflow steps, no dashboard/datamart
            print('WORKFLOW_ONLY=True — running configured workflow steps only')
            result['workflow'] = run_workflow(conn, WORKFLOW_CONFIG_JSON)
            result['action'] = 'workflow-only'
        else:
            total, element_ids = get_dashboard_load_history(conn)
            result['load_history_total'] = total
            result['load_history_element_ids'] = element_ids

            if total > 0:
                # Configurable workflow runs between the dashboard check and datamart
                result['workflow'] = run_workflow(conn, WORKFLOW_CONFIG_JSON)
                result['datamart'] = run_datamart(conn, element_ids)
                result['action'] = 'executed'
            else:
                result['action'] = 'skipped (no null load history)'
    finally:
        conn.close()

    print(json.dumps(result, indent=2))
    return result


main()
