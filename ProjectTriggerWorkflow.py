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

    $project_id        text    Project GUID          e.g. 3614601D9F9144D384FD3ABBFE2AC0F2
    $dashboard_id   text    Dashboard GUID        e.g. 92D04B0F1140547CC9F5D9B64868B052
    $chapter        text    Chapter key           default K36
    $visualization  text    Visualization key     default K52
    $datamart_id    text    Datamart/Report GUID  e.g. 35FF9EB541DDF18C483275AAF3C87E8A
    $workflow_config_json text JSON specifying the workflow configuration.
    $workflow_only  text    Specify to run only the workflow items default False
    $config_name    text    Identifies THIS configuration in logs, e.g. 'CropCanada_CacheDrop'
    $log_level      text    DEBUG | INFO | WARNING | ERROR (default INFO)

--------------------------------------------------------------------------------
Logging (MCE Planned Tasks)
--------------------------------------------------------------------------------
Each Planned Task carries its own variable set, so $config_name is what makes
logging per-configuration. Every line is tagged:

    2026-07-20 14:03:11 | INFO    | CropCanada_CacheDrop | 20260720T140311Z-a1b2c3 | ...
                                    ^ $config_name         ^ RUN_ID (unique per run)

Two sinks:
  * stdout          — captured in the Planned Task run history.
  * rotating file   — LOG_DIR/<config_name>.log (5 MB x 5 backups). Durable
                      history across runs, and the only place DEBUG lines land
                      if the task history truncates. Best-effort: if LOG_DIR is
                      not writable the run continues on stdout alone.

Plus a machine-readable report per run: LOG_DIR/<config_name>_<RUN_ID>.json,
containing the step-by-step outcome, timings, and any error. On failure the
report still lists the steps that DID complete, which is what you need to know
before re-running (e.g. caches already invalidated, events already fired).

LOG_DIR defaults to /opt/mstr/ContainerState/Logs/ProjectTriggerWorkflow and can
be overridden with the PTW_LOG_DIR environment variable. Set $log_level to DEBUG
to add datamart poll ticks and the full result payload.

The script exits non-zero on failure so the Planned Task is marked failed rather
than silently completed.

--------------------------------------------------------------------------------
Connection retry
--------------------------------------------------------------------------------
Unlike an interactive Workstation run (which borrows an already-open, already
-warm session), a Planned Task has no live session to reuse — it independently
authenticates via the injected `connectionData` object on every run. If the MCE
environment was idle/suspended and this trigger is what wakes it, the Library
service's internal DNS registration can lag behind the rest of the environment
coming up, so the very first connection attempt can fail with
`requests.exceptions.ConnectionError` (DNS "Name or service not known") before
the environment finishes waking.

`connect_with_retry()` retries ONLY that class of failure (ConnectionError /
Timeout), up to CONNECT_MAX_ATTEMPTS times with CONNECT_RETRY_DELAY_SECONDS
between attempts, then raises the last error. It does not mask a genuinely
wrong or permanently stale connectionData host — it only rides through a
transient startup window.

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
               .list_caches(conn, project_id=PROJECTID, status=STATUS,
               to_dictionary=True) to gather cache ids, then
               .delete_caches(conn, cache_ids=[...], force=True)
      projectID   (required)  project GUID whose caches to delete
      status      (optional)  cache status filter for list_caches (default 'ready')
      waitSeconds (optional)  seconds to pause AFTER deleting (default 0)

Example WORKFLOW_CONFIG_JSON:
    [
      {"type": "EventTrigger",           "eventID": "1ABCDEF...", "waitSeconds": 0},
      {"type": "ContentCacheInvalidate", "projectID": "3DKEJD...", "waitSeconds": 50},
      {"type": "EventTrigger",           "eventID": "4HIJ...",     "waitSeconds": 30}
    ]
"""

import json
import logging
import logging.handlers
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone

import requests

from mstrio.connection import get_connection
from mstrio.distribution_services.event import Event
from mstrio.project_objects.content_cache import ContentCache

# --- Substituted by Workstation (text -> 'quoted') --------------------------
PROJECT_ID = $project_id
DASHBOARD_ID = $dashboard_id
CHAPTER_KEY = $chapter
VISUALIZATION_KEY = $visualization
DATAMART_ID = $datamart_id
CONFIG_NAME = $config_name
LOG_LEVEL = $log_level
# ----------------------------------------------------------------------------

# Where per-configuration log files are written. Override with the PTW_LOG_DIR
# env var. /opt/mstr/ContainerState is the MCE container path that survives
# restarts (same volume used for file subscriptions) — verify it is writable in
# your environment. File logging is best-effort: if this path is unavailable the
# run continues with stdout logging only.
LOG_DIR = os.environ.get(
    'PTW_LOG_DIR', '/opt/mstr/ContainerState/Logs/ProjectTriggerWorkflow'
)
LOG_MAX_BYTES = 5_000_000
LOG_BACKUP_COUNT = 5

# Correlates every line of one execution, and names that run's JSON report.
RUN_ID = (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
          + '-' + uuid.uuid4().hex[:6])

log = logging.getLogger('ProjectTriggerWorkflow')

# The attribute name on the visualization that carries the Load History IDs.
# Do NOT change this unless the dashboard's attribute display name changes.
LOAD_HISTORY_ATTR_NAME = 'LoadHistoryID'

# Datamart job polling. Instance status 5 = executing/running; 1 = complete.
# Adjust RUNNING_STATUSES if your server reports a different in-progress code.
RUNNING_STATUSES     = {5}
POLL_INTERVAL_SECONDS = 3
POLL_TIMEOUT_SECONDS  = 300

# Connection retry — rides through the wake-from-idle DNS-propagation window
# described above. 6 attempts * 20s = up to ~2 minutes of tolerance before
# the initial connection attempt gives up and the run fails for real.
CONNECT_MAX_ATTEMPTS       = 6
CONNECT_RETRY_DELAY_SECONDS = 20

# Standalone toggle — set True to run ONLY the configured workflow steps and
# skip the dashboard/datamart entirely (useful for testing Event/Cache actions
# in isolation). When False, the normal dashboard -> workflow -> datamart flow
# runs and the workflow is gated on the visualization returning rows.
#WORKFLOW_ONLY = False
WORKFLOW_ONLY = $workflow_only

# Configurable workflow steps — edit this JSON array. See the module docstring
# for the schema. Runs only when the dashboard visualization returns rows.


WORKFLOW_CONFIG_JSON = $workflow_config_json


# ---------------------------------------------------------------------------
# Logging — per configuration, dual sink (stdout + rotating file)
# ---------------------------------------------------------------------------
def _safe_name(value):
    """Reduce a config name to something safe for a filename."""
    return re.sub(r'[^A-Za-z0-9._-]+', '_', str(value or 'unnamed')).strip('_')


def setup_logging():
    """Configure the module logger. Stdout is captured by the Planned Task run
    history; the file sink gives durable, per-configuration history.

    Uses the stdlib `logging` module rather than loguru: loguru is not
    guaranteed to be installed in the MCE Python runtime, and a missing import
    would fail the task before anything useful is logged.
    """
    log.setLevel(getattr(logging, str(LOG_LEVEL).upper(), logging.INFO))
    log.handlers.clear()   # re-runs in a warm interpreter must not double-log
    log.propagate = False

    fmt = logging.Formatter(
        fmt=f'%(asctime)s | %(levelname)-7s | {CONFIG_NAME} | {RUN_ID} | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    log.addHandler(stream)

    # Best-effort file sink — a read-only or missing path must not kill the run.
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        path = os.path.join(LOG_DIR, f'{_safe_name(CONFIG_NAME)}.log')
        rotating = logging.handlers.RotatingFileHandler(
            path, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT,
            encoding='utf-8',
        )
        rotating.setFormatter(fmt)
        log.addHandler(rotating)
        log.info('File logging enabled: %s', path)
    except Exception as exc:  # noqa: BLE001 - logging must never be fatal
        log.warning('File logging disabled (%s): %s', LOG_DIR, exc)

    return log


def write_run_report(result):
    """Write the run's structured result as JSON for machine-readable history."""
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        path = os.path.join(
            LOG_DIR, f'{_safe_name(CONFIG_NAME)}_{RUN_ID}.json'
        )
        with open(path, 'w', encoding='utf-8') as handle:
            json.dump(result, handle, indent=2, default=str)
        log.info('Run report written: %s', path)
    except Exception as exc:  # noqa: BLE001
        log.warning('Could not write run report: %s', exc)


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
class WorkflowStepError(RuntimeError):
    """Raised when a workflow step fails, carrying the partial step log so the
    run report still shows which steps completed before the failure."""

    def __init__(self, step_number, step_type, partial_results):
        super().__init__(f'Workflow step {step_number} ({step_type}) failed')
        self.step_number = step_number
        self.step_type = step_type
        self.partial_results = partial_results


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


def _list_cache_ids(conn, project_id, status=None):
    """List cache ids as raw dicts (to_dictionary=True).

    Building ContentCache objects fails on cache source types the installed
    enum doesn't know (e.g. 'docreport' -> CacheSource.Type ValueError), so we
    stay in dict form and only pull the ids. Omitting status uses the library
    default ('ready').
    """
    list_kwargs = {'project_id': project_id, 'to_dictionary': True}
    if status:
        list_kwargs['status'] = status
    caches = ContentCache.list_caches(conn, **list_kwargs)
    return [c['id'] for c in caches if c.get('id')]


def _process_cache_invalidate(conn, step):
    project_id = _cfg(step, 'projectID', 'project_id', required=True)
    status = _cfg(step, 'status')
    cache_ids = _list_cache_ids(conn, project_id, status)
    if cache_ids:
        ContentCache.invalidate_caches(conn, cache_ids=cache_ids)
    return f'invalidated {len(cache_ids)} cache(s) in project {project_id}'


def _process_cache_delete(conn, step):
    project_id = _cfg(step, 'projectID', 'project_id', required=True)
    status = _cfg(step, 'status')
    cache_ids = _list_cache_ids(conn, project_id, status)
    if cache_ids:
        ContentCache.delete_caches(conn, cache_ids=cache_ids, force=True)
    return f'deleted {len(cache_ids)} cache(s) in project {project_id}'


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

    total_steps = len(steps)
    log.info('Workflow: %d step(s) configured', total_steps)

    results = []
    for i, step in enumerate(steps, start=1):
        ptype = step.get('type')
        handler = _PROCESS_HANDLERS.get(ptype)
        if handler is None:
            raise ValueError(
                f'Step {i}: unknown process type {ptype!r}. '
                f'Valid types: {sorted(_PROCESS_HANDLERS)}'
            )

        log.info('[step %d/%d] %s starting — config: %s',
                 i, total_steps, ptype, json.dumps(step))
        started = time.monotonic()
        try:
            detail = handler(conn, step)
        except Exception as exc:  # noqa: BLE001 - log which step failed, then re-raise
            elapsed = round(time.monotonic() - started, 2)
            log.exception('[step %d/%d] %s FAILED after %ss: %s',
                          i, total_steps, ptype, elapsed, exc)
            results.append({'step': i, 'type': ptype, 'status': 'error',
                            'elapsed_seconds': elapsed, 'error': str(exc),
                            'config': step})
            raise WorkflowStepError(i, ptype, results) from exc

        elapsed = round(time.monotonic() - started, 2)
        log.info('[step %d/%d] %s OK in %ss — %s',
                 i, total_steps, ptype, elapsed, detail)

        wait = int(_cfg(step, 'waitSeconds', 'wait_seconds', default=0) or 0)
        if wait > 0:
            log.info('[step %d/%d] waiting %ss before next step', i, total_steps, wait)
            time.sleep(wait)

        results.append({'step': i, 'type': ptype, 'status': 'ok', 'detail': detail,
                        'elapsed_seconds': elapsed, 'wait_seconds': wait})

    log.info('Workflow complete: %d/%d step(s) succeeded', len(results), total_steps)
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
    log.info('Dashboard %s instance %s (status %s)',
             DASHBOARD_ID, instance_id, created.get('status'))

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
    log.info('Visualization %s/%s rows (data.paging.total): %s',
             CHAPTER_KEY, VISUALIZATION_KEY, total)
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
    log.info('%s elements (%d): %s', LOAD_HISTORY_ATTR_NAME, len(element_ids), element_ids)
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
            log.info('Datamart job finished with status %s', status)
            return status, False
        if time.monotonic() >= deadline:
            log.warning('Datamart job still running (status %s) after %ss — '
                        'giving up on the wait', status, POLL_TIMEOUT_SECONDS)
            return status, True
        log.debug('Datamart job running (status %s); polling again in %ss',
                  status, POLL_INTERVAL_SECONDS)
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
    log.info('Datamart %s instance %s (status %s)',
             DATAMART_ID, instance_id, created.get('status'))

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
    log.info('Prompt %s: matched %d element(s); unmatched dashboard keys: %s',
             prompt_id, len(answers), unmatched)
    if unmatched:
        log.warning('%d dashboard key(s) had no matching datamart prompt element: %s',
                    len(unmatched), unmatched)
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
    log.info('Datamart job %s submitted (status %s)',
             job.get('jobId'), job.get('status'))

    # Wait for the job to finish before cleaning up the instance
    final_status, timed_out = poll_datamart_job(conn, instance_id)

    # Clean up the instance
    _call(
        conn.delete,
        endpoint=f'/api/datamarts/{DATAMART_ID}/instances/{instance_id}',
    )
    log.info('Datamart instance %s deleted', instance_id)

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
# Connection — retry/backoff for the wake-from-idle DNS race (see docstring)
# ---------------------------------------------------------------------------
def connect_with_retry():
    """get_connection() with retry/backoff for transient connection failures.

    A Planned Task has no live Workstation session to borrow from, so it must
    independently authenticate via `connectionData` on every run. If the MCE
    environment was idle and this trigger is what wakes it, the Library
    service's internal DNS registration can lag behind the rest of the
    environment coming up, and the first attempt fails with a
    ConnectionError ("Name or service not known") before it's ready.

    This retries ONLY that class of failure (connection/timeout errors), not
    application-level errors (bad credentials, invalid project, etc.) — those
    fail immediately as before. A genuinely wrong or permanently stale
    connectionData host will still exhaust the retries and raise.
    """
    last_exc = None
    for attempt in range(1, CONNECT_MAX_ATTEMPTS + 1):
        try:
            conn = get_connection(connectionData, project_id=PROJECT_ID)  # noqa: F821
            if attempt > 1:
                log.info('Connected on attempt %d/%d', attempt, CONNECT_MAX_ATTEMPTS)
            return conn
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            last_exc = exc
            if attempt == CONNECT_MAX_ATTEMPTS:
                break
            log.warning(
                'Connection attempt %d/%d failed (%s); retrying in %ss — '
                'environment may still be waking from idle',
                attempt, CONNECT_MAX_ATTEMPTS, exc, CONNECT_RETRY_DELAY_SECONDS,
            )
            time.sleep(CONNECT_RETRY_DELAY_SECONDS)

    log.error('Could not connect after %d attempt(s): %s', CONNECT_MAX_ATTEMPTS, last_exc)
    raise last_exc


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    setup_logging()
    started = time.monotonic()

    result = {
        'config_name': CONFIG_NAME,
        'run_id': RUN_ID,
        'started_utc': datetime.now(timezone.utc).isoformat(),
        'project_id': PROJECT_ID,
        'dashboard_id': DASHBOARD_ID,
        'datamart_id': DATAMART_ID,
        'workflow_only': bool(WORKFLOW_ONLY),
        'status': 'error',          # overwritten on success
    }

    log.info('=== ProjectTriggerWorkflow start === project=%s dashboard=%s '
             'chapter=%s viz=%s datamart=%s workflow_only=%s',
             PROJECT_ID, DASHBOARD_ID, CHAPTER_KEY, VISUALIZATION_KEY,
             DATAMART_ID, WORKFLOW_ONLY)

    try:
        conn = connect_with_retry()
        log.info('Connected; project %s selected', PROJECT_ID)

        if WORKFLOW_ONLY:
            # Standalone: just run the configured workflow steps, no dashboard/datamart
            log.info('WORKFLOW_ONLY is set — running configured workflow steps only')
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
                log.info('No rows returned — workflow and datamart skipped')
                result['action'] = 'skipped (no null load history)'

        result['status'] = 'success'

    except WorkflowStepError as exc:
        # Keep the partial step log so the report shows what did complete
        result['workflow'] = exc.partial_results
        result['action'] = 'failed'
        result['error'] = str(exc)
        result['failed_step'] = {'number': exc.step_number, 'type': exc.step_type}
        log.error('Run FAILED at workflow step %s (%s)', exc.step_number, exc.step_type)
        raise
    except Exception as exc:  # noqa: BLE001
        result['action'] = 'failed'
        result['error'] = str(exc)
        log.exception('Run FAILED: %s', exc)
        raise
    finally:
        result['elapsed_seconds'] = round(time.monotonic() - started, 2)
        result['finished_utc'] = datetime.now(timezone.utc).isoformat()
        log.info('=== ProjectTriggerWorkflow end === status=%s action=%s elapsed=%ss',
                 result['status'], result.get('action'), result['elapsed_seconds'])
        log.debug('Result: %s', json.dumps(result, indent=2, default=str))
        write_run_report(result)

    return result


# Called unconditionally: the host exec()s this script as a string, so a
# `if __name__ == '__main__'` guard would not reliably fire.
try:
    main()
except Exception:  # noqa: BLE001
    # Non-zero exit so the MCE Planned Task is marked failed rather than
    # silently "completed". The traceback is already in the log above.
    sys.exit(1)
