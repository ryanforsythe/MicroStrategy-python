# System Manager ClearCache (Workstation): check pending loads, refresh and verify cubes, mark loads, notify.
"""SystemManager_ClearCache_Workstation — refresh cubes for pending MIDWH loads.

MicroStrategy Workstation version of SystemManager_ClearCache.py. Combines
ExecuteDatasourceQuery_Workstation and CubeRefreshVerify_Workstation.
Connects through the injected `connectionData` session. Safe to schedule every
few minutes: a run that starts while another is still working exits quietly.

Flow
----
  1. Release in-progress marks older than MARK_STALE_MINUTES. These are left by
     a run that stopped without cleaning up; the administrator is emailed.
  2. If any load is still marked, another run is working: exit quietly.
  3. Read the pending loads (cache timestamp IS NULL) from the load history
     table. If there are none, exit cleanly.
  4. Check that the load ids are integers and that their count matches.
  5. Mark those loads as in progress, so any run that starts now skips (step 2).
  6. Invalidate all report/content caches, trigger the refresh event, and verify
     every cube-subscription job it starts: the cube cache's `last_update_job`
     must equal that job's id.
  7. Replace the marks with the real cache timestamp and confirm it took.
  8. Trigger the completion event.

If anything fails after step 5, the marks are removed so the next run retries
those loads. Any failure emails the administrator and exits non-zero. Loads that
arrive while a run is working aren't marked, so the next free run picks them up.

Table and columns
-----------------
All SQL is built from three inputs, so there is no SQL to type:
    load_history_table      e.g. BPIPlus.MicroStrategy.MIDWHLoadHistory_Test
    load_id_column          e.g. MIDWHLoadHistoryId
    cache_timestamp_column  e.g. DropMSCacheDataTime
Names may use letters, digits and underscores; wrap a name in [brackets] if it
has other characters. The table may have up to four dotted parts. Anything else
stops the run before it connects, so a typo never reaches the database. Each
UPDATE is logged when it runs.

Running UPDATEs through the datasource SQL-execution API has not been tested
here. A statement that returns no rows may be reported as an error, so every
write is confirmed with a follow-up SELECT instead of trusting its response.

In-progress marks
-----------------
While a run is working, its loads get a cache timestamp of "now minus 100 years":
a run at 2026-09-14 10:05 writes 1926-09-14 10:05. Real cache timestamps are
never earlier than RefreshMIDWHDateTime, so any value before 2000-01-01 is a
mark, and adding the 100 years back tells how old it is. The pending-loads query
only picks up loads whose cache timestamp IS NULL, so it never sees marked loads.

Anything else that reads the cache timestamp column (dashboards, reports) sees
marked loads as non-NULL, with a 1926 date, while a run is working. Two runs that
start within the same few seconds could both mark the same loads; the worst case
is an extra refresh and a failure email, not wrong data.

Logging
-------
Every line goes to stdout through loguru with a timestamp, for example
    2026-09-16 14:05:03.217 +0000 | INFO     | --- connecting ---
mstrio's own messages (it uses the logging module) are passed through the same
format. If loguru isn't installed in the runtime, the same format is printed
without it.

Inputs
------
    project_name              text    Project to connect to (required)
    datasource_instance_id    text    Datasource instance GUID (id or name required)
    datasource_instance_name  text    Datasource instance name (id or name required)
    load_history_table        text    Load history table (required)
    load_id_column            text    Load id column (required)
    cache_timestamp_column    text    Column set when the cache is dropped (required)
    event_id                  text    Refresh event GUID (required)
    completion_event_id       text    Event fired after the cache timestamp is set.
                                      Default D5B52B8E4F6376E252F4AA91F6BF66A9.
    notification_users        list    Who to notify: MicroStrategy user GUIDs or
                                      logins, e.g. ['67A551A4461CA9F60A16C19ADC4D6107',
                                      'AltusAdministrators.com']. A text value
                                      (comma/semicolon/new-line separated) works
                                      too. Blank = no notifications.

Connects only through the injected `connectionData` session, which runs as the
subscriber. The credential login (user_name, password, base_url, login_mode) is
commented out below. Those lines have no dollar sign, because Workstation
substitutes variables even inside comments; restore it before uncommenting.

Notifications
-------------
Sent through Library's own email service with mstrio's send_email(), so no SMTP
host, port or credentials are needed — the environment's email configuration and
each user's address are used. Messages are HTML: a bold subject line and the
detail in a <pre> block, which keeps SQL, load ids and tracebacks readable.

send_email() needs the connection, so a failure BEFORE the connection is made
(for example, the Library being unreachable) can only be logged. Every
notification attempt is logged as sent or not sent.

Workstation "unset" tokens (case-insensitive): '', '-', 'null', 'blank', 'empty'.
"""

import logging
import re
import sys
import time
import traceback
from datetime import datetime
from html import escape

import requests
#from mstrio.connection import get_connection  # Connection: credential login (disabled)
from mstrio.connection import Connection
from mstrio.datasources.datasource_instance import DatasourceInstance
from mstrio.distribution_services import Event
from mstrio.distribution_services.email import send_email
from mstrio.distribution_services.subscription import list_subscriptions
from mstrio.users_and_groups import User
from mstrio.project_objects import list_cube_caches
from mstrio.api import monitors as monitors_api
from mstrio.project_objects.content_cache import ContentCache
from mstrio.server import Cluster, JobStatus, JobType, ObjectType, Project, list_jobs
from mstrio.utils.encoder import Encoder
from mstrio.utils.response_processors import datasources as ds_processors

# --- Editable configuration --------------------------------------------------
QUERY_TIMEOUT_SECONDS = 300         # per SQL statement (these queries are small)
JOB_TIMEOUT_SECONDS = 3600          # cube refresh jobs, end to end
POLL_SECONDS = 15                   # between job-monitor polls
START_GRACE_SECONDS = 180           # wait this long for refresh jobs to appear
CONNECT_MAX_ATTEMPTS = 6            # connection retries for a cold environment
CONNECT_RETRY_DELAY_SECONDS = 20
# ----------------------------------------------------------------------------

# --- Logging ----------------------------------------------------------------------
LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS ZZ} | {level: <8} | {message}"
LOG_LEVEL = "INFO"


def _stdout_sink(message):
    sys.stdout.write(message)
    sys.stdout.flush()


try:
    from loguru import logger
except ImportError:  # loguru isn't installed in this runtime
    logger = None

if logger is not None:
    logger.remove()
    logger.add(_stdout_sink, format=LOG_FORMAT, level=LOG_LEVEL, colorize=False)

    class _ToLoguru(logging.Handler):
        """Passes mstrio's own log records through loguru, so they carry the
        same timestamp."""

        def emit(self, record):
            try:
                level = logger.level(record.levelname).name
            except ValueError:
                level = record.levelno
            logger.opt(exception=record.exc_info).log(level, record.getMessage())

    logging.getLogger("mstrio").handlers = [_ToLoguru()]
else:
    class _TimestampLogger:
        """Stand-in with loguru's calls (info, warning, error, success, ...),
        printed in the same format."""

        def __getattr__(self, level):
            def log(message, *args, **kwargs):
                if args or kwargs:
                    message = message.format(*args, **kwargs)
                now = datetime.now().astimezone()
                print(f"{now:%Y-%m-%d %H:%M:%S}.{now.microsecond // 1000:03d} {now:%z} | "
                      f"{level.upper():<8} | {message}", flush=True)
            return log

    logger = _TimestampLogger()

# In-progress marks (see "In-progress marks" above).
MARK_YEARS_BACK = 100
MARK_BEFORE = "2000-01-01"          # any cache timestamp earlier than this is a mark
# Marks older than this were left by a run that stopped without cleaning up. It
# must outlast the longest a run can hold marks: the cube-job timeout plus four
# SQL statements (confirm, set cache timestamp, confirm, unmark), plus slack for
# the MicroStrategy API calls.
MARK_STALE_MINUTES = (JOB_TIMEOUT_SECONDS + 4 * QUERY_TIMEOUT_SECONDS) // 60 + 15

# The only SQL execution statuses DatasourceInstance.execute_query() recognizes.
STATUS_DONE = 3
STATUS_ERROR = 4

_EMPTY_SENTINELS = {"", "-", "null", "blank", "empty"}


def _blank(value, default=None):
    if value is None:
        return default
    if isinstance(value, str) and value.strip().lower() in _EMPTY_SENTINELS:
        return default
    return value


# --- Substituted by Workstation (text -> 'quoted') --------------------------
PROJECT_NAME = _blank($project_name)                # noqa: F821
DS_ID = _blank($datasource_instance_id)             # noqa: F821
DS_NAME = _blank($datasource_instance_name)         # noqa: F821
LOAD_HISTORY_TABLE = _blank($load_history_table)    # noqa: F821
LOAD_ID_COLUMN = _blank($load_id_column)            # noqa: F821
CACHE_TIMESTAMP_COLUMN = _blank($cache_timestamp_column)  # noqa: F821
EVENT_ID = _blank($event_id)                        # noqa: F821
COMPLETION_EVENT_ID = _blank($completion_event_id, "D5B52B8E4F6376E252F4AA91F6BF66A9")  # noqa: F821
NOTIFY_USERS = $notification_users                  # noqa: F821 (list variable)
# Credential login (disabled). Add the dollar sign back before each variable name
# when uncommenting:
USER_NAME = _blank($user_name)
PASSWORD = _blank($password)                       # (secret var type)
BASE_URL = _blank($base_url)
LOGIN_MODE = 1
# ----------------------------------------------------------------------------


class WorkflowError(Exception):
    """A step failed. The message is what the administrator receives."""


_state = {"step": "starting"}


def _step(name):
    _state["step"] = name
    logger.info("--- {step} ---", step=name)


# --- Connection ---------------------------------------------------------------


def _session_url():
#     """The Library URL carried by the injected connectionData, or None."""
    try:
        return connectionData["defaultEnvironment"]["url"]  # noqa: F821
    except Exception:  # noqa: BLE001
        return None
#
#
def _credential_connect():
    """Log in with user_name/password against base_url (else the connectionData URL)."""
    base_url, source = BASE_URL, "the base_url variable"
    if not base_url:
        base_url, source = _session_url(), "connectionData"
    if not base_url:
        raise WorkflowError("Credential login needs base_url (none provided or derivable).")
    print(f"Connecting as '{USER_NAME}' (credential login) to {base_url} (from {source}).")
    return Connection(base_url=base_url, username=USER_NAME, password=PASSWORD,
                    project_name=PROJECT_NAME, login_mode=LOGIN_MODE)


def _connect_once():
    """Connect through the injected connectionData session (runs as the subscriber)."""
    if USER_NAME and PASSWORD:
        return _credential_connect()
    logger.info("Connecting via connectionData session.")
    return get_connection(connectionData, project_name=PROJECT_NAME)  # noqa: F821


def _connect():
    """Connect, retrying only network errors. A Planned Task can wake a cold
    environment whose Library DNS is not ready on the first attempt; retrying
    here keeps that from emailing a false alarm."""
    for attempt in range(1, CONNECT_MAX_ATTEMPTS + 1):
        try:
            return _connect_once()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            if attempt == CONNECT_MAX_ATTEMPTS:
                raise
            logger.warning("Connection attempt {attempt}/{attempts} failed ({error}); "
                           "retrying in {delay}s.", attempt=attempt,
                           attempts=CONNECT_MAX_ATTEMPTS, error=exc,
                           delay=CONNECT_RETRY_DELAY_SECONDS)
            time.sleep(CONNECT_RETRY_DELAY_SECONDS)


# --- Table and column names, and the statements built from them ---------------

# A SQL Server name part: letters, digits and underscores, or [bracketed].
_NAME_PART = r"(?:\[[^\]]+\]|\w+)"
_COLUMN_NAME = re.compile(_NAME_PART)
_TABLE_NAME = re.compile(rf"{_NAME_PART}(?:\.{_NAME_PART}){{0,3}}")


def _checked_name(label, value, pattern, example):
    """Return the trimmed name, or stop if it isn't a valid SQL Server name.
    Only names that pass this check are placed into SQL."""
    name = (value or "").strip()
    if not pattern.fullmatch(name):
        raise WorkflowError(f"{label} {value!r} is not a valid name. Use letters, digits "
                            f"and underscores (for example {example}), or wrap a name "
                            f"in [brackets].")
    return name


def build_statements(table, id_col, ts):
    """Build every statement from the checked table and column names.

    Statements that act on specific loads contain a {load_ids} placeholder;
    fill it with _with_ids().
    """
    marked = f"{ts} < '{MARK_BEFORE}'"
    stale = (f"{marked}\n   AND DATEADD(year, {MARK_YEARS_BACK}, {ts}) < "
             f"DATEADD(minute, -{MARK_STALE_MINUTES}, current_timestamp)")
    loads = f"{id_col} IN ({{load_ids}})"
    return {
        "select": (f"SELECT STRING_AGG(CONVERT(varchar(10), CASE WHEN {ts} IS NULL "
                   f"THEN {id_col} END), ',') WITHIN GROUP (ORDER BY {id_col}) AS LoadIDs\n"
                   f"      ,COUNT({id_col}) AS LoadResult\n"
                   f"      ,SUM(CASE WHEN {ts} IS NULL THEN 1 ELSE 0 END) AS CacheDropCount\n"
                   f"  FROM {table}\n"
                   f" WHERE {ts} IS NULL"),
        "stale_count": f"SELECT COUNT(*) AS StaleMarks FROM {table}\n WHERE {stale}",
        "release_stale": f"UPDATE {table}\n   SET {ts} = NULL\n WHERE {stale}",
        "active_count": f"SELECT COUNT(*) AS ActiveMarks FROM {table}\n WHERE {marked}",
        "mark": (f"UPDATE {table}\n"
                 f"   SET {ts} = DATEADD(year, -{MARK_YEARS_BACK}, current_timestamp)\n"
                 f" WHERE {loads}\n   AND {ts} IS NULL"),
        "marked_count": (f"SELECT COUNT(*) AS MarkedLoads FROM {table}\n"
                         f" WHERE {loads}\n   AND {marked}"),
        "finish": (f"UPDATE {table}\n   SET {ts} = current_timestamp\n"
                   f" WHERE {loads}\n   AND {marked}"),
        "unfinished_count": (f"SELECT COUNT(*) AS UnfinishedLoads FROM {table}\n"
                             f" WHERE {loads}\n   AND ({ts} IS NULL OR {marked})"),
        "unmark": f"UPDATE {table}\n   SET {ts} = NULL\n WHERE {loads}\n   AND {marked}",
    }


def _id_list(load_ids):
    return ",".join(str(i) for i in load_ids)


def _with_ids(statement, load_ids):
    return statement.replace("{load_ids}", _id_list(load_ids))


# --- SQL through the datasource instance -------------------------------------


def run_query(conn, datasource_id, project_id, query):
    """Submit SQL once, then poll until it finishes, fails or times out.

    These are the same two calls DatasourceInstance.execute_query() makes, but
    with a real deadline instead of its 50-second budget that returns None.
    """
    execution_id = ds_processors.execute_query(
        connection=conn,
        body={"query": Encoder(str(query)).encoded_text},
        id=datasource_id,
        project_id=project_id,
    ).get("id")
    if not execution_id:
        raise RuntimeError("Query submission returned no execution id.")

    start = time.monotonic()
    deadline = start + QUERY_TIMEOUT_SECONDS
    delay, last_beat = 1.0, start
    while True:
        execution = ds_processors.get_query_results(connection=conn, id=execution_id)
        status = execution.get("status")
        message = execution.get("message") or ""
        if status == STATUS_DONE:
            return execution
        if status == STATUS_ERROR:
            if "Odbc success with info" in message:
                return execution
            raise RuntimeError(f"Query failed: {message}")
        now = time.monotonic()
        if now >= deadline:
            raise TimeoutError(f"Query {execution_id} still running after "
                               f"{QUERY_TIMEOUT_SECONDS}s (last status {status}).")
        if now - last_beat >= 30:
            logger.info("  query still running: {elapsed}s elapsed", elapsed=int(now - start))
            last_beat = now
        time.sleep(min(delay, deadline - now))
        delay = min(delay * 2, 5)


def _first_value(execution, alias, source):
    """First-row value of `alias` in a SQL-execution result (case-insensitive)."""
    data = (execution.get("results") or {}).get("data") or {}
    columns = {str(name).lower(): values for name, values in data.items()}
    if alias.lower() not in columns:
        raise WorkflowError(f"{source} returned no '{alias}' column; "
                            f"columns were {sorted(data)}.")
    return (columns[alias.lower()] or [None])[0]


def _run_count(conn, datasource_id, project_id, statement, alias):
    """Run a COUNT(*) statement and return the count."""
    execution = run_query(conn, datasource_id, project_id, statement)
    return int(_first_value(execution, alias, "A mark check") or 0)


def _run_write(conn, datasource_id, project_id, statement):
    """Run an UPDATE and return the error it reported, if any.

    A statement that returns no rows may be reported as an error even when it
    worked, so callers confirm the effect with a follow-up SELECT.
    """
    logger.info("Running:\n{statement}", statement=statement)
    try:
        run_query(conn, datasource_id, project_id, statement)
    except RuntimeError as exc:
        logger.warning("  The UPDATE reported: {error}. Checking whether it applied.", error=exc)
        return str(exc)
    return None


def _detail(reported):
    return f" The UPDATE reported: {reported}" if reported else ""


def _parse_load_ids(raw):
    """'238,239' -> [238, 239]. Anything that is not a list of integers is
    rejected, so only well-formed ids can reach an UPDATE."""
    if raw is None or str(raw).strip() == "":
        return []
    ids = []
    for part in str(raw).split(","):
        part = part.strip()
        if not part.isdigit():
            raise WorkflowError(f"LoadIDs contains a non-integer value: {raw!r}")
        ids.append(int(part))
    return ids


def _load_history(conn, datasource_id, project_id, statements):
    """Read the pending loads; return (load_ids, load_result, cache_drop_count)."""
    execution = run_query(conn, datasource_id, project_id, statements["select"])
    source = "The load history query"
    cache_drop_count = int(_first_value(execution, "CacheDropCount", source) or 0)
    load_result = int(_first_value(execution, "LoadResult", source) or 0)
    load_ids = _parse_load_ids(_first_value(execution, "LoadIDs", source))
    return load_ids, load_result, cache_drop_count


# --- In-progress marks ------------------------------------------------------------


def release_stale_marks(conn, datasource_id, project_id, statements):
    """Release marks older than MARK_STALE_MINUTES and tell the administrator.

    A mark that old was left by a run that stopped without cleaning up (for
    example, when its runtime was killed), which would otherwise go unnoticed.
    """
    stale = _run_count(conn, datasource_id, project_id, statements["stale_count"], "StaleMarks")
    if not stale:
        return
    reported = _run_write(conn, datasource_id, project_id, statements["release_stale"])
    still_stale = _run_count(conn, datasource_id, project_id,
                             statements["stale_count"], "StaleMarks")
    if still_stale:
        raise WorkflowError(f"{still_stale} mark(s) older than {MARK_STALE_MINUTES} minutes "
                            f"could not be released.{_detail(reported)}")
    logger.success("Released {count} mark(s) older than {minutes} minutes.",
                   count=stale, minutes=MARK_STALE_MINUTES)
    email_admin(
        subject="[MicroStrategy] ClearCache released marks from an earlier run",
        body=(f"Project: {PROJECT_NAME}\n"
              f"Refresh event: {EVENT_ID}\n\n"
              f"{stale} load(s) were still marked as in progress after "
              f"{MARK_STALE_MINUTES} minutes, so an earlier run most likely stopped "
              f"without cleaning up. The marks were released and those loads will "
              f"be processed again."),
    )


def mark_loads(conn, datasource_id, project_id, statements, load_ids):
    """Mark load_ids as in progress, so runs that start meanwhile skip."""
    reported = _run_write(conn, datasource_id, project_id,
                          _with_ids(statements["mark"], load_ids))
    marked = _run_count(conn, datasource_id, project_id,
                        _with_ids(statements["marked_count"], load_ids), "MarkedLoads")
    if marked != len(load_ids):
        note = unmark_loads(conn, datasource_id, project_id, statements, load_ids)
        raise WorkflowError(f"Only {marked} of {len(load_ids)} load(s) could be marked as "
                            f"in progress.{_detail(reported)}\n{note}")
    logger.success("Marked load id(s) {ids} as in progress.", ids=_id_list(load_ids))


def finish_loads(conn, datasource_id, project_id, statements, load_ids):
    """Replace the marks on load_ids with the real cache timestamp and confirm it."""
    reported = _run_write(conn, datasource_id, project_id,
                          _with_ids(statements["finish"], load_ids))
    unfinished = _run_count(conn, datasource_id, project_id,
                            _with_ids(statements["unfinished_count"], load_ids),
                            "UnfinishedLoads")
    if unfinished:
        raise WorkflowError(f"{unfinished} of {len(load_ids)} load(s) did not get a cache "
                            f"timestamp.{_detail(reported)}")
    logger.success("Set the cache timestamp on load id(s) {ids}.", ids=_id_list(load_ids))


def unmark_loads(conn, datasource_id, project_id, statements, load_ids):
    """Remove the marks from load_ids so the next run retries them.

    Never raises, because it runs while another failure is being handled.
    Returns a sentence for the failure email.
    """
    ids = _id_list(load_ids)
    try:
        _run_write(conn, datasource_id, project_id, _with_ids(statements["unmark"], load_ids))
        still_marked = _run_count(conn, datasource_id, project_id,
                                  _with_ids(statements["marked_count"], load_ids),
                                  "MarkedLoads")
    except Exception as exc:  # noqa: BLE001
        return (f"Could not remove the marks from load id(s) {ids} ({exc}); they will be "
                f"released automatically after {MARK_STALE_MINUTES} minutes.")
    if still_marked:
        return (f"{still_marked} load(s) are still marked; they will be released "
                f"automatically after {MARK_STALE_MINUTES} minutes.")
    return f"Removed the marks from load id(s) {ids}, so the next run will retry them."


# --- Cube refresh and verification -------------------------------------------


def _job_num(job_id):
    """Job.id is '<jobNumber>:<node>'; the cube cache stores only <jobNumber>."""
    return str(job_id).split(":", 1)[0].strip()


def _cube_contents(subscription):
    """Cube contents of a subscription. Cube-update subscriptions carry one."""
    cubes = [(c.id, getattr(c, "name", "") or "")
             for c in (subscription.contents or [])
             if str(getattr(c.type, "value", c.type)).lower() == "cube"]
    if len(subscription.contents or []) > 1:
        logger.warning("  Subscription '{name}' has {count} contents (expected 1).",
                       name=subscription.name, count=len(subscription.contents))
    return cubes


def _cubes_from_event(conn, event, project_id):
    """{cube_id: cube_name} across the event's related subscriptions (one project)."""
    related_ids = {r.get("id") for r in event.list_related_subscriptions(to_dictionary=True)
                   if r.get("id")}
    cube_map, sub_names = {}, []
    for s in list_subscriptions(conn, project_id=project_id):
        if s.id in related_ids:
            sub_names.append(s.name)
            for cid, cname in _cube_contents(s):
                cube_map[cid] = cname
    return cube_map, sub_names


def _cube_sub_jobs(conn, project):
    """Cube-subscription jobs currently in the monitor (errors NOT excluded).
    `project` must be a Project object: list_jobs treats a string as a NAME."""
    return list_jobs(conn, project=project,
                     object_type=ObjectType.CUBE, type=JobType.SUBSCRIPTION)


def _cube_cache_job_nums(conn, cube_id):
    """The `last_update_job` ids across this cube's caches (all nodes)."""
    nums = set()
    try:
        caches = list_cube_caches(conn, cube_id=cube_id, to_dictionary=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning("  Could not read cube caches for {cube_id}: {error}",
                       cube_id=cube_id, error=exc)
        return nums
    for c in caches:
        job = c.get("lastUpdateJob", c.get("last_update_job"))
        if job is not None:
            nums.add(_job_num(job))
    return nums


def _cache_nodes(conn, project_id):
    """Cluster node names for the project (to_dictionary dodges enum parsing)."""
    nodes = Cluster(conn).list_nodes(project_id=project_id, to_dictionary=True)
    return [n.get("name") for n in nodes or [] if n.get("name")]


def _invalidate_report_caches(conn, project_id):
    """Invalidate every report/content cache in the project, one PATCH per node.

    Deliberately NOT ContentCache.invalidate_caches(): that builds a
    ContentCache(connection, cache_id) for every id, and the constructor parses
    source.type into CacheSource.Type. A single cache of a type the enum doesn't
    know ('docreport', seen in production but not in dev) raises
    "'docreport' is not a valid CacheSource.Type" and nothing is invalidated.
    It also costs one GET per cache. Listing with to_dictionary=True and sending
    the PATCH ourselves avoids the enum and is one request per node.
    """
    total = 0
    for node in _cache_nodes(conn, project_id):
        caches = ContentCache.list_caches(conn, project_id=project_id, nodes=node,
                                          to_dictionary=True)
        operations = []
        for cache in caches or []:
            # the PATCH addresses caches by combinedId (cache id + project + node)
            combined = (cache.get("combinedId") or cache.get("combined_id")
                        or cache.get("id"))
            if combined:
                operations.append({
                    "op": "replace",
                    "path": f"/contentCaches/{combined}/status/invalid",
                    "value": True,
                })
        if not operations:
            logger.info("  node {node}: no caches to invalidate", node=node)
            continue
        monitors_api.update_contents_caches(conn, node, {"operationList": operations})
        total += len(operations)
        logger.info("  node {node}: invalidated {n} cache(s)", node=node, n=len(operations))
    logger.info("Invalidated {count} report/content cache(s).", count=total)


def refresh_and_verify_cubes(conn, project_id):
    """Trigger the refresh event and verify each cube job against its cube cache.
    Returns the number of verified jobs; raises WorkflowError otherwise."""
    project = Project(conn, id=project_id)
    event = Event(conn, id=EVENT_ID)
    cube_map, sub_names = _cubes_from_event(conn, event, project_id)
    name_by_id = dict(cube_map)
    logger.info("Event {name} ({event_id}): {subs} related subscription(s), "
                "{cubes} distinct cube(s).", name=getattr(event, "name", EVENT_ID),
                event_id=EVENT_ID, subs=len(sub_names), cubes=len(cube_map))
    if not cube_map:
        raise WorkflowError("The refresh event has no cube subscriptions in this project.")

    # Baseline identity uses ALL pre-existing jobs, so a stale errored job is
    # never mistaken for one this run started.
    baseline = _cube_sub_jobs(conn, project)
    before_job_nums = {_job_num(j.id) for j in baseline}
    not_error = sum(1 for j in baseline if j.status != JobStatus.ERROR)
    logger.info("Baseline: {count} cube-subscription job(s) in the monitor "
                "({not_error} not in Error).", count=len(baseline), not_error=not_error)
    for cid, cname in cube_map.items():
        logger.info("  cube {name} ({cube_id}) cache last_update_job={jobs}",
                    name=cname, cube_id=cid, jobs=sorted(_cube_cache_job_nums(conn, cid)))

    _invalidate_report_caches(conn, project_id)
    event.trigger()
    logger.info("Triggered refresh event: {event_id}", event_id=EVENT_ID)

    our_jobs = {}                                   # job number -> details
    finished, verified, errored = set(), set(), set()
    start_deadline = time.time() + START_GRACE_SECONDS
    deadline = time.time() + JOB_TIMEOUT_SECONDS
    while time.time() < deadline:
        try:
            jobs = _cube_sub_jobs(conn, project)
        except Exception as exc:  # noqa: BLE001
            logger.warning("  [poll] could not list jobs: {error}", error=exc)
            jobs = []
        current = {_job_num(j.id): j for j in jobs}

        for num, j in current.items():              # not in the baseline = ours
            if num in before_job_nums:
                continue
            if num not in our_jobs:
                our_jobs[num] = {"cube_id": j.object_id,
                                 "cube_name": getattr(j, "object_name", "")
                                 or name_by_id.get(j.object_id, "")}
                logger.info("  [job] started {job} -> {cube} ({cube_id})", job=num,
                            cube=our_jobs[num]["cube_name"], cube_id=j.object_id)
            if j.status == JobStatus.ERROR:
                errored.add(num)

        for num in our_jobs:                        # done = left monitor or errored
            if num not in current or num in errored:
                finished.add(num)

        for num in sorted(finished - verified - errored):
            if num in _cube_cache_job_nums(conn, our_jobs[num]["cube_id"]):
                verified.add(num)
                logger.success("  [verified] job {job} rebuilt {cube}", job=num,
                               cube=our_jobs[num]["cube_name"])

        if our_jobs and finished == set(our_jobs) and (verified | errored) >= finished:
            break
        if not our_jobs and time.time() >= start_deadline:
            break

        left = int(deadline - time.time())
        running = set(our_jobs) - finished
        if running:
            logger.info("  [poll] {left}s left - {count} job(s) running: {cubes}", left=left,
                        count=len(running),
                        cubes=", ".join(sorted(our_jobs[n]["cube_name"] or n for n in running)))
        elif our_jobs:
            logger.info("  [poll] {left}s left - awaiting cache verification for {jobs}",
                        left=left, jobs=sorted(finished - verified - errored))
        else:
            logger.info("  [poll] {left}s left - waiting for cube jobs to start", left=left)
        time.sleep(POLL_SECONDS)

    lines = []
    for num, info in sorted(our_jobs.items()):
        if num in errored:
            state = "ERROR"
        elif num in verified:
            state = "VERIFIED"
        elif num in finished:
            state = "FINISHED, cache job id did not match"
        else:
            state = "STILL RUNNING at timeout"
        lines.append(f"[{state}] job {num} - {info['cube_name']} ({info['cube_id']})")
    seen_cubes = {i["cube_id"] for i in our_jobs.values()}
    lines += [f"[no job observed] {name_by_id.get(cid, '')} ({cid})"
              for cid in cube_map if cid not in seen_cubes]
    logger.info("=== Cube refresh verification ===")
    for line in lines:
        logger.info("  {line}", line=line)

    if not our_jobs:
        raise WorkflowError("No cube-subscription jobs started after the refresh event.\n"
                            + "\n".join(lines))
    if errored or verified != set(our_jobs):
        raise WorkflowError("Not every cube job finished and verified:\n" + "\n".join(lines))
    return len(verified)


# --- Email -----------------------------------------------------------------------


_GUID = re.compile(r"[0-9A-Fa-f]{32}")


def notify_entries(value):
    """Entries from the notification_users variable: a Workstation list variable
    arrives as a list; a text value is split on commas, semicolons and new lines."""
    if isinstance(value, (list, tuple)):
        parts = [str(item) for item in value if item is not None]
    else:
        parts = re.split(r"[,;\r\n]+", str(value or ""))
    return [part.strip() for part in parts
            if part.strip() and part.strip().lower() not in _EMPTY_SENTINELS]


def resolve_recipients(conn, entries):
    """User ids for send_email(). A 32-character GUID is used as it is; anything
    else is looked up as a login. Unknown entries are logged and skipped, so one
    bad name doesn't silence the rest."""
    recipients = []
    for entry in entries:
        if _GUID.fullmatch(entry):
            recipients.append(entry)
            continue
        try:
            recipients.append(User(conn, username=entry).id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Notification user {entry} not found, skipped: {error}",
                           entry=entry, error=exc)
    return recipients


def email_admin(subject, body):
    """Notify notification_users through Library's own email (mstrio send_email).

    Never raises, so it cannot hide another error. Needs the connection, so a
    failure before connecting can only be logged.
    """
    conn, recipients = _state.get("conn"), _state.get("recipients") or []
    if conn is None or not recipients:
        reason = "no connection yet" if conn is None else "no notification_users resolved"
        logger.warning("Notification NOT sent ({subject}): {reason}.",
                       subject=subject, reason=reason)
        return
    content = (f"<p><b>{escape(subject)}</b></p>"
               f"<pre style=\"font-family:Consolas,monospace;font-size:12px\">"
               f"{escape(body)}</pre>")
    try:
        send_email(conn, users=recipients, subject=subject, content=content, is_html=True)
        logger.success("Notification sent to {count} user(s): {subject}",
                       count=len(recipients), subject=subject)
    except Exception as exc:  # noqa: BLE001
        logger.error("Notification could not be sent: {error}", error=exc)


# --- Main ------------------------------------------------------------------------


def main():
    _step("validating inputs")
    missing = [name for name, value in (
        ("project_name", PROJECT_NAME),
        ("load_history_table", LOAD_HISTORY_TABLE),
        ("load_id_column", LOAD_ID_COLUMN),
        ("cache_timestamp_column", CACHE_TIMESTAMP_COLUMN),
        ("event_id", EVENT_ID)) if not value]
    if missing:
        raise WorkflowError(f"Missing required input(s): {', '.join(missing)}")
    if not (DS_ID or DS_NAME):
        raise WorkflowError("Provide datasource_instance_id or datasource_instance_name.")
    table = _checked_name("load_history_table", LOAD_HISTORY_TABLE, _TABLE_NAME,
                          "BPIPlus.MicroStrategy.MIDWHLoadHistory_Test")
    id_col = _checked_name("load_id_column", LOAD_ID_COLUMN, _COLUMN_NAME,
                           "MIDWHLoadHistoryId")
    ts = _checked_name("cache_timestamp_column", CACHE_TIMESTAMP_COLUMN, _COLUMN_NAME,
                       "DropMSCacheDataTime")
    statements = build_statements(table, id_col, ts)
    logger.info("Table {table}, load id column {id_col}, cache timestamp column {ts}.",
                table=table, id_col=id_col, ts=ts)
    if not notify_entries(NOTIFY_USERS):
        logger.warning("notification_users is empty, so nothing will be emailed.")

    _step("connecting")
    conn = _connect()
    _state["conn"] = conn
    _state["recipients"] = resolve_recipients(conn, notify_entries(NOTIFY_USERS))
    logger.info("Notifications go to {count} user(s).", count=len(_state["recipients"]))
    project_id = conn.project_id
    datasource_id = DatasourceInstance(conn, id=DS_ID, name=DS_NAME).id

    _step("checking for a run in progress")
    release_stale_marks(conn, datasource_id, project_id, statements)
    active = _run_count(conn, datasource_id, project_id, statements["active_count"], "ActiveMarks")
    if active:
        logger.info("{count} load(s) are marked by a run that is still working. "
                    "Skipping this run.", count=active)
        return 0

    _step("checking load history")
    load_ids, load_result, cache_drop_count = _load_history(
        conn, datasource_id, project_id, statements)
    logger.info("LoadIDs={ids} LoadResult={result} CacheDropCount={count}",
                ids=load_ids, result=load_result, count=cache_drop_count)
    if cache_drop_count < 1:
        logger.info("No pending loads (CacheDropCount is 0). Nothing to do.")
        return 0
    if len(load_ids) != cache_drop_count:
        raise WorkflowError(f"LoadIDs has {len(load_ids)} id(s) but CacheDropCount is "
                            f"{cache_drop_count}; stopping before any change is made.")

    _step("marking loads as in progress")
    mark_loads(conn, datasource_id, project_id, statements, load_ids)

    try:
        _step("refreshing and verifying cubes")
        verified = refresh_and_verify_cubes(conn, project_id)
        _step("setting the cache timestamp")
        finish_loads(conn, datasource_id, project_id, statements, load_ids)
    except Exception as exc:
        note = unmark_loads(conn, datasource_id, project_id, statements, load_ids)
        logger.warning("{note}", note=note)
        raise WorkflowError(f"{exc}\n{note}") from exc

    _step("triggering completion event")
    if COMPLETION_EVENT_ID:
        Event(conn, id=COMPLETION_EVENT_ID).trigger()
        logger.info("Triggered completion event: {event_id}", event_id=COMPLETION_EVENT_ID)

    logger.success("=== Done: {verified} cube job(s) verified; cache timestamp set on load "
                   "id(s) {ids}. ===", verified=verified, ids=_id_list(load_ids))
    return 0


# The host exec()s this script as a string, so the __main__ guard is unreliable.
try:
    _rc = main()
except Exception as exc:  # noqa: BLE001
    failed_step = _state["step"]
    logger.error("FAILED while {step}: {error}", step=failed_step, error=exc)
    email_admin(
        subject=f"[MicroStrategy] ClearCache workflow failed: {failed_step}",
        body=(f"Project: {PROJECT_NAME}\n"
              f"Refresh event: {EVENT_ID}\n"
              f"Failed while: {failed_step}\n"
              f"Error: {exc}\n\n"
              f"{traceback.format_exc()}"),
    )
    _rc = 1
if _rc:
    sys.exit(_rc)
