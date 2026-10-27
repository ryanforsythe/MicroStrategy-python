# Run SQL on a datasource instance and wait for the result (real timeout, no silent None).
"""ExecuteDatasourceQuery_Workstation — run SQL through a datasource instance.

MicroStrategy Workstation script. Uses the injected `connectionData` session.

Why not DatasourceInstance.execute_query() directly
---------------------------------------------------
execute_query() submits the SQL once, then polls the execution `max_retries`
times, sleeping `retry_delay` seconds between polls (the "retries" never re-run
the SQL). Its default budget is 10 x 5 = 50 seconds. When the query outlasts
that budget it logs an error and returns None instead of raising, so a slow
query looks like an empty result.

This script makes the same two mstrio-py calls (submit, then poll the execution
id) but waits against a wall-clock deadline, backs off between polls, and raises
TimeoutError when the deadline passes.

Note: the submit/poll helpers come from mstrio.utils.response_processors, which
is internal to mstrio-py rather than its documented public API. Re-check this
script after upgrading mstrio-py.

Timing out here does not cancel the query; it keeps running on the server.

Inputs
------
    project_name              text  Project to connect to (required)
    datasource_instance_id    text  Datasource instance GUID (id or name required)
    datasource_instance_name  text  Datasource instance name (id or name required)
    sql_query                 text  SQL to execute (required)

Workstation "unset" tokens (case-insensitive): '', '-', 'null', 'blank', 'empty'.
"""

import json
import time

from mstrio.connection import get_connection
from mstrio.datasources.datasource_instance import DatasourceInstance
from mstrio.utils.encoder import Encoder
from mstrio.utils.response_processors import datasources as ds_processors

TIMEOUT_SECONDS = 1800   # total wall-clock wait for the query
MAX_POLL_SECONDS = 5     # polling backs off from 1s up to this interval
HEARTBEAT_SECONDS = 30   # print a progress line at most this often

# The only execution statuses execute_query() recognizes.
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
PROJECT_NAME = _blank($project_name)                 # noqa: F821
DS_ID = _blank($datasource_instance_id)              # noqa: F821
DS_NAME = _blank($datasource_instance_name)          # noqa: F821
SQL_QUERY = _blank($sql_query)                       # noqa: F821
# ----------------------------------------------------------------------------


def run_query(conn, datasource_id, project_id, query,
              timeout_seconds=TIMEOUT_SECONDS, max_poll_seconds=MAX_POLL_SECONDS):
    """Submit the query once, then poll until it finishes, fails, or times out."""
    execution_id = ds_processors.execute_query(
        connection=conn,
        body={"query": Encoder(str(query)).encoded_text},
        id=datasource_id,
        project_id=project_id,
    ).get("id")
    if not execution_id:
        raise RuntimeError("Query submission returned no execution id.")
    print(f"Submitted query; execution id {execution_id}")

    start = time.monotonic()
    deadline = start + timeout_seconds
    delay = 1.0
    last_beat = start
    while True:
        execution = ds_processors.get_query_results(connection=conn, id=execution_id)
        status = execution.get("status")
        message = execution.get("message") or ""

        if status == STATUS_DONE:
            print(f"Query finished in {time.monotonic() - start:.1f}s.")
            return execution
        if status == STATUS_ERROR:
            if "Odbc success with info" in message:
                return execution
            raise RuntimeError(f"Query execution failed: {message}")

        now = time.monotonic()
        if now >= deadline:
            raise TimeoutError(
                f"Query {execution_id} still running after {timeout_seconds}s "
                f"(last status {status}).")
        if now - last_beat >= HEARTBEAT_SECONDS:
            print(f"  still running: {int(now - start)}s elapsed, status {status}")
            last_beat = now
        time.sleep(min(delay, deadline - now))
        delay = min(delay * 2, max_poll_seconds)


def main():
    if not PROJECT_NAME:
        raise ValueError("project_name is required.")
    if not SQL_QUERY:
        raise ValueError("sql_query is required.")
    if not (DS_ID or DS_NAME):
        raise ValueError("Provide datasource_instance_id or datasource_instance_name.")

    conn = get_connection(connectionData, project=PROJECT_NAME)  # noqa: F821
    dsi = DatasourceInstance(conn, id=DS_ID, name=DS_NAME)
    return run_query(conn, dsi.id, conn.project_id, SQL_QUERY)


# The host exec()s this script as a string, so the __main__ guard is unreliable.
result = main()
print(json.dumps(result, default=str))


def get_results():
    return result
