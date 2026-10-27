"""
SystemManager_ClearCache.py — Python conversion of the legacy MicroStrategy
System Manager workflow `MarketIntelligence_ClearCache`.

Replaces a System Manager process with an mstrio-py / REST-API script suitable
for the MicroStrategy Cloud Environment (MCE) Python runtime. The same script
works for any project/database — only the YAML config (report GUIDs, project,
event names) changes per workflow.

Source workflow (SystemManager_ClearCache.MD)
─────────────────────────────────────────────
    1. SQL: CheckIfMIDWHRefreshHappened.sql
       └─ if result = 1 continue, else exit
    2. SQL: CheckIfMIDWHRefreshHappened_RetrieveIDs.sql  (output: MIDWHLOADHISTORYIDS)
    3. I-Server: INVALIDATE ALL REPORT CACHES IN PROJECT "Crop Canada"
    4. SQL: EXEC IndicateCacheDropHappened '${MIDWHLOADHISTORYIDS}'
    5. I-Server: TRIGGER EVENT "Refresh-Crop-Canada-Cubes Trigger"
    6. I-Server (on success): TRIGGER EVENT "NotifyClearCache"
    7. On failure: high-importance email to Administrators

Conversion design
─────────────────
SQL execution
    The MCE Python runtime does NOT include `pyodbc` / `pymssql`, and database
    credentials should not live in Python. SQL is therefore executed through
    pre-created Freeform SQL reports — one per SQL statement — that target the
    project's existing datasource. Only the report GUIDs change per project,
    so the same Python template is reusable.

    Report 1: check_refresh
        SELECT TOP 1 case when MIDWHLoadHistoryId is not null then 1 else 0 end AS Result
        FROM   [BPIPlus].[MicroStrategy].[MIDWHLoadHistory]
        WHERE  DropMSCacheDataTime is null
        GROUP  BY [MIDWHLoadHistoryId], DropMSCacheDataTime
        ORDER  BY [MIDWHLoadHistoryId] DESC;

    Report 2: retrieve_ids
        SELECT substring(
                 (SELECT ',' + convert(varchar(10), MIDWHLoadHistoryId) AS 'data()'
                  FROM   BPIPlus.MicroStrategy.MIDWHLoadHistory
                  WHERE  DropMSCacheDataTime IS NULL
                  FOR XML PATH('')) ,2 ,9999) AS MILoadIDs;

    Report 3: indicate_cache_drop  (one prompt: MIDWHLoadHistoryIds, type=Value)
        EXEC BPIPlus.MicroStrategy.IndicateCacheDropHappened ?[MIDWHLoadHistoryIds];
        SELECT 1 AS Status;     -- result set is required by FFSQL

Cache invalidation
    Uses the mstrio-py `ContentCache` SDK (added in 11.4):

        from mstrio.project_objects.content_cache import ContentCache
        caches = ContentCache.list_caches(conn, project_id=pid, status="ready")
        ContentCache.invalidate_caches(conn, cache_ids=[c.id for c in caches])

    `list_caches(status="ready")` returns only caches that are currently loaded —
    matching the System Manager semantic of `INVALIDATE ALL REPORT CACHES IN
    PROJECT` (already-invalid caches are skipped). The bulk
    `ContentCache.invalidate_caches()` call replaces what would otherwise be
    one PATCH per cache.

Event triggering
    Uses `mstrio.distribution_services.Event(connection, name=).trigger()` for
    both `Refresh-Crop-Canada-Cubes Trigger` and `NotifyClearCache`.

Failure email
    Tries `smtplib` against the configured SMTP server. MCE outbound SMTP is
    sometimes blocked — on send failure the error is logged and the script
    exits non-zero, letting the calling MCE Workflow handle escalation.

Configuration
    All workflow-specific values live in a YAML file. See the bundled
    system_manager_clear_cache_config.yaml for a template. Multiple projects
    keep separate config files; pass `--config PATH` to switch.

Authentication
    `MstrConfig` reads connection settings from environment variables (.env in
    standalone runs). Inside the MCE Workstation Python runtime the script
    can also be adapted to use `get_connection(workstationData)` — see the
    `_build_session()` helper.

Dry run / apply
    Defaults to dry run (logs what *would* happen). Pass `--apply` to execute
    cache invalidation, the IndicateCacheDropHappened call, and event triggers.
    The two SELECT-only reports (check_refresh, retrieve_ids) always run so the
    dry run reflects real state.

Usage
─────
    python SystemManager_ClearCache.py                          # dry run
    python SystemManager_ClearCache.py --apply                  # execute
    python SystemManager_ClearCache.py --config crop_eu.yaml --apply

REST API references
    https://microstrategy.github.io/rest-api-docs/common-workflows/monitoring/cache-monitors/
    https://microstrategy.github.io/rest-api-docs/common-workflows/analytics/manage-prompts/
    https://www2.microstrategy.com/producthelp/Current/workstation/en-us/Content/python_runtimes.htm
"""

from __future__ import annotations

import argparse
import smtplib
import sys
import time
from dataclasses import dataclass
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Optional

import yaml
from loguru import logger

from mstrio_core import MstrConfig, MstrRestSession
from mstrio_core.config import MstrEnvironment


# ── Constants ─────────────────────────────────────────────────────────────────

DEFAULT_CONFIG = "system_manager_clear_cache_config.yaml"
ENVS = [e.value for e in MstrEnvironment]

CACHE_LIST_STATUS = "ready"           # ContentCache.list_caches() filter
EVENT_TRIGGER_TIMEOUT = 60            # seconds


# ── Config dataclass ──────────────────────────────────────────────────────────


@dataclass
class WorkflowConfig:
    """All workflow parameters loaded from the YAML config file."""

    # MicroStrategy environment (dev/qa/prod) — selects MSTR_{ENV}_* vars
    env: str

    # Project: provide either name or id
    project_name: Optional[str]
    project_id: Optional[str]

    # Pre-created Freeform SQL report GUIDs (per project)
    check_refresh_report_id: str
    retrieve_ids_report_id: str
    indicate_cache_drop_report_id: str

    # Prompt key / title for the IDs parameter on the IndicateCacheDrop report
    indicate_cache_drop_prompt_key: str

    # I-Server event names to trigger
    refresh_cubes_event: str
    notify_event: str

    # Failure notification (optional — leave email blank to disable)
    notification_email: Optional[str] = None
    smtp_server: Optional[str] = None
    smtp_port: int = 25
    smtp_from: Optional[str] = None
    smtp_use_tls: bool = False
    smtp_username: Optional[str] = None
    smtp_password: Optional[str] = None


def load_config(path: Path) -> WorkflowConfig:
    """Load and validate the YAML configuration file."""
    if not path.exists():
        logger.error("Config file not found: {path}", path=path)
        sys.exit(2)

    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        logger.error("Config root must be a mapping: {path}", path=path)
        sys.exit(2)

    env = (raw.get("env") or "dev").strip().lower()
    if env not in ENVS:
        logger.error("env must be one of {envs}, got {val!r}", envs=ENVS, val=env)
        sys.exit(2)

    project = raw.get("project") or {}
    project_name = project.get("name")
    project_id = project.get("id")
    if not project_name and not project_id:
        logger.error("config.project must specify name or id.")
        sys.exit(2)

    reports = raw.get("freeform_sql_reports") or {}
    required_reports = ["check_refresh", "retrieve_ids", "indicate_cache_drop"]
    for key in required_reports:
        if not reports.get(key):
            logger.error("config.freeform_sql_reports.{k} is required.", k=key)
            sys.exit(2)

    events = raw.get("events") or {}
    if not events.get("refresh_cubes") or not events.get("notify"):
        logger.error("config.events.refresh_cubes and config.events.notify are required.")
        sys.exit(2)

    notify = raw.get("notification") or {}

    return WorkflowConfig(
        env=env,
        project_name=project_name,
        project_id=project_id,
        check_refresh_report_id=reports["check_refresh"],
        retrieve_ids_report_id=reports["retrieve_ids"],
        indicate_cache_drop_report_id=reports["indicate_cache_drop"],
        indicate_cache_drop_prompt_key=reports.get(
            "indicate_cache_drop_prompt_key", "MIDWHLoadHistoryIds"
        ),
        refresh_cubes_event=events["refresh_cubes"],
        notify_event=events["notify"],
        notification_email=notify.get("on_failure_email"),
        smtp_server=notify.get("smtp_server"),
        smtp_port=int(notify.get("smtp_port", 25)),
        smtp_from=notify.get("from_address"),
        smtp_use_tls=bool(notify.get("use_tls", False)),
        smtp_username=notify.get("smtp_username"),
        smtp_password=notify.get("smtp_password"),
    )


# ── Freeform SQL report execution (REST API — version-agnostic) ───────────────


def _create_report_instance(
    session: MstrRestSession,
    report_id: str,
    prompt_answers: Optional[list[dict]] = None,
) -> str:
    """
    Create a report instance and answer any prompts. Returns the instance ID.

    Two-step pattern is used so we can answer prompts on instances that have
    them, while still supporting prompt-less reports.
    """
    body: dict[str, Any] = {}
    r = session.post(
        f"/v2/reports/{report_id}/instances",
        scope="project",
        json=body,
    )
    r.raise_for_status()
    instance_id: str = r.json()["instanceId"]

    # Answer prompts if requested
    if prompt_answers:
        ar = session.put(
            f"/reports/{report_id}/instances/{instance_id}/prompts/answers",
            scope="project",
            json={"prompts": prompt_answers},
        )
        ar.raise_for_status()
        # Re-execute the instance now that prompts are answered
        re = session.post(
            f"/v2/reports/{report_id}/instances/{instance_id}",
            scope="project",
            json={},
        )
        re.raise_for_status()

    return instance_id


def _read_first_cell(payload: dict) -> Any:
    """
    Return the value of the first metric/attribute element of the first row
    from a v2 report instance payload. Tolerant of either Freeform SQL flavor
    (single metric, no attributes) or grid-style (attribute + metric).
    """
    definition = payload.get("definition") or {}
    grid = definition.get("grid") or {}
    data = payload.get("data") or {}

    # Metric values come back in data.metricValues.raw — a 2D array
    metrics_raw = (data.get("metricValues") or {}).get("raw") or []
    if metrics_raw and metrics_raw[0]:
        return metrics_raw[0][0]

    # Fall back to attribute first-row form for label-only reports
    rows = (data.get("paging") or {}).get("total")
    headers = (data.get("headers") or {}).get("rows") or []
    if headers and headers[0]:
        # row of attribute element references — resolve to forms[0].values[0]
        elems = grid.get("rows") or []
        if elems:
            forms = elems[0].get("elements") or []
            if forms and forms[0].get("formValues"):
                return forms[0]["formValues"][0]

    return None


def _execute_freeform_report(
    session: MstrRestSession,
    report_id: str,
    prompt_answers: Optional[list[dict]] = None,
) -> dict:
    """
    Execute a Freeform SQL report and return the v2 instance payload.

    For the IndicateCacheDropHappened SP wrapper (which writes data and returns
    a dummy `SELECT 1 AS Status`), the returned dict is mostly used for
    success confirmation. For the SELECT-only reports the caller passes the
    payload to _read_first_cell to get the answer.
    """
    instance_id = _create_report_instance(session, report_id, prompt_answers)

    # Pull the result body
    r = session.get(
        f"/v2/reports/{report_id}/instances/{instance_id}",
        scope="project",
        params={"limit": 100},
    )
    r.raise_for_status()
    return r.json()


# ── Workflow steps ────────────────────────────────────────────────────────────


def step_check_dwh_refresh_happened(
    session: MstrRestSession, report_id: str
) -> bool:
    """
    Returns True if a DWH refresh has occurred since the last cache drop
    (i.e. there is at least one MIDWHLoadHistory row with DropMSCacheDataTime
    IS NULL — the SQL emits 1 in that case).
    """
    logger.info("Step 1: CheckIfMIDWHRefreshHappened ({rid})", rid=report_id)
    payload = _execute_freeform_report(session, report_id)
    val = _read_first_cell(payload)
    refreshed = str(val).strip() == "1"
    logger.info(
        "CheckIfMIDWHRefreshHappened result: {val} → refreshed={ref}",
        val=val, ref=refreshed,
    )
    return refreshed


def step_retrieve_load_history_ids(
    session: MstrRestSession, report_id: str
) -> str:
    """
    Returns the comma-separated MIDWHLoadHistoryIds (e.g. "101,102,103").
    Empty string if none.
    """
    logger.info("Step 2: RetrieveIDs ({rid})", rid=report_id)
    payload = _execute_freeform_report(session, report_id)
    val = _read_first_cell(payload)
    ids = "" if val is None else str(val).strip()
    logger.info("MIDWHLoadHistoryIds = {ids!r}", ids=ids)
    return ids


def step_invalidate_caches(session: MstrRestSession, project_id: str) -> int:
    """
    Invalidate all loaded ("ready") content caches for the project, equivalent
    to the legacy System Manager command:

        INVALIDATE ALL REPORT CACHES IN PROJECT "<name>";

    Uses the mstrio-py SDK (mstrio 11.4+):

        ContentCache.list_caches(conn, project_id=, status="ready")
        ContentCache.invalidate_caches(conn, cache_ids=[...])

    `status="ready"` skips caches that are already invalid/expired so the bulk
    invalidate call only touches caches that actually need it. Returns the
    number of caches invalidated (zero when no ready caches exist).
    """
    logger.info("Step 3: invalidate report caches in project {pid}", pid=project_id)

    try:
        from mstrio.project_objects.content_cache import ContentCache
    except ImportError as exc:
        raise ImportError(
            "mstrio.project_objects.content_cache.ContentCache is required for "
            "cache invalidation. Update mstrio-py to 11.4 or later."
        ) from exc

    conn = session.mstrio_conn

    caches = ContentCache.list_caches(
        connection=conn,
        project_id=project_id,
        status=CACHE_LIST_STATUS,
    )
    if not caches:
        logger.info("No ready caches in project {pid} — nothing to invalidate.", pid=project_id)
        return 0

    cache_ids = [c.id for c in caches]
    logger.info("Bulk-invalidating {n} cache(s) via ContentCache.invalidate_caches.", n=len(cache_ids))

    ContentCache.invalidate_caches(connection=conn, cache_ids=cache_ids)

    logger.success(
        "Invalidated {n} cache(s) in project {pid}",
        n=len(cache_ids), pid=project_id,
    )
    return len(cache_ids)


def step_indicate_cache_drop(
    session: MstrRestSession,
    report_id: str,
    prompt_key: str,
    ids: str,
) -> None:
    """
    Run the IndicateCacheDropHappened stored procedure via the FFSQL wrapper.

    The wrapper report takes a single value prompt (default key
    `MIDWHLoadHistoryIds`) and emits a dummy `SELECT 1 AS Status` so it
    qualifies as a Freeform SQL report.
    """
    logger.info(
        "Step 4: IndicateCacheDropHappened ({rid}) — IDs={ids!r}",
        rid=report_id, ids=ids,
    )
    prompt_answers = [
        {
            "key": prompt_key,         # title-or-key resolution depends on prompt
            "type": "VALUE",
            "answers": [{"value": ids}],
        }
    ]
    _execute_freeform_report(session, report_id, prompt_answers=prompt_answers)
    logger.success("IndicateCacheDropHappened executed.")


def step_trigger_event(session: MstrRestSession, event_name: str) -> None:
    """Trigger an I-Server event by name via mstrio-py."""
    logger.info("Triggering event: {ev!r}", ev=event_name)
    try:
        from mstrio.distribution_services.event import Event
    except ImportError as exc:
        raise ImportError(
            "mstrio.distribution_services.event.Event is required to trigger "
            "events. Update mstrio-py."
        ) from exc

    ev = Event(connection=session.mstrio_conn, name=event_name)
    ev.trigger()
    logger.success("Event triggered: {ev!r}", ev=event_name)


# ── Failure notification ──────────────────────────────────────────────────────


def send_failure_email(cfg: WorkflowConfig, subject: str, body: str) -> None:
    """
    Send a high-importance failure email via SMTP.

    Logs and silently returns if SMTP / recipient is not configured. On send
    failure logs an error but does not raise — the caller is expected to exit
    non-zero so the calling MCE Workflow handles escalation.
    """
    if not cfg.notification_email:
        logger.warning("No notification_email configured — skipping email.")
        return
    if not cfg.smtp_server:
        logger.warning("No smtp_server configured — skipping email.")
        return

    try:
        msg = MIMEText(body, _charset="utf-8")
        msg["Subject"] = subject
        msg["From"] = cfg.smtp_from or cfg.notification_email
        msg["To"] = cfg.notification_email
        msg["X-Priority"] = "1 (Highest)"
        msg["Importance"] = "High"

        with smtplib.SMTP(cfg.smtp_server, cfg.smtp_port, timeout=30) as smtp:
            smtp.ehlo()
            if cfg.smtp_use_tls:
                smtp.starttls()
                smtp.ehlo()
            if cfg.smtp_username and cfg.smtp_password:
                smtp.login(cfg.smtp_username, cfg.smtp_password)
            smtp.send_message(msg)

        logger.info("Failure email sent to {to}", to=cfg.notification_email)
    except Exception as exc:
        logger.error(
            "Failed to send failure email via {host}:{port} — {exc}",
            host=cfg.smtp_server, port=cfg.smtp_port, exc=exc,
        )


# ── Session / project setup ───────────────────────────────────────────────────


def _build_session(env: str) -> MstrRestSession:
    """
    Build an MstrRestSession for the requested environment.

    Standalone:  reads MSTR_{ENV}_* env vars (.env file).
    MCE runtime: adapt this to use `get_connection(workstationData)` and
                 wrap it in a session-like shim — see comment below.
    """
    config = MstrConfig(environment=MstrEnvironment(env))
    return MstrRestSession(config)


def _set_project(session: MstrRestSession, cfg: WorkflowConfig) -> str:
    if cfg.project_id:
        return session.set_project(project_id=cfg.project_id)
    return session.set_project(name=cfg.project_name)


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Run the MarketIntelligence ClearCache workflow — Python conversion "
            "of the legacy System Manager process."
        )
    )
    p.add_argument(
        "--config", "-c",
        type=Path,
        default=Path(DEFAULT_CONFIG),
        help=f"YAML config file (default: {DEFAULT_CONFIG})",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Execute mutating steps (cache invalidate, SP call, event triggers). "
             "Without this flag, runs in dry-run mode.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    cfg = load_config(args.config)

    started = time.time()
    logger.info(
        "MarketIntelligence ClearCache — env={env}, project={p}, apply={apply}",
        env=cfg.env,
        p=cfg.project_id or cfg.project_name,
        apply=args.apply,
    )

    with _build_session(cfg.env) as session:
        project_id = _set_project(session, cfg)
        logger.info("Active project: {pid}", pid=project_id)

        # ── Step 1: Check refresh ─────────────────────────────────────────────
        try:
            refreshed = step_check_dwh_refresh_happened(
                session, cfg.check_refresh_report_id
            )
        except Exception as exc:
            logger.error("CheckIfMIDWHRefreshHappened failed: {exc}", exc=exc)
            send_failure_email(
                cfg,
                subject="MarketIntelligence ClearCache - SQL Failed",
                body=f"The Freeform SQL report CheckIfMIDWHRefreshHappened "
                     f"({cfg.check_refresh_report_id}) failed:\n\n{exc}",
            )
            return 1

        if not refreshed:
            logger.info("No outstanding DWH refresh — exiting cleanly.")
            return 0

        # ── Step 2: Retrieve IDs ──────────────────────────────────────────────
        try:
            ids = step_retrieve_load_history_ids(
                session, cfg.retrieve_ids_report_id
            )
        except Exception as exc:
            logger.error("RetrieveIDs failed: {exc}", exc=exc)
            send_failure_email(
                cfg,
                subject="MarketIntelligence ClearCache - SQL Failed",
                body=f"The Freeform SQL report RetrieveIDs "
                     f"({cfg.retrieve_ids_report_id}) failed:\n\n{exc}",
            )
            return 1

        if not ids:
            logger.warning("Refresh flag is set but RetrieveIDs returned empty — exiting.")
            return 0

        # ── Step 3: Invalidate caches ─────────────────────────────────────────
        if not args.apply:
            logger.info("[DRY RUN] Would invalidate caches for project {pid}", pid=project_id)
        else:
            try:
                step_invalidate_caches(session, project_id)
            except Exception as exc:
                logger.error("Cache invalidation failed: {exc}", exc=exc)
                send_failure_email(
                    cfg,
                    subject="MarketIntelligence ClearCache - Cache Invalidate Failed",
                    body=f"Cache invalidation for project {project_id} failed:\n\n{exc}",
                )
                return 1

        # ── Step 4: IndicateCacheDropHappened ────────────────────────────────
        if not args.apply:
            logger.info(
                "[DRY RUN] Would EXEC IndicateCacheDropHappened with IDs={ids}",
                ids=ids,
            )
        else:
            try:
                step_indicate_cache_drop(
                    session,
                    cfg.indicate_cache_drop_report_id,
                    cfg.indicate_cache_drop_prompt_key,
                    ids,
                )
            except Exception as exc:
                logger.error("IndicateCacheDropHappened failed: {exc}", exc=exc)
                send_failure_email(
                    cfg,
                    subject="MarketIntelligence ClearCache - SQL Failed",
                    body=f"The Stored Procedure IndicateCacheDropHappened has failed:\n\n{exc}",
                )
                return 1

        # ── Step 5: Trigger refresh-cubes event ───────────────────────────────
        if not args.apply:
            logger.info(
                "[DRY RUN] Would trigger event: {ev!r}",
                ev=cfg.refresh_cubes_event,
            )
        else:
            try:
                step_trigger_event(session, cfg.refresh_cubes_event)
            except Exception as exc:
                logger.error(
                    "Failed to trigger {ev!r}: {exc}",
                    ev=cfg.refresh_cubes_event, exc=exc,
                )
                send_failure_email(
                    cfg,
                    subject="MarketIntelligence ClearCache - Event Trigger Failed",
                    body=f"Failed to trigger event "
                         f"{cfg.refresh_cubes_event!r}:\n\n{exc}",
                )
                return 1

        # ── Step 6: Trigger notify-users event (best effort) ──────────────────
        if not args.apply:
            logger.info(
                "[DRY RUN] Would trigger event: {ev!r}",
                ev=cfg.notify_event,
            )
        else:
            try:
                step_trigger_event(session, cfg.notify_event)
            except Exception as exc:
                # Notification is best-effort — log but don't escalate to email
                logger.warning(
                    "Failed to trigger notify event {ev!r}: {exc}",
                    ev=cfg.notify_event, exc=exc,
                )

    elapsed = time.time() - started
    logger.success(
        "ClearCache workflow finished in {sec:.1f}s (apply={apply})",
        sec=elapsed, apply=args.apply,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
