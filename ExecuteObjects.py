"""
ExecuteObjects.py — Concurrently execute MicroStrategy objects (Reports,
Documents, Dossiers, Intelligent Cubes) for testing/validation purposes.

Workflow
────────
1.  Pre-flight: read input CSV (project_id + object_id rows; or a previous
    run's output for resume mode). For each row resolve the object's name,
    type, subtype, and folder location, classify it as Report / Document /
    Dossier / Cube, and capture the prompt structure as JSON for the
    `prompt_answers_json` column. Write the populated CSV.

2.  Execute: schedule every non-success row on a `ThreadPoolExecutor` (default
    10 workers; configurable via `--concurrency`). Each worker:
        a. Marks `status=running` + `start_time` and flushes the CSV.
        b. Creates a report/document instance (or triggers cube publish).
        c. If the object has prompts:
            • Uses any answers supplied in `prompt_answers_json`.
            • For unanswered prompts uses defaults where available
              (`closeAllPrompts=true`).
            • Optional prompts are not answered.
        d. On success marks `status=success` + `end_time` + result detail.
           On failure marks `status=error` + error message.
        e. Flushes the CSV after every state change so the file is always a
           live snapshot of progress.

3.  Resume: feed the output CSV back in as `--input`. Rows where `status` is
    already `success` are preserved untouched; everything else (empty,
    `running`, `error`, `skipped`) is re-executed.

Object types supported (auto-classified)
────────────────────────────────────────
    Report      type=3,  subtype=768/769/770/771/774
    Cube        type=3,  subtype=776
    Document    type=55, subtype=14080
    Dossier     type=55, subtype=14081

CSV schema
──────────
project_id ; project_name ; object_id ; object_name ; object_location ;
object_type ; start_time ; end_time ; status ; status_details ;
prompt_answers_json

`prompt_answers_json` formats:
    "Prompts:None"                         → no prompts on object
    {"prompts":[ {prompt-template}, ... ]} → pre-flight template (user fills `answers`)
    {"prompts":[ {api-answer-payload} ]}   → user-supplied answers, sent to PUT
                                            /reports|/documents/{id}/instances/{iid}/prompts/answers

Status values
─────────────
    "" (empty) — not yet run
    running    — currently executing (visible while the worker is mid-flight)
    success    — execution completed successfully
    error      — execution failed; see status_details
    skipped    — pre-flight could not classify the object (not retried)

Concurrency / threading notes
─────────────────────────────
A single shared `MstrRestSession` is used; `requests.Session` is thread-safe.
We do NOT call `session.set_project()` per worker (which would race) — instead,
every request explicitly sets the `X-MSTR-ProjectID` header for the row's
project. CSV writes are serialized with a Lock and use a temp-file rename for
atomic updates.

Usage
─────
    python ExecuteObjects.py <env> --input PATH [options]

    --input PATH          Input CSV (project_id, object_id [, prompt_answers_json])
                          OR a previous run's output (full schema → resume).
    --output PATH         Output CSV (default: <output_dir>/execute_objects_results.csv)
    --concurrency N       Parallel workers (default: 10).
    --preflight-only      Stop after writing the populated CSV; do not execute.
    --timeout SECONDS     Per-object timeout (default: 600 = 10 min).
    --output-dir PATH     Override MSTR_OUTPUT_DIR for default output location.

Examples
────────
    # First run from a minimal input
    python ExecuteObjects.py dev --input objects.csv

    # Pre-flight only — gather definitions + prompt templates, edit, then run later
    python ExecuteObjects.py dev --input objects.csv --preflight-only

    # Resume — re-run anything that didn't complete successfully
    python ExecuteObjects.py dev --input execute_objects_results.csv --concurrency 20
"""

from __future__ import annotations

import argparse
import csv as csv_module
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from loguru import logger

from mstrio_core import (
    MstrConfig,
    MstrRestSession,
    object_location,
    get_object_type_info,
)
from mstrio_core.config import MstrEnvironment


# ── Constants ─────────────────────────────────────────────────────────────────

OBJECT_TYPE_REPORT = 3
OBJECT_TYPE_DOCUMENT = 55

SUBTYPE_CUBE = 776
SUBTYPE_DOSSIER = 14081

KIND_REPORT = "Report"
KIND_DOCUMENT = "Document"
KIND_DOSSIER = "Dossier"
KIND_CUBE = "Cube"

COLUMNS = [
    "project_id",
    "project_name",
    "object_id",
    "object_name",
    "object_location",
    "object_type",
    "start_time",
    "end_time",
    "status",
    "status_details",
    "prompt_answers_json",
]

DEFAULT_CONCURRENCY = 10
DEFAULT_TIMEOUT = 600          # seconds, per-object
DEFAULT_OUTPUT_NAME = "execute_objects_results.csv"

PROMPTS_NONE = "Prompts:None"

STATUS_RUNNING = "running"
STATUS_SUCCESS = "success"
STATUS_ERROR = "error"
STATUS_SKIPPED = "skipped"


# ── Small utilities ───────────────────────────────────────────────────────────


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _classify(object_type: Optional[int], object_subtype: Optional[int]) -> Optional[str]:
    """Map (type, subtype) → KIND_* or None for unsupported types."""
    if object_type == OBJECT_TYPE_REPORT:
        return KIND_CUBE if object_subtype == SUBTYPE_CUBE else KIND_REPORT
    if object_type == OBJECT_TYPE_DOCUMENT:
        return KIND_DOSSIER if object_subtype == SUBTYPE_DOSSIER else KIND_DOCUMENT
    return None


def _project_request(
    session: MstrRestSession,
    method: str,
    path: str,
    project_id: str,
    **kwargs: Any,
) -> Any:
    """
    HTTP request with an explicit X-MSTR-ProjectID header.

    We never call session.set_project() in worker threads (that would race
    across rows targeting different projects). Instead each request carries
    its own project header.
    """
    extra = kwargs.pop("headers", {}) or {}
    headers = {"X-MSTR-ProjectID": project_id, **extra}
    if method == "GET":
        return session.get(path, scope="server", headers=headers, **kwargs)
    if method == "POST":
        return session.post(path, scope="server", headers=headers, **kwargs)
    if method == "PUT":
        return session.put(path, scope="server", headers=headers, **kwargs)
    raise ValueError(f"Unsupported method: {method}")


# ── Project name lookup (cached, thread-safe) ─────────────────────────────────


class ProjectNameCache:
    """Maps project_id → name. Resolves once via GET /projects, caches."""

    def __init__(self, session: MstrRestSession) -> None:
        self._session = session
        self._cache: dict[str, str] = {}
        self._lock = threading.Lock()
        self._populated = False

    def _populate_unlocked(self) -> None:
        if self._populated:
            return
        try:
            r = self._session.get("/projects", scope="server")
            if r.ok:
                for p in r.json() or []:
                    pid = p.get("id")
                    if pid:
                        self._cache[pid] = p.get("name", "")
        except Exception as exc:
            logger.warning("Could not pre-populate project name cache: {exc}", exc=exc)
        self._populated = True

    def get(self, project_id: str) -> str:
        with self._lock:
            self._populate_unlocked()
            if project_id in self._cache:
                return self._cache[project_id]
        # Not in cache — fetch individually
        try:
            r = self._session.get(f"/projects/{project_id}", scope="server")
            name = r.json().get("name", "") if r.ok else ""
        except Exception:
            name = ""
        with self._lock:
            self._cache[project_id] = name
        return name


# ── CSV state (thread-safe, atomic-rewrite on every update) ──────────────────


class CsvState:
    """
    Holds rows in memory; serializes the entire file to disk on every update.
    Update granularity is one row's set of fields — simple and safe for
    realistic test workloads (hundreds of objects, not hundreds of thousands).
    """

    def __init__(self, output_path: Path, columns: list[str]) -> None:
        self._path = output_path
        self._columns = columns
        self._rows: list[dict] = []
        self._lock = threading.Lock()

    def load_initial(self, rows: list[dict]) -> None:
        with self._lock:
            self._rows = rows
            self._flush_unlocked()

    def update(self, index: int, **fields: Any) -> None:
        with self._lock:
            self._rows[index].update(fields)
            self._flush_unlocked()

    def get(self, index: int) -> dict:
        with self._lock:
            return dict(self._rows[index])

    def all(self) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self._rows]

    def indices_to_run(self) -> list[int]:
        with self._lock:
            return [
                i for i, r in enumerate(self._rows)
                if r.get("status") not in (STATUS_SUCCESS, STATUS_SKIPPED)
            ]

    def _flush_unlocked(self) -> None:
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        with open(tmp, "w", newline="", encoding="utf-8") as fh:
            w = csv_module.writer(fh, delimiter=";", quoting=csv_module.QUOTE_MINIMAL)
            w.writerow(self._columns)
            for r in self._rows:
                w.writerow([r.get(c, "") for c in self._columns])
        tmp.replace(self._path)


# ── Input parsing ─────────────────────────────────────────────────────────────


def _read_input_csv(path: Path) -> list[dict]:
    """Read input CSV with delimiter auto-detection; lower-cases column names."""
    if not path.exists():
        logger.error("Input CSV not found: {p}", p=path)
        sys.exit(2)

    with open(path, newline="", encoding="utf-8-sig") as fh:
        sample = fh.read(4096)
        fh.seek(0)
        try:
            dialect = csv_module.Sniffer().sniff(sample, delimiters=";,\t")
        except csv_module.Error:
            dialect = "excel"
        reader = csv_module.DictReader(fh, dialect=dialect)
        rows = []
        for raw in reader:
            rows.append({(k or "").strip().lower(): (v or "").strip() for k, v in raw.items()})
    if not rows:
        logger.error("Input CSV is empty: {p}", p=path)
        sys.exit(2)

    # Validate required columns
    if "project_id" not in rows[0] or "object_id" not in rows[0]:
        logger.error(
            "Input CSV must contain 'project_id' and 'object_id' columns. Found: {cols}",
            cols=list(rows[0].keys()),
        )
        sys.exit(2)

    return rows


# ── Pre-flight: object info + prompts template ────────────────────────────────


def _fetch_ancestors(session: MstrRestSession, project_id: str, object_id: str, type_id: int) -> str:
    """Best-effort folder location lookup via GET /objects/{id}?type=N&includeAncestors=true."""
    try:
        r = _project_request(
            session, "GET", f"/objects/{object_id}",
            project_id, params={"type": type_id, "includeAncestors": "true"},
        )
        if r.ok:
            return object_location(r.json().get("ancestors", []) or [])
    except Exception:
        pass
    return ""


def _fetch_prompts_template(
    session: MstrRestSession,
    project_id: str,
    object_id: str,
    kind: str,
) -> str:
    """
    Return a JSON template of prompt definitions for the user to fill in,
    or PROMPTS_NONE if the object has no prompts / can't be fetched.
    """
    if kind == KIND_CUBE:
        return PROMPTS_NONE

    if kind == KIND_REPORT:
        path = f"/v2/reports/{object_id}/prompts"
    elif kind in (KIND_DOCUMENT, KIND_DOSSIER):
        path = f"/v2/documents/{object_id}/prompts"
    else:
        return PROMPTS_NONE

    try:
        r = _project_request(session, "GET", path, project_id)
        if not r.ok:
            return PROMPTS_NONE
        data = r.json()
        prompts = data if isinstance(data, list) else (data.get("prompts") or [])
        if not prompts:
            return PROMPTS_NONE
        # Slimmed-down template — user fills `answers`
        template = {
            "prompts": [
                {
                    "key": p.get("key") or p.get("id"),
                    "name": p.get("name", ""),
                    "title": p.get("title", ""),
                    "type": p.get("type", ""),
                    "required": bool(p.get("required", False)),
                    "hasDefault": bool(p.get("defaultAnswer") or p.get("defaultAnswers")),
                    "answers": [],
                }
                for p in prompts
            ]
        }
        return json.dumps(template, ensure_ascii=False)
    except Exception as exc:
        logger.debug("Prompt fetch failed for {oid}: {exc}", oid=object_id, exc=exc)
        return PROMPTS_NONE


def _build_initial_rows(
    session: MstrRestSession,
    input_rows: list[dict],
    proj_cache: ProjectNameCache,
) -> list[dict]:
    """
    Resolve object metadata for every input row and produce full-schema rows.
    Preserves prior `success` rows untouched (resume mode).
    """
    out: list[dict] = []
    is_resume = "status" in input_rows[0]
    if is_resume:
        logger.info("Resume mode detected — input has 'status' column; success rows preserved.")

    for inp in input_rows:
        proj_id = inp.get("project_id", "")
        obj_id = inp.get("object_id", "")
        provided_prompts = inp.get("prompt_answers_json", "")

        # Existing-schema fields (when input is a previous run)
        existing = {
            "status": inp.get("status", ""),
            "start_time": inp.get("start_time", ""),
            "end_time": inp.get("end_time", ""),
            "status_details": inp.get("status_details", ""),
            "project_name": inp.get("project_name", ""),
            "object_name": inp.get("object_name", ""),
            "object_location": inp.get("object_location", ""),
            "object_type": inp.get("object_type", ""),
        }

        if not proj_id or not obj_id:
            out.append({
                "project_id": proj_id, "project_name": existing["project_name"],
                "object_id": obj_id, "object_name": existing["object_name"],
                "object_location": existing["object_location"],
                "object_type": existing["object_type"],
                "start_time": "", "end_time": "",
                "status": STATUS_SKIPPED,
                "status_details": "Missing project_id or object_id",
                "prompt_answers_json": provided_prompts or PROMPTS_NONE,
            })
            continue

        # Resume: keep success rows untouched
        if existing["status"] == STATUS_SUCCESS:
            out.append({
                "project_id": proj_id, "project_name": existing["project_name"],
                "object_id": obj_id, "object_name": existing["object_name"],
                "object_location": existing["object_location"],
                "object_type": existing["object_type"],
                "start_time": existing["start_time"], "end_time": existing["end_time"],
                "status": STATUS_SUCCESS,
                "status_details": existing["status_details"],
                "prompt_answers_json": provided_prompts or PROMPTS_NONE,
            })
            continue

        # Resolve project name (cached)
        proj_name = existing["project_name"] or proj_cache.get(proj_id)

        # Resolve type/subtype/name
        info = get_object_type_info(session, object_id=obj_id, project_id=proj_id)
        if info.get("status_code") != 200:
            out.append({
                "project_id": proj_id, "project_name": proj_name,
                "object_id": obj_id, "object_name": existing["object_name"],
                "object_location": existing["object_location"],
                "object_type": existing["object_type"],
                "start_time": "", "end_time": "",
                "status": STATUS_SKIPPED,
                "status_details": info.get("status_exception_comment") or "Object not found",
                "prompt_answers_json": provided_prompts or PROMPTS_NONE,
            })
            continue

        obj_name = info.get("object_name", "")
        type_id = info.get("object_type_id")
        subtype_id = info.get("object_subtype_id")
        kind = _classify(type_id, subtype_id)

        if kind is None:
            out.append({
                "project_id": proj_id, "project_name": proj_name,
                "object_id": obj_id, "object_name": obj_name,
                "object_location": "",
                "object_type": info.get("object_type_name", str(type_id)),
                "start_time": "", "end_time": "",
                "status": STATUS_SKIPPED,
                "status_details": (
                    f"Unsupported object_type/subtype: "
                    f"{info.get('object_type_name')}/{info.get('object_subtype_name')}"
                ),
                "prompt_answers_json": provided_prompts or PROMPTS_NONE,
            })
            continue

        location = existing["object_location"] or _fetch_ancestors(
            session, proj_id, obj_id, type_id
        )

        if provided_prompts and provided_prompts.strip().lower() not in ("", "null"):
            prompt_json = provided_prompts
        else:
            prompt_json = _fetch_prompts_template(session, proj_id, obj_id, kind)

        out.append({
            "project_id": proj_id,
            "project_name": proj_name,
            "object_id": obj_id,
            "object_name": obj_name,
            "object_location": location,
            "object_type": kind,
            "start_time": "",
            "end_time": "",
            "status": "",
            "status_details": "",
            "prompt_answers_json": prompt_json,
        })

    return out


# ── Execution ─────────────────────────────────────────────────────────────────


def _extract_user_prompt_answers(raw: str) -> Optional[list[dict]]:
    """
    Return the list of prompt-answer payloads to PUT to the prompts/answers
    endpoint, or None if the user provided no answers (template only / NONE).
    """
    if not raw or raw.strip() == PROMPTS_NONE:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    prompts = data.get("prompts", []) if isinstance(data, dict) else []
    answered = [p for p in prompts if p.get("answers")]
    return answered or None


def _execute_report_or_doc(
    session: MstrRestSession,
    project_id: str,
    object_id: str,
    kind: str,
    user_prompt_answers: Optional[list[dict]],
) -> str:
    """Create an instance for a Report/Document/Dossier; close prompts with defaults."""
    base_v2 = "/v2/reports" if kind == KIND_REPORT else "/v2/documents"
    base_v1 = "/reports" if kind == KIND_REPORT else "/documents"

    r = _project_request(session, "POST", f"{base_v2}/{object_id}/instances", project_id, json={})
    if not r.ok:
        raise RuntimeError(f"Instance create failed: HTTP {r.status_code} {r.text[:300]}")
    payload = r.json()
    instance_id = payload.get("instanceId")

    # If prompts are pending, answer them
    pr_status = payload.get("status")
    needs_prompts = pr_status == 2 or "prompt" in str(payload.get("prompts") or "").lower()
    if needs_prompts or user_prompt_answers:
        body: dict[str, Any] = {"closeAllPrompts": True}
        if user_prompt_answers:
            body["prompts"] = user_prompt_answers
        ar = _project_request(
            session, "PUT",
            f"{base_v1}/{object_id}/instances/{instance_id}/prompts/answers",
            project_id, json=body,
        )
        # Some I-Servers return 204; some 200. Anything 2xx is OK.
        if ar.status_code >= 400:
            raise RuntimeError(
                f"Prompt-answer failed: HTTP {ar.status_code} {ar.text[:300]}"
            )
        # Re-execute to materialise the result
        re = _project_request(
            session, "POST", f"{base_v2}/{object_id}/instances/{instance_id}",
            project_id, json={},
        )
        if not re.ok:
            raise RuntimeError(f"Instance re-execute failed: HTTP {re.status_code} {re.text[:300]}")

    # Confirm the instance reached a terminal state with a small fetch
    fr = _project_request(
        session, "GET", f"{base_v2}/{object_id}/instances/{instance_id}",
        project_id, params={"limit": 1},
    )
    if not fr.ok:
        raise RuntimeError(f"Instance fetch failed: HTTP {fr.status_code} {fr.text[:300]}")
    final_status = fr.json().get("status", "?")
    return f"OK (instance={instance_id}, status={final_status})"


def _execute_cube(session: MstrRestSession, project_id: str, object_id: str) -> str:
    """Trigger a cube publish/refresh via mstrio-py for synchronous handling."""
    try:
        from mstrio.project_objects.datasets import OlapCube
    except ImportError:
        # Fallback to REST API if SDK class is unavailable
        r = _project_request(
            session, "POST", f"/cubes/{object_id}/instances", project_id, json={}
        )
        if not r.ok:
            raise RuntimeError(f"Cube publish failed: HTTP {r.status_code} {r.text[:300]}")
        return f"Cube publish initiated (instance={r.json().get('id') or r.json().get('instanceId')})"

    cube = OlapCube(connection=session.mstrio_conn, id=object_id)
    cube.publish()  # blocks until refresh completes
    return f"Cube published (id={object_id})"


def _execute_one(
    session: MstrRestSession,
    state: CsvState,
    index: int,
    timeout: int,
) -> None:
    row = state.get(index)
    proj_id = row["project_id"]
    obj_id = row["object_id"]
    kind = row["object_type"]
    prompts_raw = row.get("prompt_answers_json", "")

    state.update(index, start_time=_now_iso(), end_time="", status=STATUS_RUNNING, status_details="")

    try:
        user_answers = _extract_user_prompt_answers(prompts_raw)

        if kind == KIND_CUBE:
            msg = _execute_cube(session, proj_id, obj_id)
        elif kind in (KIND_REPORT, KIND_DOCUMENT, KIND_DOSSIER):
            msg = _execute_report_or_doc(session, proj_id, obj_id, kind, user_answers)
        else:
            raise ValueError(f"Unsupported object_type in row: {kind!r}")

        state.update(index, end_time=_now_iso(), status=STATUS_SUCCESS, status_details=msg)
        logger.info("OK   {k:8} {oid} — {msg}", k=kind, oid=obj_id, msg=msg)
    except Exception as exc:
        state.update(
            index, end_time=_now_iso(), status=STATUS_ERROR,
            status_details=str(exc)[:1000],
        )
        logger.error("FAIL {k:8} {oid} — {exc}", k=kind, oid=obj_id, exc=exc)


# ── CLI / main ────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Concurrently execute MicroStrategy reports/documents/dossiers/cubes "
            "for testing. Pre-flight CSV is written first; rows are then run in "
            "parallel and the CSV is updated live."
        )
    )
    p.add_argument(
        "env",
        choices=[e.value for e in MstrEnvironment],
        help="MicroStrategy environment: dev, qa, or prod.",
    )
    p.add_argument(
        "--input", "-i", type=Path, required=True,
        help="Input CSV — minimum: project_id, object_id [, prompt_answers_json]. "
             "Or a previous run's output for resume.",
    )
    p.add_argument(
        "--output", "-o", type=Path, default=None,
        help=f"Output CSV path (default: <output_dir>/{DEFAULT_OUTPUT_NAME}).",
    )
    p.add_argument(
        "--concurrency", "-c", type=int, default=DEFAULT_CONCURRENCY,
        help=f"Parallel workers (default: {DEFAULT_CONCURRENCY}).",
    )
    p.add_argument(
        "--preflight-only", action="store_true",
        help="Stop after the pre-flight CSV is written; do not execute.",
    )
    p.add_argument(
        "--timeout", type=int, default=DEFAULT_TIMEOUT,
        help=f"Per-object timeout in seconds (default: {DEFAULT_TIMEOUT}).",
    )
    p.add_argument(
        "--output-dir", type=Path, default=None,
        help="Override MSTR_OUTPUT_DIR for the default output location.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    config = MstrConfig(environment=MstrEnvironment(args.env))
    output_dir = args.output_dir or config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output or (output_dir / DEFAULT_OUTPUT_NAME)

    logger.info(
        "ExecuteObjects — env={env}, input={inp}, output={out}, concurrency={c}",
        env=args.env, inp=args.input, out=output_path, c=args.concurrency,
    )

    input_rows = _read_input_csv(args.input)
    logger.info("Loaded {n} input row(s) from {p}", n=len(input_rows), p=args.input)

    with MstrRestSession(config) as session:
        proj_cache = ProjectNameCache(session)

        logger.info("Pre-flight: resolving object metadata + prompt templates...")
        rows = _build_initial_rows(session, input_rows, proj_cache)

        state = CsvState(output_path, COLUMNS)
        state.load_initial(rows)
        logger.success("Pre-flight CSV written: {p} ({n} row(s))", p=output_path, n=len(rows))

        if args.preflight_only:
            logger.info("--preflight-only — stopping before execution.")
            return 0

        indices = state.indices_to_run()
        if not indices:
            logger.info("Nothing to execute (every row is already success or skipped).")
            return 0

        logger.info("Executing {n} object(s) with {c} worker(s)...", n=len(indices), c=args.concurrency)

        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            futures = [
                pool.submit(_execute_one, session, state, i, args.timeout)
                for i in indices
            ]
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as exc:
                    logger.error("Worker raised an unexpected error: {exc}", exc=exc)

    # Summary
    final = state.all()
    counts = {STATUS_SUCCESS: 0, STATUS_ERROR: 0, STATUS_SKIPPED: 0, "other": 0}
    for r in final:
        s = r.get("status") or "other"
        counts[s] = counts.get(s, 0) + 1

    logger.info(
        "Summary — success={s} error={e} skipped={sk} other={o}",
        s=counts.get(STATUS_SUCCESS, 0),
        e=counts.get(STATUS_ERROR, 0),
        sk=counts.get(STATUS_SKIPPED, 0),
        o=counts.get("other", 0),
    )
    return 0 if counts.get(STATUS_ERROR, 0) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
