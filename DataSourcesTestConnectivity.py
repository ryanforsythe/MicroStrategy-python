"""
DataSourcesTestConnectivity.py

Test the connectivity of one or more MicroStrategy datasource connections.

Uses mstrio-py's internal REST call:
    POST /api/datasources/connections/test
    body: {"id": "<connection_id>"}
    → HTTP 204  connection successful
    → HTTP 400  connection failed (response body contains error detail)

Input modes
───────────
  Single      --id <instance_guid>
                  Test one instance by GUID supplied on the command line.

  JSON config --input <file>.json
                  Read instance_id + connection_id from a structured JSON file
                  (default: DBInstance_DSN_Config.json).
                  Use --instance-id GUID ... to limit to a subset.

  ID list     --input <file>.csv  |  --input <file>.txt
                  Plain list of DatasourceInstance GUIDs — one per line for
                  .txt, first column for .csv (header optional).
                  Blank lines and lines starting with # are ignored.
                  Each GUID is resolved via the API to get the connection info
                  before the connectivity test runs.

Connection
──────────
  Default  : mstrio_core / .env  (CLI / PyCharm)
  Workstation: search "WORKSTATION" below to swap.

Usage
─────
  python DataSourcesTestConnectivity.py qa
  python DataSourcesTestConnectivity.py qa --id B1913336469D067F981ED6BBD8C11DAB
  python DataSourcesTestConnectivity.py qa --instance-id GUID1 GUID2
  python DataSourcesTestConnectivity.py qa --input DBInstance_DSN_Config.json
  python DataSourcesTestConnectivity.py qa --input my_instances.txt
  python DataSourcesTestConnectivity.py qa --input my_instances.csv
"""

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger
from mstrio.api.datasources import test_datasource_connection as _api_test
from mstrio.datasources import DatasourceInstance

from mstrio_core import MstrConfig, get_mstrio_connection, write_csv
from mstrio_core.config import MstrEnvironment

# ── Workstation import (uncomment when running from Workstation) ───────────────
# from mstrio.connection import get_connection

# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

DEFAULT_INPUT   = Path(__file__).parent / 'DBInstance_DSN_Config.json'
OUTPUT_FILENAME = 'datasources_connectivity_{env}_{ts}.csv'

RESULT_COLUMNS = [
    'instance_id',
    'instance_name',
    'connection_id',
    'connection_name',
    'result',           # PASS / FAIL / ERROR
    'http_status',
    'error_code',
    'error_message',
]

# 32-character hex GUID (with or without hyphens)
_GUID_RE = re.compile(r'^[0-9A-Fa-f]{32}$|^[0-9A-Fa-f-]{36}$')

# ══════════════════════════════════════════════════════════════════════════════
#  CORE TEST FUNCTION
# ══════════════════════════════════════════════════════════════════════════════

def _test_one(
    conn,
    instance_id: str,
    instance_name: str,
    connection_id: str,
    connection_name: str,
) -> dict:
    """
    Run POST /api/datasources/connections/test for one connection.
    Returns a result dict.
    """
    row = {
        'instance_id':     instance_id,
        'instance_name':   instance_name,
        'connection_id':   connection_id,
        'connection_name': connection_name,
        'result':          '',
        'http_status':     '',
        'error_code':      '',
        'error_message':   '',
    }

    if not connection_id:
        row['result']        = 'ERROR'
        row['error_message'] = 'No connection_id available'
        return row

    try:
        r = _api_test(conn, body={'id': connection_id})
        row['http_status'] = r.status_code

        if r.ok:                        # 204 No Content → success
            row['result'] = 'PASS'
        else:                           # 400 or other failure
            row['result'] = 'FAIL'
            try:
                err    = r.json()
                errors = err.get('errors') or [err]
                first  = errors[0] if errors else {}
                row['error_code']    = first.get('code', '')
                row['error_message'] = first.get('message', r.text[:300])
            except Exception:
                row['error_message'] = r.text[:300]

    except Exception as exc:
        row['result']        = 'ERROR'
        row['error_message'] = str(exc)

    return row


# ══════════════════════════════════════════════════════════════════════════════
#  INPUT HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _load_from_instance(conn, instance_id: str) -> dict:
    """
    Fetch a DatasourceInstance by ID and return the four identity fields
    needed by _test_one().  Used for --id and for ID-list file entries.
    """
    inst    = DatasourceInstance(conn, id=instance_id)
    ds_conn = inst.datasource_connection
    return {
        'instance_id':     inst.id,
        'instance_name':   inst.name,
        'connection_id':   getattr(ds_conn, 'id', ''),
        'connection_name': getattr(ds_conn, 'name', ''),
    }


def _entries_from_json(path: Path, filter_ids: Optional[list[str]]) -> list[dict]:
    """
    Read the structured JSON config (DBInstance_DSN_Config.json format).
    Returns fully-populated entry dicts; no API calls needed.
    Optionally filters to a subset of instance GUIDs.
    """
    data = json.loads(path.read_text(encoding='utf-8'))

    if filter_ids:
        upper = {i.upper() for i in filter_ids}
        data  = [e for e in data if (e.get('instance_id') or '').upper() in upper]

    return [
        {
            'instance_id':     e.get('instance_id', ''),
            'instance_name':   e.get('instance_name', ''),
            'connection_id':   e.get('connection_id', ''),
            'connection_name': e.get('connection_name', ''),
        }
        for e in data
    ]


def _read_id_file(path: Path) -> list[str]:
    """
    Read a plain-text or CSV file that contains a list of GUIDs.

    .txt — one GUID per line; blank lines and lines starting with # ignored.
    .csv — first column used; header row skipped when it is not a GUID.

    Returns a deduplicated list of GUID strings preserving file order.
    """
    suffix = path.suffix.lower()
    lines: list[str] = []

    if suffix == '.csv':
        with path.open(newline='', encoding='utf-8-sig') as fh:
            reader = csv.reader(fh)
            for row in reader:
                if not row:
                    continue
                val = row[0].strip()
                if val.startswith('#') or not val:
                    continue
                lines.append(val)
    else:   # .txt or any other plain-text extension
        for line in path.read_text(encoding='utf-8-sig').splitlines():
            val = line.strip()
            if val and not val.startswith('#'):
                lines.append(val)

    # Remove header row if the first entry is not a GUID
    if lines and not _GUID_RE.match(lines[0].replace('-', '')):
        logger.debug('Skipping non-GUID header row: {h!r}', h=lines[0])
        lines = lines[1:]

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for val in lines:
        key = val.upper()
        if key not in seen:
            seen.add(key)
            unique.append(val)

    return unique


def _entries_from_id_file(conn, path: Path) -> list[dict]:
    """
    Read GUIDs from a .txt or .csv file, then resolve each via the API
    to get instance name and connection info.

    Entries that fail to resolve are still included in the work list with
    empty name / connection_id so they appear as ERROR in the results.
    """
    raw_ids = _read_id_file(path)
    logger.info('Read {n} GUID(s) from {p}', n=len(raw_ids), p=path.name)

    entries: list[dict] = []
    for raw_id in raw_ids:
        try:
            entry = _load_from_instance(conn, raw_id)
            logger.debug(
                'Resolved {id} → {name}  (conn: {cid})',
                id=raw_id, name=entry['instance_name'], cid=entry['connection_id'],
            )
        except Exception as exc:
            logger.warning(
                'Could not resolve instance {id}: {exc} — will appear as ERROR',
                id=raw_id, exc=exc,
            )
            entry = {
                'instance_id':     raw_id,
                'instance_name':   '',
                'connection_id':   '',
                'connection_name': '',
            }
        entries.append(entry)

    return entries


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main(
    env: str,
    single_id: Optional[str],
    input_path: Path,
    filter_ids: Optional[list[str]],
    output_dir: Optional[Path],
) -> int:

    config  = MstrConfig(environment=MstrEnvironment(env))
    out_dir = output_dir or config.output_dir

    # ── CONNECTION ─────────────────────────────────────────────────────────────

    # ── Option A: mstrio_core / .env (CLI / PyCharm) ──────────────────────────
    conn = get_mstrio_connection()

    # ── Option B: Workstation (WORKSTATION) ───────────────────────────────────
    # Comment out Option A above and uncomment below when running from Workstation.
    # conn = get_connection(workstationData)  # noqa: F821

    # ── BUILD WORK LIST ────────────────────────────────────────────────────────
    if single_id:
        logger.info('Loading instance {id}...', id=single_id)
        try:
            entries = [_load_from_instance(conn, single_id)]
        except Exception as exc:
            logger.error('Failed to load instance {id}: {exc}', id=single_id, exc=exc)
            return 1

    else:
        suffix = input_path.suffix.lower()
        if suffix == '.json':
            logger.info('Reading JSON config from {p}', p=input_path)
            entries = _entries_from_json(input_path, filter_ids)
        elif suffix in ('.csv', '.txt'):
            if filter_ids:
                logger.warning(
                    '--instance-id filter is ignored for CSV/TXT input — '
                    'include only the desired GUIDs in the file itself.'
                )
            logger.info('Reading ID list from {p}', p=input_path)
            entries = _entries_from_id_file(conn, input_path)
        else:
            logger.error(
                'Unsupported file type {s!r}. Use .json, .csv, or .txt.',
                s=suffix,
            )
            return 1

        logger.info('{n} instance(s) queued for testing', n=len(entries))

    if not entries:
        logger.warning('No entries to test.')
        return 0

    # ── TEST EACH CONNECTION ───────────────────────────────────────────────────
    results = []
    passed  = 0
    failed  = 0
    errored = 0

    for i, entry in enumerate(entries, 1):
        name = entry['instance_name'] or entry['instance_id']
        logger.info(
            '[{i}/{t}] {n}  (conn: {c})',
            i=i, t=len(entries), n=name, c=entry['connection_id'] or '—',
        )

        row = _test_one(conn, **entry)
        results.append(row)

        if row['result'] == 'PASS':
            passed  += 1
            logger.success('  PASS  {n}', n=name)
        elif row['result'] == 'FAIL':
            failed  += 1
            logger.error(
                '  FAIL  {n}  [{code}] {msg}',
                n=name, code=row['error_code'], msg=row['error_message'][:120],
            )
        else:
            errored += 1
            logger.error('  ERROR {n}  {msg}', n=name, msg=row['error_message'][:120])

    # ── SUMMARY ────────────────────────────────────────────────────────────────
    logger.info(
        'Results — PASS: {p}  FAIL: {f}  ERROR: {e}  Total: {t}',
        p=passed, f=failed, e=errored, t=len(results),
    )

    # ── WRITE CSV ──────────────────────────────────────────────────────────────
    ts       = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_file = out_dir / OUTPUT_FILENAME.format(env=env, ts=ts)
    rows     = [[r.get(c, '') for c in RESULT_COLUMNS] for r in results]
    write_csv(rows, columns=RESULT_COLUMNS, path=out_file)
    logger.success('Results written → {p}', p=out_file)

    return 0 if (failed + errored) == 0 else 1


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=(
            'Test connectivity for MicroStrategy datasource connections. '
            'Accepts a single GUID, a JSON config file, a plain-text GUID list, '
            'or a CSV file. Uses POST /api/datasources/connections/test internally.'
        )
    )
    parser.add_argument(
        'env',
        choices=[e.value for e in MstrEnvironment],
        help='MicroStrategy environment (dev / qa / prod).',
    )

    src = parser.add_mutually_exclusive_group()
    src.add_argument(
        '--id',
        dest='single_id',
        metavar='GUID',
        help='Test a single DatasourceInstance by GUID.',
    )
    src.add_argument(
        '--input', '-i',
        type=Path,
        default=DEFAULT_INPUT,
        metavar='FILE',
        help=(
            'Input file. '
            '.json = structured config (default: DBInstance_DSN_Config.json); '
            '.csv / .txt = plain list of DatasourceInstance GUIDs.'
        ),
    )

    parser.add_argument(
        '--instance-id',
        nargs='+',
        metavar='GUID',
        dest='filter_ids',
        help='Limit to specific instance GUIDs (JSON input only).',
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=None,
        help='Directory for the results CSV (default: MSTR_OUTPUT_DIR).',
    )

    args = parser.parse_args()

    raise SystemExit(
        main(
            env=args.env,
            single_id=args.single_id,
            input_path=args.input,
            filter_ids=args.filter_ids,
            output_dir=args.output_dir,
        )
    )
