"""
SubscriptionsExport.py

Export all subscription information across every loaded project.

Iterates every loaded project, retrieves all subscriptions (including last_run
where the server supports it), and writes one row per recipient so the output
can be sorted or filtered by user, delivery mode, schedule, etc.

Output columns
──────────────
  project_id / project_name
  subscription_id / subscription_name
  delivery_mode            EMAIL | FILE | FTP | HISTORY_LIST | CACHE | MOBILE | …
  delivery_filename        Filename template on the subscription delivery settings
                           (same for all recipients of a given subscription).
  email_subject            Email subject line (EMAIL mode only).
  schedule_ids             Semicolon-separated schedule GUIDs.
  schedule_names           Semicolon-separated schedule names.
  content_ids              Semicolon-separated content GUIDs.
  content_names            Semicolon-separated content names.
  content_types            Semicolon-separated content types (report/document/…).
  owner_id / owner_name
  date_created / date_modified
  last_run                 Last execution time (requires server ≥ 11.4.0600).
  recipient_id / recipient_name
  recipient_type           USER | CONTACT | USER_GROUP | CONTACT_GROUP | PERSONAL_ADDRESS
  recipient_include_type   TO | CC | BCC
  physical_address         Physical delivery address when embedded in the
                           API response (email address or file path).

Connection
──────────
  Default     : mstrio_core / .env  (CLI / PyCharm)
  Workstation : search "WORKSTATION" below to swap.

Usage
─────
  python SubscriptionsExport.py dev
  python SubscriptionsExport.py dev --format json
  python SubscriptionsExport.py dev --output-dir C:/tmp
  python SubscriptionsExport.py dev --project-id GUID1 GUID2   # limit to specific projects
"""

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger
from mstrio.server import Environment

from mstrio_core import MstrConfig, get_mstrio_connection, write_csv
from mstrio_core.config import MstrEnvironment

# ── Workstation import (uncomment when running from Workstation) ──────────────
# from mstrio.connection import get_connection

# ── Lazy import — list_subscriptions may not exist on older mstrio-py builds ─
try:
    from mstrio.distribution_services import list_subscriptions
except ImportError:
    list_subscriptions = None  # handled gracefully in main

# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

CSV_FILENAME  = 'subscriptions_export_{env}_{ts}.csv'
JSON_FILENAME = 'subscriptions_export_{env}_{ts}.json'

COLUMNS = [
    'project_id',
    'project_name',
    'subscription_id',
    'subscription_name',
    'delivery_mode',
    'delivery_filename',
    'email_subject',
    'schedule_ids',
    'schedule_names',
    'content_ids',
    'content_names',
    'content_types',
    'owner_id',
    'owner_name',
    'date_created',
    'date_modified',
    'last_run',
    'recipient_id',
    'recipient_name',
    'recipient_type',
    'recipient_include_type',
    'physical_address',
]


# ══════════════════════════════════════════════════════════════════════════════
#  RAW-DICT HELPERS
# ══════════════════════════════════════════════════════════════════════════════
# list_subscriptions(to_dictionary=True) returns the raw API dicts (camelCase).
# The helpers below accept both camelCase and snake_case keys defensively.

def _v(d: dict, *keys, default: str = '') -> str:
    """Return the first non-None value found under any of the given keys."""
    for k in keys:
        v = d.get(k)
        if v is not None:
            return v
    return default


def _fmt_dt(val) -> str:
    """Format a datetime or ISO string for output."""
    if val is None:
        return ''
    if isinstance(val, datetime):
        return val.strftime('%Y-%m-%d %H:%M:%S')
    return str(val).replace('T', ' ').replace('Z', '')


def _join(items: list, key: str) -> str:
    """Build a semicolon-separated string of `key` from a list of dicts."""
    return '; '.join(
        str(i.get(key) or '')
        for i in (items or [])
        if i.get(key)
    )


def _delivery_fields(delivery: dict) -> tuple[str, str, str]:
    """
    Return (mode, delivery_filename, email_subject) from the raw delivery dict.

    The delivery dict has a top-level 'mode' key and a nested sub-object keyed
    by the mode name in lowercase, e.g. {'mode': 'FILE', 'file': {'filename': '…'}}.
    """
    if not delivery:
        return '', '', ''

    mode = (_v(delivery, 'mode') or '').upper()
    mode_key = mode.lower()
    sub = delivery.get(mode_key) or {}

    filename = _v(sub, 'filename', 'fileName', 'file_name')
    subject  = _v(sub, 'subject')

    return mode, filename, subject


def _recipient_address(rec: dict) -> str:
    """
    Extract the physical delivery address embedded in a recipient dict when
    the API includes it (not all server versions do).

    Expected shape:
        {"address": {"physicalAddress": "user@example.com"}}
    or  {"address": {"physical_address": "\\\\server\\path"}}
    """
    addr = rec.get('address') or {}
    return _v(addr, 'physicalAddress', 'physical_address')


def _flatten_subscription(
    sub: dict,
    project_id: str,
    project_name: str,
) -> list[dict]:
    """
    Expand a raw subscription dict into one output row per recipient.
    Returns [] for subscriptions with no recipients (should not occur in
    practice, but handled defensively).
    """
    sub_id   = _v(sub, 'id')
    sub_name = _v(sub, 'name')

    owner    = sub.get('owner') or {}
    owner_id = _v(owner, 'id')
    owner_nm = _v(owner, 'name')

    schedules = sub.get('schedules') or []
    contents  = sub.get('contents')  or []

    sch_ids   = _join(schedules, 'id')
    sch_names = _join(schedules, 'name')
    cnt_ids   = _join(contents,  'id')
    cnt_names = _join(contents,  'name')
    cnt_types = _join(contents,  'type')

    delivery = sub.get('delivery') or {}
    mode, filename, subject = _delivery_fields(delivery)

    date_created  = _fmt_dt(_v(sub, 'dateCreated',  'date_created'))
    date_modified = _fmt_dt(_v(sub, 'dateModified', 'date_modified'))
    last_run      = _fmt_dt(_v(sub, 'lastRun',      'last_run'))

    recipients = sub.get('recipients') or []
    if not recipients:
        return []

    rows = []
    for rec in recipients:
        rows.append({
            'project_id':           project_id,
            'project_name':         project_name,
            'subscription_id':      sub_id,
            'subscription_name':    sub_name,
            'delivery_mode':        mode,
            'delivery_filename':    filename,
            'email_subject':        subject,
            'schedule_ids':         sch_ids,
            'schedule_names':       sch_names,
            'content_ids':          cnt_ids,
            'content_names':        cnt_names,
            'content_types':        cnt_types,
            'owner_id':             owner_id,
            'owner_name':           owner_nm,
            'date_created':         date_created,
            'date_modified':        date_modified,
            'last_run':             last_run,
            'recipient_id':         _v(rec, 'id'),
            'recipient_name':       _v(rec, 'name'),
            'recipient_type':       _v(rec, 'type'),
            'recipient_include_type': _v(rec, 'includeType', 'include_type'),
            'physical_address':     _recipient_address(rec),
        })

    return rows


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main(
    env: str,
    fmt: str,
    output_dir: Optional[Path],
    filter_project_ids: Optional[list[str]],
) -> int:

    if list_subscriptions is None:
        logger.error(
            'mstrio.distribution_services.list_subscriptions not available. '
            'Upgrade mstrio-py.'
        )
        return 1

    config  = MstrConfig(environment=MstrEnvironment(env))
    out_dir = output_dir or config.output_dir

    # ── CONNECTION ────────────────────────────────────────────────────────────
    # Option A: mstrio_core / .env (CLI / PyCharm)
    conn = get_mstrio_connection(config=config)

    # Option B: Workstation (WORKSTATION)
    # Comment out Option A above and uncomment below.
    # conn = get_connection(workstationData)  # noqa: F821

    # ── PROJECTS ──────────────────────────────────────────────────────────────
    all_projects = Environment(conn).list_projects()
    if filter_project_ids:
        upper = {p.upper() for p in filter_project_ids}
        all_projects = [p for p in all_projects if p.id.upper() in upper]

    logger.info('{n} project(s) to process', n=len(all_projects))

    all_rows: list[dict] = []
    total_subs = 0
    total_rows = 0

    for proj in all_projects:
        logger.info('  [{name}]  fetching subscriptions...', name=proj.name)
        try:
            subs = list_subscriptions(
                conn,
                project_id=proj.id,
                last_run=True,
                to_dictionary=True,
            )
        except Exception as exc:
            logger.warning(
                '  [{name}] failed to list subscriptions: {exc}',
                name=proj.name, exc=exc,
            )
            continue

        logger.info(
            '  [{name}]  {n} subscription(s)', name=proj.name, n=len(subs),
        )
        total_subs += len(subs)

        for sub in subs:
            rows = _flatten_subscription(sub, proj.id, proj.name)
            all_rows.extend(rows)
            total_rows += len(rows)

    logger.info(
        'Total: {s} subscription(s) → {r} row(s) ({p} project(s))',
        s=total_subs, r=total_rows, p=len(all_projects),
    )

    # ── OUTPUT ────────────────────────────────────────────────────────────────
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_dir.mkdir(parents=True, exist_ok=True)

    if fmt == 'json':
        out_file = out_dir / JSON_FILENAME.format(env=env, ts=ts)
        out_file.write_text(
            json.dumps(all_rows, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
        logger.success('JSON written → {p}  ({n} rows)', p=out_file, n=total_rows)
        print(f'\nExported: {out_file}')
    else:
        out_file = out_dir / CSV_FILENAME.format(env=env, ts=ts)
        csv_rows = [[r.get(c, '') for c in COLUMNS] for r in all_rows]
        write_csv(csv_rows, columns=COLUMNS, path=out_file)
        logger.success('CSV written → {p}  ({n} rows)', p=out_file, n=total_rows)
        print(f'\nExported: {out_file}')

    return 0


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=(
            'Export all MicroStrategy subscriptions across every loaded project. '
            'One row per recipient.'
        )
    )
    parser.add_argument(
        'env',
        choices=[e.value for e in MstrEnvironment],
        help='MicroStrategy environment (dev / qa / prod).',
    )
    parser.add_argument(
        '--format', '-f',
        choices=['csv', 'json'],
        default='csv',
        dest='fmt',
        help='Output format (default: csv).',
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=None,
        metavar='DIR',
        help='Output directory (default: MSTR_OUTPUT_DIR).',
    )
    parser.add_argument(
        '--project-id',
        nargs='+',
        metavar='GUID',
        dest='filter_project_ids',
        help='Limit to specific project GUIDs (default: all loaded projects).',
    )

    args = parser.parse_args()

    raise SystemExit(
        main(
            env=args.env,
            fmt=args.fmt,
            output_dir=args.output_dir,
            filter_project_ids=args.filter_project_ids,
        )
    )
