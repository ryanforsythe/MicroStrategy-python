"""
SubscriptionsExport.py

Export all subscription information across every loaded project.

Iterates every loaded project, retrieves all subscriptions (including last_run
where the server supports it), and writes one row per recipient so the output
can be sorted or filtered by user, delivery mode, schedule, etc.

Output modes
────────────
  --format csv  (default) : flat, one row per recipient (see columns below).
  --format json           : COMPREHENSIVE — one full nested object per
                            subscription (raw definition: contents +
                            personalization, delivery, recipients, schedules),
                            with each non-group recipient enriched with its
                            resolved physical_address / device_id / device_name,
                            AND for prompted subscriptions the prompt definitions
                            + saved answers from /subscriptions/{id}/prompts.
                            The per-prompt `key` + `answers` are what a recreation
                            step needs to re-apply answers on a target env.

Address resolution
──────────────────
Each recipient dict includes `addressId` + `addressName` but not the physical
path.  The script resolves paths in a second pass: all unique recipient user
IDs are collected, their ContactAddresses are fetched in parallel via
ThreadPoolExecutor, and the physical_address is filled from that lookup.
Groups (isGroup=true) have no per-recipient address and are left blank.

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
  recipient_id             User or group GUID.
  recipient_name
  recipient_type           user | usergroup | contact | contactgroup | …
  recipient_include_type   TO | CC | BCC
  address_id               ContactAddress GUID (from addressId in recipient).
  address_name             ContactAddress name (from addressName in recipient).
  physical_address         Resolved from User.addresses lookup (email or file
                           path); blank for groups or when not found.
  device_id                Device GUID backing the recipient's address (from
                           ContactAddress.device); blank for groups. Join key
                           to the device → GoAnywhere mapping.
  device_name              Device name backing the recipient's address.

Connection
──────────
  Default     : mstrio_core / .env  (CLI / PyCharm)
  Workstation : search "WORKSTATION" below to swap.

Usage
─────
  python SubscriptionsExport.py dev
  python SubscriptionsExport.py dev --format json
  python SubscriptionsExport.py dev --output-dir C:/tmp
  python SubscriptionsExport.py dev --project-id GUID1 GUID2
  python SubscriptionsExport.py dev --concurrency 20
"""

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger
from mstrio.server import Environment
from mstrio.users_and_groups import User

from mstrio_core import MstrConfig, get_mstrio_connection, write_csv
from mstrio_core.config import MstrEnvironment

# ── Workstation import (uncomment when running from Workstation) ──────────────
# from mstrio.connection import get_connection

try:
    from mstrio.distribution_services import list_subscriptions
except ImportError:
    list_subscriptions = None

try:
    # Per-subscription prompt definitions + saved answers (for comprehensive JSON)
    from mstrio.api.subscriptions import get_subscription_prompts
except ImportError:
    get_subscription_prompts = None

# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

CSV_FILENAME  = 'subscriptions_export_{env}_{ts}.csv'
JSON_FILENAME = 'subscriptions_export_{env}_{ts}.json'
DEFAULT_CONCURRENCY = 10

# Group recipient types — no individual address to resolve
_GROUP_TYPES = {'usergroup', 'user_group', 'contactgroup', 'contact_group', 'group'}

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
    'address_id',
    'address_name',
    'physical_address',
    'device_id',
    'device_name',
]


# ══════════════════════════════════════════════════════════════════════════════
#  RAW-DICT HELPERS
#  list_subscriptions(to_dictionary=True) returns raw camelCase API dicts.
#  Helpers below accept both camelCase and snake_case defensively.
# ══════════════════════════════════════════════════════════════════════════════

def _v(d: dict, *keys, default: str = '') -> str:
    """Return the first non-None value found under any of the given keys."""
    for k in keys:
        v = d.get(k)
        if v is not None:
            return v
    return default


def _fmt_dt(val) -> str:
    """Format a datetime or ISO string for output; empty string if None."""
    if val is None:
        return ''
    if isinstance(val, datetime):
        return val.strftime('%Y-%m-%d %H:%M:%S')
    return str(val).replace('T', ' ').replace('Z', '')


def _join(items: list, key: str) -> str:
    """Semicolon-separated string of `key` values from a list of dicts."""
    return '; '.join(
        str(i.get(key) or '')
        for i in (items or [])
        if i.get(key)
    )


def _delivery_fields(delivery: dict) -> tuple[str, str, str]:
    """Return (mode, delivery_filename, email_subject) from the raw delivery dict."""
    if not delivery:
        return '', '', ''
    mode     = (_v(delivery, 'mode') or '').upper()
    mode_key = mode.lower()
    sub      = delivery.get(mode_key) or {}
    filename = _v(sub, 'filename', 'fileName', 'file_name')
    subject  = _v(sub, 'subject')
    return mode, filename, subject


def _is_group_recipient(rec: dict) -> bool:
    """Return True if this recipient is a group (no per-user address to resolve)."""
    if rec.get('isGroup') or rec.get('is_group'):
        return True
    rec_type = (_v(rec, 'type') or '').lower().replace(' ', '')
    return rec_type in _GROUP_TYPES


# ══════════════════════════════════════════════════════════════════════════════
#  ADDRESS LOOKUP
# ══════════════════════════════════════════════════════════════════════════════

def _build_address_lookup(
    conn,
    user_ids: set[str],
    concurrency: int,
) -> dict[str, dict[str, str]]:
    """
    Fetch ContactAddresses for every user_id in parallel.

    Returns:
        {user_id: {address_id: {"physical_address", "device_id", "device_name"}}}
    """
    if not user_ids:
        return {}

    logger.info(
        'Resolving addresses for {n} unique recipient user(s)...', n=len(user_ids),
    )

    lookup: dict[str, dict[str, dict[str, str]]] = {}

    def _fetch(uid: str) -> tuple[str, dict[str, dict[str, str]]]:
        try:
            user = User(conn, id=uid)
            addrs: dict[str, dict[str, str]] = {}
            for addr in (user.addresses or []):
                dev = addr.device
                addrs[addr.id] = {
                    'physical_address': addr.physical_address or '',
                    'device_id':        dev.id if dev else '',
                    'device_name':      dev.name if dev else '',
                }
            return uid, addrs
        except Exception as exc:
            logger.warning(
                'Could not fetch addresses for user {uid}: {err}', uid=uid, err=exc,
            )
            return uid, {}

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(_fetch, uid): uid for uid in user_ids}
        done = 0
        for future in as_completed(futures):
            uid, addrs = future.result()
            if addrs:
                lookup[uid] = addrs
            done += 1
            if done % 50 == 0:
                logger.debug('  Address lookup: {done}/{total}', done=done, total=len(user_ids))

    logger.info('Address lookup complete ({n} user(s) resolved)', n=len(lookup))
    return lookup


# ══════════════════════════════════════════════════════════════════════════════
#  FLATTENING
# ══════════════════════════════════════════════════════════════════════════════

def _flatten_subscription(
    sub: dict,
    project_id: str,
    project_name: str,
    addr_lookup: dict[str, dict[str, dict[str, str]]],
) -> list[dict]:
    """
    Expand a raw subscription dict into one output row per recipient.
    Resolves physical_address, device_id, device_name from addr_lookup.
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
        rec_id   = _v(rec, 'id')
        addr_id  = _v(rec, 'addressId',   'address_id')
        addr_nm  = _v(rec, 'addressName', 'address_name')

        if _is_group_recipient(rec):
            phys = device_id = device_name = ''
        else:
            user_addrs = addr_lookup.get(rec_id) or {}
            addr_info  = user_addrs.get(addr_id) or {}
            phys        = addr_info.get('physical_address', '')
            device_id   = addr_info.get('device_id', '')
            device_name = addr_info.get('device_name', '')

        rows.append({
            'project_id':             project_id,
            'project_name':           project_name,
            'subscription_id':        sub_id,
            'subscription_name':      sub_name,
            'delivery_mode':          mode,
            'delivery_filename':      filename,
            'email_subject':          subject,
            'schedule_ids':           sch_ids,
            'schedule_names':         sch_names,
            'content_ids':            cnt_ids,
            'content_names':          cnt_names,
            'content_types':          cnt_types,
            'owner_id':               owner_id,
            'owner_name':             owner_nm,
            'date_created':           date_created,
            'date_modified':          date_modified,
            'last_run':               last_run,
            'recipient_id':           rec_id,
            'recipient_name':         _v(rec, 'name'),
            'recipient_type':         _v(rec, 'type'),
            'recipient_include_type': _v(rec, 'includeType', 'include_type'),
            'address_id':             addr_id,
            'address_name':           addr_nm,
            'physical_address':       phys,
            'device_id':              device_id,
            'device_name':            device_name,
        })

    return rows


# ══════════════════════════════════════════════════════════════════════════════
#  COMPREHENSIVE JSON (full definition + prompt answers + resolved addresses)
# ══════════════════════════════════════════════════════════════════════════════

def _subscription_has_prompts(sub: dict) -> bool:
    """True if any content of the subscription has a prompt enabled."""
    for c in (sub.get('contents') or []):
        prompt = (c.get('personalization') or {}).get('prompt') or {}
        if prompt.get('enabled'):
            return True
    return False


def _enrich_recipient_addresses(sub: dict, addr_lookup: dict) -> None:
    """Add resolved physical_address / device_id / device_name onto each
    non-group recipient dict (in place)."""
    for rec in (sub.get('recipients') or []):
        if _is_group_recipient(rec):
            continue
        uid     = _v(rec, 'id')
        addr_id = _v(rec, 'addressId', 'address_id')
        info = (addr_lookup.get(uid) or {}).get(addr_id) or {}
        rec['physical_address'] = info.get('physical_address', '')
        rec['device_id']        = info.get('device_id', '')
        rec['device_name']      = info.get('device_name', '')


def _build_comprehensive(conn, project_subs, addr_lookup, concurrency) -> list[dict]:
    """Build one full nested object per subscription: the raw definition plus
    resolved recipient addresses and, for prompted subscriptions, the prompt
    definitions + saved answers from /subscriptions/{id}/prompts.

    The prompt `key` + `answers` captured here are what a recreation step needs
    to re-apply answers on the target environment.
    """
    # Which subscriptions are prompted (limit the extra REST calls)
    prompt_targets: list[tuple[str, str]] = []
    if get_subscription_prompts is not None:
        for pid, _pname, subs in project_subs:
            for sub in subs:
                if _subscription_has_prompts(sub):
                    sid = _v(sub, 'id')
                    if sid:
                        prompt_targets.append((pid, sid))

    prompts_map: dict[tuple[str, str], list] = {}
    if prompt_targets:
        logger.info('Fetching prompt answers for {n} prompted subscription(s)...',
                    n=len(prompt_targets))

        def _fetch(pid: str, sid: str):
            try:
                resp = get_subscription_prompts(conn, subscription_id=sid, project_id=pid)
                return (pid, sid), (resp.json().get('prompts') or [])
            except Exception as exc:
                logger.warning('Prompt fetch failed for {sid}: {e}', sid=sid, e=exc)
                return (pid, sid), []

        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(_fetch, pid, sid) for pid, sid in prompt_targets]
            for fut in as_completed(futures):
                key, prompts = fut.result()
                prompts_map[key] = prompts

    out: list[dict] = []
    for pid, pname, subs in project_subs:
        for sub in subs:
            sub = dict(sub)  # shallow copy; we add a few keys
            sub.setdefault('project_id', pid)
            sub.setdefault('project_name', pname)
            _enrich_recipient_addresses(sub, addr_lookup)
            prompts = prompts_map.get((pid, _v(sub, 'id')))
            if prompts is not None:
                sub['prompts'] = prompts
            out.append(sub)
    return out


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main(
    env: str,
    fmt: str,
    output_dir: Optional[Path],
    filter_project_ids: Optional[list[str]],
    concurrency: int,
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

    # ── PASS 1: collect all subscription dicts and recipient user IDs ─────────
    project_subs: list[tuple[str, str, list[dict]]] = []
    recipient_user_ids: set[str] = set()
    total_subs = 0

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
        project_subs.append((proj.id, proj.name, subs))
        total_subs += len(subs)

        for sub in subs:
            for rec in (sub.get('recipients') or []):
                uid    = _v(rec, 'id')
                addr_id = _v(rec, 'addressId', 'address_id')
                if uid and addr_id and not _is_group_recipient(rec):
                    recipient_user_ids.add(uid)

    # ── PASS 2: resolve physical addresses ────────────────────────────────────
    addr_lookup = _build_address_lookup(conn, recipient_user_ids, concurrency)

    # ── OUTPUT ────────────────────────────────────────────────────────────────
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_dir.mkdir(parents=True, exist_ok=True)

    if fmt == 'json':
        # Comprehensive: full nested definition + prompt answers + resolved addresses.
        comprehensive = _build_comprehensive(conn, project_subs, addr_lookup, concurrency)
        prompted = sum(1 for s in comprehensive if s.get('prompts'))
        logger.info(
            'Total: {s} subscription(s), {pr} with prompt answers ({p} project(s))',
            s=total_subs, pr=prompted, p=len(project_subs),
        )
        out_file = out_dir / JSON_FILENAME.format(env=env, ts=ts)
        out_file.write_text(
            json.dumps(comprehensive, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
        logger.success('Comprehensive JSON written → {p}  ({n} subscriptions, {pr} prompted)',
                       p=out_file, n=len(comprehensive), pr=prompted)
        print(f'\nExported: {out_file}  ({len(comprehensive)} subscriptions, {prompted} with prompt answers)')
    else:
        # Flat: one row per recipient.
        all_rows: list[dict] = []
        for project_id, project_name, subs in project_subs:
            for sub in subs:
                all_rows.extend(
                    _flatten_subscription(sub, project_id, project_name, addr_lookup))
        logger.info(
            'Total: {s} subscription(s) → {r} row(s) ({p} project(s))',
            s=total_subs, r=len(all_rows), p=len(project_subs),
        )
        out_file = out_dir / CSV_FILENAME.format(env=env, ts=ts)
        csv_rows = [[r.get(c, '') for c in COLUMNS] for r in all_rows]
        write_csv(csv_rows, columns=COLUMNS, path=out_file)
        logger.success('CSV written → {p}  ({n} rows)', p=out_file, n=len(all_rows))
        print(f'\nExported: {out_file}')

    return 0


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=(
            'Export all MicroStrategy subscriptions across every loaded project. '
            'One row per recipient, with physical delivery address resolved.'
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
    parser.add_argument(
        '--concurrency',
        type=int,
        default=DEFAULT_CONCURRENCY,
        metavar='N',
        help=f'Parallel workers for address resolution (default: {DEFAULT_CONCURRENCY}).',
    )

    args = parser.parse_args()

    raise SystemExit(
        main(
            env=args.env,
            fmt=args.fmt,
            output_dir=args.output_dir,
            filter_project_ids=args.filter_project_ids,
            concurrency=args.concurrency,
        )
    )
