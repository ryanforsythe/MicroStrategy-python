"""
SubscriptionsExport.py

Export all subscription information across every loaded project.

Iterates every loaded project, retrieves all subscriptions (including last_run
where the server supports it), and writes one row per recipient so the output
can be sorted or filtered by user, delivery mode, schedule, etc.

Output modes
────────────
  --format csv  (default) : flat, one row per recipient (see columns below).
  --format json           : COMPREHENSIVE — one object per subscription, built
                            to be edited and recreated on a target env (see
                            JSON structure below).

  --format excel          : same rows and columns as csv, as .xlsx. Use this to edit:
                            Excel reformats dates when it opens a CSV.

The csv/excel output is also the working sheet for SubscriptionsCreate.py: filter
with target_action, edit the visible columns. definition_json carries the full
definition on every row (see JSON structure), so one file is enough.

JSON structure
──────────────
The file is a list. Each entry is the unchanged GET /subscriptions/{id} response,
which is the POST /subscriptions body shape: name, allow*, owner, schedules, contents
(+ personalization: formatType, compressed, bursting, exportToPdfSettings,
delimiter, prompt; + refreshCondition), recipients, and delivery (mode, expiration,
expirationTimeZone, contactSecurity, notificationEnabled, personalNotification and
the mode's block, e.g. delivery.email.zip). Compression is in two places:
contents[].personalization.compressed and delivery.<mode>.zip.

Keys the export adds are snake_case, REST keys are camelCase, so dropping the
snake_case keys leaves the REST body:
  project_id / project_name
  last_run                    From the list call; GET does not return it.
  prompts[]                   Prompted subscriptions only: definition + saved
                              `answers`, from /subscriptions/{id}/prompts. `key` +
                              `answers` re-apply through answer_prompts().
  recipients[].login          Plus physical_address / device_id / device_name
                              (not on groups). GUIDs may differ between
                              environments; these match on the target.
  recipients[].dynamic_recipient_list
                              Definition of a dynamic list recipient.
  resolved.owner_login
  resolved.content_locations  {content id: folder path}, backslash-delimited
  resolved.personal_notification
                              Physical address of delivery.personalNotification.
  export_issues[]             Text notes on what a recreation must handle itself:
                              a zip password (never returned by the API), an
                              expiration date in the past, an address that did not
                              resolve, a failed fetch. Empty = nothing to handle.
Read-only REST keys a POST does not take: id, editable, dateCreated, dateModified,
recipients[].childSubscriptionId, schedules[].nextDelivery / expired.

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
  compressed               True if any content or the delivery is zipped.
  zip_filename             delivery.<mode>.zip filename.
  zip_password_protected   delivery.<mode>.zip passwordProtect.
  format_types             Semicolon-separated content format types (PDF/EXCEL/…).
  expiration               Delivery expiration date.
  prompted                 True if any content has prompts enabled.
  prompts_json             /subscriptions/{id}/prompts `prompts` array as JSON,
                           with each prompt's `answers`; blank if not prompted,
                           `ERROR: …` if the fetch failed.
  owner_login / recipient_login
                           Logins; these match users on a target environment.
  target_action            `Create`. Anything else makes SubscriptionsCreate.py
                           skip that subscription.
  definition_json          The JSON entry below, without prompts. Must fit an Excel
                           cell (32,767 characters); an entry that does not is
                           logged as an error.

A subscription with no recipients (e.g. bursting to an address attribute) gets one
row with the recipient columns blank.

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
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from loguru import logger
from mstrio.api import objects as objects_api
from mstrio.api import subscriptions as subscriptions_api
from mstrio.server import Environment
from mstrio.users_and_groups import User

from mstrio_core import MstrConfig, get_mstrio_connection, write_csv, write_excel
from mstrio_core.config import MstrEnvironment

# ── Workstation import (uncomment when running from Workstation) ──────────────
# from mstrio.connection import get_connection

try:
    from mstrio.distribution_services import list_subscriptions
except ImportError:
    list_subscriptions = None


# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

CSV_FILENAME  = 'subscriptions_export_{env}_{ts}.csv'
JSON_FILENAME = 'subscriptions_export_{env}_{ts}.json'
EXCEL_FILENAME = 'subscriptions_export_{env}_{ts}.xlsx'
EXCEL_CELL_LIMIT = 32767
DEFAULT_CONCURRENCY = 10

# Group recipient types — no individual address to resolve
_GROUP_TYPES = {'usergroup', 'user_group', 'contactgroup', 'contact_group', 'group'}

# Subscription content type → metadata object type (a cube is a report definition)
_CONTENT_OBJECT_TYPES = {'report': 3, 'cube': 3, 'document': 55, 'dossier': 55}

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
    'compressed',
    'zip_filename',
    'zip_password_protected',
    'format_types',
    'expiration',
    'prompted',
    'prompts_json',
    'owner_login',
    'recipient_login',
    'target_action',
    'definition_json',
]


# ══════════════════════════════════════════════════════════════════════════════
#  RAW-DICT HELPERS
#  list_subscriptions(to_dictionary=True) snake_cases dict keys but not those
#  inside lists (contents, recipients stay camelCase); GET /subscriptions/{id} is
#  camelCase throughout. Helpers below accept both.
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


def _mode_settings(delivery: dict) -> dict:
    """Return the block of the delivery mode (delivery.email, delivery.file, …)."""
    mode_key = (_v(delivery, 'mode') or '').lower()
    camel_key = mode_key.replace('_l', 'L')  # history_list → historyList
    return delivery.get(mode_key) or delivery.get(camel_key) or {}


def _personalization_values(sub: dict, key: str) -> list:
    """Return the non-empty values of a personalization key across contents."""
    values = (
        (c.get('personalization') or {}).get(key)
        for c in (sub.get('contents') or [])
    )
    return [v for v in values if v]


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
) -> tuple[dict[str, dict[str, dict[str, str]]], dict[str, str]]:
    """
    Fetch login and ContactAddresses for every user_id in parallel.

    Returns:
        ({user_id: {address_id: {"physical_address", "device_id", "device_name"}}},
         {user_id: login})
    """
    if not user_ids:
        return {}, {}

    logger.info(
        'Resolving addresses for {n} unique recipient user(s)...', n=len(user_ids),
    )

    lookup: dict[str, dict[str, dict[str, str]]] = {}
    logins: dict[str, str] = {}

    def _fetch(uid: str) -> tuple[str, str, dict[str, dict[str, str]]]:
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
            return uid, user.username, addrs
        except Exception as exc:
            logger.warning(
                'Could not fetch addresses for user {uid}: {err}', uid=uid, err=exc,
            )
            return uid, '', {}

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(_fetch, uid): uid for uid in user_ids}
        done = 0
        for future in as_completed(futures):
            uid, login, addrs = future.result()
            if addrs:
                lookup[uid] = addrs
            if login:
                logins[uid] = login
            done += 1
            if done % 50 == 0:
                logger.debug('  Address lookup: {done}/{total}', done=done, total=len(user_ids))

    logger.info('Address lookup complete ({n} user(s) resolved)', n=len(lookup))
    return lookup, logins


# ══════════════════════════════════════════════════════════════════════════════
#  FLATTENING
# ══════════════════════════════════════════════════════════════════════════════

def _flatten_subscription(
    sub: dict,
    project_id: str,
    project_name: str,
    addr_lookup: dict[str, dict[str, dict[str, str]]],
    prompts_json: str = '',
    definition_json: str = '',
) -> list[dict]:
    """
    Expand a raw subscription dict into one output row per recipient, or one
    row with blank recipient columns if it has none.
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
    zip_settings = _mode_settings(delivery).get('zip') or {}
    compressed   = bool(zip_settings or _personalization_values(sub, 'compressed'))

    date_created  = _fmt_dt(_v(sub, 'dateCreated',  'date_created'))
    date_modified = _fmt_dt(_v(sub, 'dateModified', 'date_modified'))
    last_run      = _fmt_dt(_v(sub, 'lastRun',      'last_run'))

    rows = []
    for rec in (sub.get('recipients') or [{}]):
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
            'compressed':             compressed,
            'zip_filename':           _v(zip_settings, 'filename'),
            'zip_password_protected': _v(zip_settings, 'passwordProtect', 'password_protect'),
            'format_types':           '; '.join(_personalization_values(sub, 'formatType')),
            'expiration':             _v(delivery, 'expiration'),
            'prompted':               _subscription_has_prompts(sub),
            'prompts_json':           prompts_json,
            'owner_login':            _v(sub.get('resolved') or {}, 'owner_login'),
            'recipient_login':        _v(rec, 'login'),
            'target_action':          'Create',
            'definition_json':        definition_json,
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


def _enrich_recipients(sub: dict, addr_lookup: dict, logins: dict) -> None:
    """Add login + resolved physical_address / device_id / device_name onto each
    non-group recipient dict (in place)."""
    for rec in (sub.get('recipients') or []):
        if _is_group_recipient(rec):
            continue
        uid     = _v(rec, 'id')
        addr_id = _v(rec, 'addressId', 'address_id')
        info = (addr_lookup.get(uid) or {}).get(addr_id) or {}
        if addr_id and not info:
            sub['export_issues'].append(f'address {addr_id} of recipient {uid} not found')
        rec['login']            = logins.get(uid, '')
        rec['physical_address'] = info.get('physical_address', '')
        rec['device_id']        = info.get('device_id', '')
        rec['device_name']      = info.get('device_name', '')


class _ThreadConnections:
    """One logged-in Connection (= one I-Server session) per worker thread.

    Reading prompts creates messages in the session's working set: parallel reads
    on a shared session evict each other, and they fail intermittently with ERR004
    "Message not found in user history list".
    """

    def __init__(self, config: MstrConfig):
        self._config = config
        self._local = threading.local()
        self._lock = threading.Lock()
        self._all: list = []

    def get(self):
        conn = getattr(self._local, 'conn', None)
        if conn is None:
            conn = get_mstrio_connection(config=self._config)
            self._local.conn = conn
            with self._lock:
                self._all.append(conn)
        return conn

    def close_all(self) -> None:
        for conn in self._all:
            try:
                conn.close()
            except Exception as exc:
                logger.warning('Worker logout failed: {e}', e=exc)
        self._all.clear()


def _fetch_prompts(conn, pid: str, sub_id: str) -> tuple[list, str]:
    """Return (prompts with saved answers, error text). A failure must be reported
    by the caller, so a lost answer does not read as an unanswered prompt."""
    try:
        resp = subscriptions_api.get_subscription_prompts(
            conn, subscription_id=sub_id, project_id=pid)
        return resp.json().get('prompts') or [], ''
    except Exception as exc:
        logger.warning('Prompt fetch failed for {sid}: {e}', sid=sub_id, e=exc)
        return [], str(exc)


def _sheet_cells(sub: dict) -> tuple[str, str]:
    """(prompts_json, definition_json) cells for one subscription."""
    failed = [i for i in sub['export_issues'] if i.startswith('prompt fetch failed')]
    if failed:
        prompts_json = f'ERROR: {failed[0]}'
    elif 'prompts' in sub:
        prompts_json = json.dumps(sub['prompts'], ensure_ascii=False)
    else:
        prompts_json = ''
    definition = {k: v for k, v in sub.items() if k != 'prompts'}
    definition_json = json.dumps(definition, ensure_ascii=False, separators=(',', ':'))
    for label, cell in (('definition_json', definition_json), ('prompts_json', prompts_json)):
        if len(cell) > EXCEL_CELL_LIMIT:
            logger.error('{col} of {name} ({sid}) is {n} characters, over the Excel cell limit',
                         col=label, name=_v(sub, 'name'), sid=_v(sub, 'id'), n=len(cell))
    return prompts_json, definition_json


def _fetch_definition(conn, pid: str, listed: dict) -> dict:
    """Fetch one subscription's full definition: the GET /subscriptions/{id} body,
    prompts with saved answers, and any dynamic recipient list definitions.
    A failed fetch keeps what the list call returned and adds to export_issues."""
    sub_id = _v(listed, 'id')
    issues: list[str] = []
    try:
        sub = subscriptions_api.get_subscription(conn, sub_id, pid).json()
    except Exception as exc:
        logger.warning('Definition fetch failed for {sid}: {e}', sid=sub_id, e=exc)
        issues.append(f'definition fetch failed, list data used: {exc}')
        sub = dict(listed)

    if _subscription_has_prompts(sub):
        sub['prompts'], error = _fetch_prompts(conn, pid, sub_id)
        if error:
            issues.append(f'prompt fetch failed: {error}')

    for rec in (sub.get('recipients') or []):
        if rec.get('type') != 'dynamic_recipient_list':
            continue
        try:
            rec['dynamic_recipient_list'] = subscriptions_api.get_dynamic_recipient_list(
                conn, rec['id'], pid).json()
        except Exception as exc:
            logger.warning('Dynamic recipient list fetch failed for {rid}: {e}',
                           rid=rec['id'], e=exc)
            issues.append(f"dynamic recipient list {rec['id']} fetch failed: {exc}")

    delivery = sub.get('delivery') or {}
    if (_mode_settings(delivery).get('zip') or {}).get('passwordProtect'):
        issues.append('zip password protected: the API does not return the password')
    expiration = delivery.get('expiration')
    if expiration and date.fromisoformat(expiration[:10]) < date.today():
        issues.append(f'expiration {expiration} is in the past')

    last_run = _v(listed, 'lastRun', 'last_run')
    if last_run:
        sub['last_run'] = _fmt_dt(last_run)
    sub['export_issues'] = issues
    return sub


def _fetch_content_location(conn, pid: str, content_id: str, content_type: str) -> str:
    """Return a content's folder path, or blank if it cannot be read."""
    object_type = _CONTENT_OBJECT_TYPES.get(content_type)
    if object_type is None:
        logger.warning('Unknown content type {t} for {cid}', t=content_type, cid=content_id)
        return ''
    try:
        resp = objects_api.get_object_info(conn, content_id, object_type, project_id=pid)
        if resp.ok:
            # Backslashes: a folder name can contain "/" (dev has "Central/Farnam").
            # ancestors[0] is the project root.
            ancestors = resp.json().get('ancestors') or []
            return '\\'.join(a.get('name', '') for a in ancestors[1:])
        logger.warning('Location fetch failed for {cid}: HTTP {status}',
                       cid=content_id, status=resp.status_code)
    except Exception as exc:
        logger.warning('Location fetch failed for {cid}: {e}', cid=content_id, e=exc)
    return ''


def _build_comprehensive(config, project_subs, addr_lookup, logins, concurrency) -> list[dict]:
    """Build one recreation-ready object per subscription. See "JSON structure"
    in the module docstring: REST body unchanged, additions in snake_case keys."""
    listed = [(pid, pname, sub) for pid, pname, subs in project_subs for sub in subs]
    logger.info('Fetching full definitions for {n} subscription(s)...', n=len(listed))

    workers = _ThreadConnections(config)
    try:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            definitions = list(pool.map(
                lambda item: _fetch_definition(workers.get(), item[0], item[2]), listed))

            content_keys = {
                (pid, c['id'], c.get('type'))
                for (pid, _pname, _sub), definition in zip(listed, definitions)
                for c in (definition.get('contents') or [])
            }
            logger.info('Resolving folders for {n} distinct content object(s)...',
                        n=len(content_keys))
            locations = dict(zip(content_keys, pool.map(
                lambda key: _fetch_content_location(workers.get(), *key), content_keys)))
    finally:
        workers.close_all()

    addresses = {
        addr_id: info
        for user_addrs in addr_lookup.values()
        for addr_id, info in user_addrs.items()
    }

    out: list[dict] = []
    for (pid, pname, _sub), sub in zip(listed, definitions):
        sub['project_id'] = pid
        sub['project_name'] = pname
        _enrich_recipients(sub, addr_lookup, logins)
        notification = (sub.get('delivery') or {}).get('personalNotification') or {}
        sub['resolved'] = {
            'owner_login': logins.get(_v(sub.get('owner') or {}, 'id'), ''),
            'content_locations': {
                c['id']: locations.get((pid, c['id'], c.get('type')), '')
                for c in (sub.get('contents') or [])
            },
            'personal_notification': addresses.get(notification.get('addressId'), {}),
        }
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
            # Owner too: login + personal notification address
            recipient_user_ids.add(_v(sub.get('owner') or {}, 'id'))
            for rec in (sub.get('recipients') or []):
                uid = _v(rec, 'id')
                if uid and not _is_group_recipient(rec):
                    recipient_user_ids.add(uid)

    # ── PASS 2: resolve physical addresses ────────────────────────────────────
    recipient_user_ids.discard('')
    addr_lookup, logins = _build_address_lookup(conn, recipient_user_ids, concurrency)

    # ── OUTPUT ────────────────────────────────────────────────────────────────
    ts = datetime.now().strftime('%Y%m%d_%H%M')
    out_dir.mkdir(parents=True, exist_ok=True)

    # Full definitions, prompt answers and resolved addresses, for every format.
    comprehensive = _build_comprehensive(config, project_subs, addr_lookup, logins, concurrency)
    prompted = sum(1 for s in comprehensive if s.get('prompts'))
    with_issues = sum(1 for s in comprehensive if s['export_issues'])

    if fmt == 'json':
        logger.info(
            'Total: {s} subscription(s), {pr} with prompts, {i} with export_issues '
            '({p} project(s))',
            s=total_subs, pr=prompted, i=with_issues, p=len(project_subs),
        )
        out_file = out_dir / JSON_FILENAME.format(env=env, ts=ts)
        out_file.write_text(
            json.dumps(comprehensive, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
        logger.success('Comprehensive JSON written → {p}  ({n} subscriptions, {pr} prompted)',
                       p=out_file, n=len(comprehensive), pr=prompted)
        print(f'\nExported: {out_file}  ({len(comprehensive)} subscriptions, {prompted} with prompts, {with_issues} with export_issues)')
    else:
        # One row per recipient.
        all_rows: list[dict] = []
        for sub in comprehensive:
            all_rows.extend(_flatten_subscription(
                sub, sub['project_id'], sub['project_name'], addr_lookup, *_sheet_cells(sub)))
        logger.info(
            'Total: {s} subscription(s) → {r} row(s), {pr} with prompts, {i} with '
            'export_issues ({p} project(s))',
            s=total_subs, r=len(all_rows), pr=prompted, i=with_issues, p=len(project_subs),
        )
        sheet_rows = [[r.get(c, '') for c in COLUMNS] for r in all_rows]
        if fmt == 'excel':
            out_file = out_dir / EXCEL_FILENAME.format(env=env, ts=ts)
            write_excel(sheet_rows, out_file, columns=COLUMNS, sheet_name='Subscriptions')
        else:
            out_file = out_dir / CSV_FILENAME.format(env=env, ts=ts)
            write_csv(sheet_rows, columns=COLUMNS, path=out_file)
        logger.success('Sheet written → {p}  ({n} rows)', p=out_file, n=len(all_rows))
        print(f'\nExported: {out_file}')

    return 0


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=(
            'Export all MicroStrategy subscriptions across every loaded project. '
            'CSV/Excel: one row per recipient, the working sheet for SubscriptionsCreate.py. '
            'JSON: one full definition per subscription.'
        )
    )
    parser.add_argument(
        'env',
        choices=[e.value for e in MstrEnvironment],
        help='MicroStrategy environment (dev / qa / prod).',
    )
    parser.add_argument(
        '--format', '-f',
        choices=['csv', 'excel', 'json'],
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
        help=f'Parallel workers for API lookups (default: {DEFAULT_CONCURRENCY}).',
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
