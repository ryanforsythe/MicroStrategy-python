"""
SubscriptionFileMigration.py

One-time orchestrator for migrating MicroStrategy file-subscription delivery
from on-prem UNC paths to the MicroStrategy Cloud (MCE) local directory
(/opt/mstr/ContainerState/FileSubscriptions), which GoAnywhere then syncs to
the original on-prem destinations.

Driven by a workbook (default: subscription_file_path_mapping.xlsx) with sheets:
  - DistinctSubscriptionLocations : mapping decisions (the driver)
        Device, Distinct Subscription Locations, GoAnywhereSyncBase,
        DeviceBase, SubscriptionsUserAppend, NewDevice
  - subscriptions_export_dev      : SubscriptionsExport.py output (source/dev)
  - Devices                       : FileSubscriptionLocations.py device export
  - UserAddresses                 : FileSubscriptionLocations.py address export

Confirmed design decisions
──────────────────────────
  * Writes hit a TARGET environment (CLI arg).  The "used" set for cleanup comes
    from the workbook's subscriptions_export_dev sheet (not live target data).
  * Device path  =  /opt/mstr/ContainerState/FileSubscriptions + DeviceBase.
    GoAnywhereSyncBase is ONLY the GoAnywhere grouping key, not in the path.
  * For append_user_path devices, the user address physical_address is set to
    SubscriptionsUserAppend.
  * New devices clone all FileDeviceProperties from the device named in the
    row's 'Device' column, then override file_path.
  * Administrator subscriptions are recreated under user subscription_admin
    (BFF662B4C6451F461A754CAEC51B1036), keeping each sub's original recipients.

Steps (run individually via --steps or 'all')
  update-devices  create-devices  update-addresses
  clean-addresses clean-devices   admin-subs       json-map

Usage
─────
  # Full dry run (no writes) — produces the review workbook
  python SubscriptionFileMigration.py dev

  # Execute only the device updates + new devices
  python SubscriptionFileMigration.py dev --steps update-devices,create-devices --apply

  # Full apply, with source env for faithful Administrator-sub recreation
  python SubscriptionFileMigration.py prod --steps all --source-env dev --apply
"""

import argparse
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from loguru import logger
from mstrio.distribution_services import (
    CacheUpdateSubscription,
    EmailSubscription,
    FileSubscription,
    FTPSubscription,
    HistoryListSubscription,
    Subscription,
    list_subscriptions,
)
from mstrio.distribution_services.device import Device, list_devices
from mstrio.distribution_services.device.device_properties import FileDeviceProperties
from mstrio.server import Environment
from mstrio.users_and_groups import User, list_users

from mstrio_core import MstrConfig, get_mstrio_connection
from mstrio_core.config import MstrEnvironment

# ── Workstation import (uncomment when running from Workstation) ──────────────
# from mstrio.connection import get_connection

# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

LINUX_BASE = '/opt/mstr/ContainerState/FileSubscriptions'

SUBSCRIPTION_ADMIN_ID    = 'BFF662B4C6451F461A754CAEC51B1036'
SUBSCRIPTION_ADMIN_LOGIN = 'subscription_admin'

DEFAULT_WORKBOOK   = 'subscription_file_path_mapping.xlsx'
DEFAULT_CONCURRENCY = 10

SHEET_MAP    = 'DistinctSubscriptionLocations'
SHEET_SUBS   = 'subscriptions_export_dev'
SHEET_DEVS   = 'Devices'
SHEET_ADDR   = 'UserAddresses'

# Step keys
STEP_VALIDATE        = 'validate'
STEP_UPDATE_DEVICES  = 'update-devices'
STEP_CREATE_DEVICES  = 'create-devices'
STEP_UPDATE_ADDRESSES = 'update-addresses'
STEP_CLEAN_ADDRESSES = 'clean-addresses'
STEP_CLEAN_DEVICES   = 'clean-devices'
STEP_ADMIN_SUBS      = 'admin-subs'
STEP_JSON_MAP        = 'json-map'

ALL_STEPS = [
    STEP_VALIDATE,
    STEP_UPDATE_DEVICES,
    STEP_CREATE_DEVICES,
    STEP_UPDATE_ADDRESSES,
    STEP_CLEAN_ADDRESSES,
    STEP_CLEAN_DEVICES,
    STEP_ADMIN_SUBS,
    STEP_JSON_MAP,
]

JSON_MAP_FILENAME = 'device_goanywhere_map_{env}.json'

# Delivery-mode → creation handler (admin-subs); others reported as unsupported
SUPPORTED_RECREATE_MODES = {'EMAIL', 'FILE', 'FTP', 'HISTORY_LIST', 'CACHE'}


# ══════════════════════════════════════════════════════════════════════════════
#  WORKBOOK LOADING
# ══════════════════════════════════════════════════════════════════════════════

def _load_workbook(path: Path) -> dict[str, list[dict]]:
    """Read every sheet to a list of row dicts (all strings, NaN → '')."""
    frames = pd.read_excel(path, sheet_name=None, dtype=str)
    out: dict[str, list[dict]] = {}
    for name, df in frames.items():
        df = df.fillna('')
        out[name] = [
            {str(k).strip(): ('' if v is None else str(v)) for k, v in row.items()}
            for row in df.to_dict(orient='records')
        ]
    return out


def _norm_path(p: str) -> str:
    """Normalise a UNC/Windows path for matching: unify slashes, strip, lower,
    drop trailing separators."""
    if not p:
        return ''
    s = p.strip().replace('\\', '/').lower()
    s = re.sub(r'/+', '/', s)
    return s.rstrip('/')


def _linux_path(device_base: str) -> str:
    """Compose the device Linux path: LINUX_BASE + DeviceBase."""
    db = (device_base or '').strip()
    if not db:
        return LINUX_BASE
    if not db.startswith('/'):
        db = '/' + db
    return LINUX_BASE + db


# ══════════════════════════════════════════════════════════════════════════════
#  TARGET-OBJECT SNAPSHOTS
# ══════════════════════════════════════════════════════════════════════════════

def _device_snapshot(conn) -> tuple[dict[str, dict], dict[str, str]]:
    """Return ({device_id: device_dict}, {device_name_lower: device_id}).
    Uses to_dictionary=True to avoid DeviceType enum errors (e.g. 'gcs')."""
    devs = list_devices(conn, to_dictionary=True)
    by_id, by_name = {}, {}
    for d in devs:
        did, name = d.get('id'), d.get('name') or ''
        if did:
            by_id[did] = d
            if name and name.lower() not in by_name:
                by_name[name.lower()] = did
    return by_id, by_name


def _resolve_device_id(name: str, snap_by_name: dict[str, str]) -> Optional[str]:
    return snap_by_name.get((name or '').lower())


# ══════════════════════════════════════════════════════════════════════════════
#  STEP: validate (read-only coverage / pre-flight check)
# ══════════════════════════════════════════════════════════════════════════════

def step_validate(data) -> list[dict]:
    """Read-only pre-flight. For every active FILE subscription in the export,
    check that its (device_name, normalized path) is covered by the mapping
    sheet. Uncovered file subs would NOT be re-pointed and would deliver to a
    wrong/changed path once their device's file_path is rewritten.

    Also flags rows with a blank physical_address (no path to match on)."""
    # Mapping coverage keys
    mapped_keys = set()
    for r in data[SHEET_MAP]:
        dev = (r.get('Device', '') or '').strip().lower()
        loc = _norm_path(r.get('Distinct Subscription Locations', ''))
        mapped_keys.add((dev, loc))

    results = []
    covered = uncovered = blank = 0
    for r in data[SHEET_SUBS]:
        if (r.get('delivery_mode', '') or '').upper() != 'FILE':
            continue
        dev_name = (r.get('device_name', '') or '').strip()
        phys     = (r.get('physical_address', '') or '').strip()
        key = (dev_name.lower(), _norm_path(phys))

        if not phys:
            status, details = 'blank-path', 'FILE recipient has no physical_address to match'
            blank += 1
        elif key in mapped_keys:
            covered += 1
            continue  # covered rows omitted — only surface problems
        else:
            status, details = 'UNCOVERED', 'not in DistinctSubscriptionLocations — will not be re-pointed'
            uncovered += 1

        results.append({
            'subscription_name': r.get('subscription_name', ''),
            'project_name': r.get('project_name', ''),
            'device_name': dev_name,
            'physical_address': phys,
            'recipient_name': r.get('recipient_name', ''),
            'address_id': r.get('address_id', ''),
            'status': status, 'details': details,
        })

    logger.info('Coverage: {c} covered, {u} UNCOVERED, {b} blank-path FILE recipients',
                c=covered, u=uncovered, b=blank)
    if not results:
        results.append({'status': 'ok',
                        'details': f'all {covered} FILE recipients covered by mapping'})
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  STEP: update-devices
# ══════════════════════════════════════════════════════════════════════════════

def step_update_devices(conn, data, snap_by_name, apply: bool) -> list[dict]:
    """Set file_path = LINUX_BASE + DeviceBase on each existing mapped device
    (rows where NewDevice is blank)."""
    rows = data[SHEET_MAP]

    # Group by device name → DeviceBase (existing devices only: NewDevice blank)
    device_base: dict[str, set] = {}
    for r in rows:
        if r.get('NewDevice', '').strip():
            continue  # handled by create-devices
        name = r.get('Device', '').strip()
        if not name:
            continue
        device_base.setdefault(name, set()).add(r.get('DeviceBase', '').strip())

    results = []
    for name, bases in sorted(device_base.items()):
        new_path = _linux_path(sorted(bases)[0])
        note = ''
        if len(bases) > 1:
            note = f'WARNING: multiple DeviceBase values {sorted(bases)} — used first'

        device_id = _resolve_device_id(name, snap_by_name)
        if not device_id:
            results.append({
                'device_name': name, 'device_id': '', 'new_file_path': new_path,
                'status': 'error', 'details': 'device not found on target', 'note': note,
            })
            continue

        old_path = ''
        status, details = ('dry-run', 'would update file_path')
        if apply:
            try:
                dev = Device(conn, id=device_id)
                dp = dev.device_properties
                old_path = getattr(getattr(dp, 'file_location', None), 'file_path', '') or ''
                if not isinstance(dp, FileDeviceProperties) or dp.file_location is None:
                    raise ValueError('device has no FileLocation (not a FILE device?)')
                dp.file_location.file_path = new_path
                dev.alter(device_properties=dp)
                status, details = 'updated', f'{old_path!r} → {new_path!r}'
            except Exception as exc:
                status, details = 'error', str(exc)

        results.append({
            'device_name': name, 'device_id': device_id, 'old_file_path': old_path,
            'new_file_path': new_path, 'status': status, 'details': details, 'note': note,
        })
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  STEP: create-devices
# ══════════════════════════════════════════════════════════════════════════════

def step_create_devices(conn, data, snap_by_name, apply: bool) -> list[dict]:
    """Create each NewDevice, cloning FileDeviceProperties from the source
    device in the 'Device' column, overriding file_path = LINUX_BASE + DeviceBase."""
    rows = data[SHEET_MAP]

    # Group by NewDevice → (source device names, DeviceBases, SyncBases)
    plan: dict[str, dict] = {}
    for r in rows:
        new_name = r.get('NewDevice', '').strip()
        if not new_name:
            continue
        p = plan.setdefault(new_name, {'sources': set(), 'bases': set(), 'sync': set()})
        p['sources'].add(r.get('Device', '').strip())
        p['bases'].add(r.get('DeviceBase', '').strip())
        p['sync'].add(r.get('GoAnywhereSyncBase', '').strip())

    results = []
    for new_name, p in sorted(plan.items()):
        bases   = sorted(p['bases'])
        sources = sorted(s for s in p['sources'] if s)
        new_path = _linux_path(bases[0])
        note = ''
        if len(bases) > 1:
            note = f'WARNING: multiple DeviceBase {bases} — used first'
        if len(sources) != 1:
            note = (note + ' | ' if note else '') + f'clone source ambiguous {sources} — used first'

        # Already exists?
        existing_id = _resolve_device_id(new_name, snap_by_name)
        if existing_id:
            results.append({
                'new_device_name': new_name, 'device_id': existing_id,
                'clone_source': sources[0] if sources else '', 'new_file_path': new_path,
                'status': 'skipped', 'details': 'device already exists on target', 'note': note,
            })
            continue

        src_name = sources[0] if sources else ''
        src_id = _resolve_device_id(src_name, snap_by_name)
        if not src_id:
            results.append({
                'new_device_name': new_name, 'device_id': '', 'clone_source': src_name,
                'new_file_path': new_path, 'status': 'error',
                'details': f'clone source device not found: {src_name!r}', 'note': note,
            })
            continue

        status, details, new_id = 'dry-run', f'would create cloning {src_name!r}', ''
        if apply:
            try:
                src = Device(conn, id=src_id)
                dp = FileDeviceProperties.from_dict(src.device_properties.to_dict(), conn)
                if dp.file_location is None:
                    raise ValueError(f'clone source {src_name!r} has no FileLocation')
                dp.file_location.file_path = new_path
                created = Device.create(
                    connection=conn,
                    name=new_name,
                    device_type=src.device_type,
                    transmitter=src.transmitter,
                    device_properties=dp,
                    description=f'Created by SubscriptionFileMigration (clone of {src_name})',
                )
                new_id = created.id
                snap_by_name[new_name.lower()] = new_id  # so later steps resolve it
                status, details = 'created', f'cloned {src_name!r} → file_path {new_path!r}'
            except Exception as exc:
                status, details = 'error', str(exc)

        results.append({
            'new_device_name': new_name, 'device_id': new_id, 'clone_source': src_name,
            'new_file_path': new_path, 'append_from_source': 'cloned',
            'status': status, 'details': details, 'note': note,
        })
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  STEP: update-addresses
# ══════════════════════════════════════════════════════════════════════════════

def _build_addr_match_index(data) -> dict[tuple, list[dict]]:
    """From subscriptions_export FILE rows, map
    (device_name_lower, normalized_physical_address) → [{user_id, address_id, ...}]."""
    idx: dict[tuple, list[dict]] = {}
    for r in data[SHEET_SUBS]:
        if (r.get('delivery_mode', '') or '').upper() != 'FILE':
            continue
        dev_name = (r.get('device_name', '') or '').strip().lower()
        phys     = _norm_path(r.get('physical_address', ''))
        addr_id  = r.get('address_id', '').strip()
        user_id  = r.get('recipient_id', '').strip()
        if not addr_id or not user_id:
            continue
        key = (dev_name, phys)
        idx.setdefault(key, []).append({
            'user_id': user_id, 'address_id': addr_id,
            'address_name': r.get('address_name', ''),
            'recipient_name': r.get('recipient_name', ''),
            'subscription_name': r.get('subscription_name', ''),
        })
    return idx


def step_update_addresses(conn, data, snap_by_id, snap_by_name, apply: bool) -> list[dict]:
    """Re-point each subscription address to its target device and set
    physical_address = SubscriptionsUserAppend. The subscription follows
    automatically because it references the address_id."""
    match_idx = _build_addr_match_index(data)

    results = []
    seen_addr: set[str] = set()  # avoid double-updating an address shared by many subs

    for r in data[SHEET_MAP]:
        src_device = r.get('Device', '').strip()
        location   = r.get('Distinct Subscription Locations', '').strip()
        new_device = r.get('NewDevice', '').strip()
        append     = r.get('SubscriptionsUserAppend', '').strip()

        target_device_name = new_device or src_device
        target_id = _resolve_device_id(target_device_name, snap_by_name)

        # Does the target device append the user path?
        tgt_dict = snap_by_id.get(target_id, {}) if target_id else {}
        appends_user = _device_appends_user(tgt_dict)
        new_phys = append if appends_user else ''

        key = (src_device.lower(), _norm_path(location))
        matches = match_idx.get(key, [])

        if not matches:
            results.append({
                'src_device': src_device, 'location': location,
                'target_device': target_device_name, 'new_physical_address': new_phys,
                'user_id': '', 'address_id': '', 'subscription_name': '',
                'status': 'not-found',
                'details': 'no subscription address matched (device_name + path)',
            })
            continue

        if not target_id:
            for m in matches:
                results.append({
                    'src_device': src_device, 'location': location,
                    'target_device': target_device_name, 'new_physical_address': new_phys,
                    'user_id': m['user_id'], 'address_id': m['address_id'],
                    'subscription_name': m['subscription_name'],
                    'status': 'error', 'details': 'target device not found on target env',
                })
            continue

        for m in matches:
            addr_id = m['address_id']
            if addr_id in seen_addr:
                continue
            seen_addr.add(addr_id)

            status, details = 'dry-run', (
                f'would set device={target_device_name!r} '
                f'physical_address={new_phys!r}'
            )
            if apply:
                try:
                    user = User(conn, id=m['user_id'])
                    user.update_address(id=addr_id, address=new_phys, device_id=target_id)
                    status, details = 'updated', f'device→{target_device_name!r}, addr={new_phys!r}'
                except Exception as exc:
                    status, details = 'error', str(exc)

            results.append({
                'src_device': src_device, 'location': location,
                'target_device': target_device_name, 'target_device_id': target_id,
                'new_physical_address': new_phys, 'user_id': m['user_id'],
                'address_id': addr_id, 'recipient_name': m['recipient_name'],
                'subscription_name': m['subscription_name'],
                'status': status, 'details': details,
            })
    return results


def _device_appends_user(device_dict: dict) -> bool:
    """Read append_user_path out of a to_dictionary device dict's nested
    deviceProperties → file → fileLocation."""
    props = device_dict.get('deviceProperties') or device_dict.get('device_properties') or {}
    file_props = props.get('file') or props.get('FILE') or {}
    loc = file_props.get('fileLocation') or file_props.get('file_location') or {}
    val = loc.get('appendUserPath')
    if val is None:
        val = loc.get('append_user_path')
    return bool(val)


# ══════════════════════════════════════════════════════════════════════════════
#  STEP: clean-addresses
# ══════════════════════════════════════════════════════════════════════════════

def step_clean_addresses(conn, data, concurrency: int, apply: bool) -> list[dict]:
    """Remove FILE-delivery user addresses whose address_id is NOT referenced by
    any subscription in the workbook (used-set). Scoped to FILE addresses so
    email/FTP/etc. addresses are never touched."""
    used_addr = {r.get('address_id', '').strip()
                 for r in data[SHEET_SUBS] if r.get('address_id', '').strip()}
    logger.info('Workbook references {n} used address_id(s)', n=len(used_addr))

    users = list_users(conn)
    logger.info('Scanning FILE addresses across {n} user(s)...', n=len(users))

    def _scan(u):
        try:
            full = User(conn, id=u.id)
            out = []
            for a in (full.addresses or []):
                dt = getattr(getattr(a, 'delivery_type', None), 'value', None)
                if dt != 'file':
                    continue  # FILE-only scope
                out.append({
                    'address_id': a.id, 'address_name': a.name,
                    'physical_address': a.physical_address or '',
                    'device_name': a.device.name if a.device else '',
                })
            return u.id, u.name, out
        except Exception as exc:
            logger.warning('Could not read addresses for {uid}: {e}', uid=u.id, e=exc)
            return u.id, u.name, []

    user_addrs = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for fut in as_completed([pool.submit(_scan, u) for u in users]):
            user_addrs.append(fut.result())

    results = []
    for uid, uname, addrs in user_addrs:
        for a in addrs:
            if a['address_id'] in used_addr:
                continue  # keep
            status, details = 'dry-run', 'would remove (no active subscription)'
            if apply:
                try:
                    User(conn, id=uid).remove_address(id=a['address_id'])
                    status, details = 'removed', 'removed (no active subscription)'
                except Exception as exc:
                    status, details = 'error', str(exc)
            results.append({
                'user_id': uid, 'user_name': uname,
                'address_id': a['address_id'], 'address_name': a['address_name'],
                'device_name': a['device_name'], 'physical_address': a['physical_address'],
                'status': status, 'details': details,
            })
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  STEP: clean-devices
# ══════════════════════════════════════════════════════════════════════════════

def _is_file_device(device_dict: dict) -> bool:
    """True if a to_dictionary device dict is a FILE device."""
    dt = device_dict.get('deviceType') or device_dict.get('device_type') or ''
    return str(dt).lower() == 'file'


def step_clean_devices(conn, data, snap_by_id, apply: bool) -> list[dict]:
    """Remove FILE devices whose device_id is NOT in the workbook used-set,
    protecting every device that the mapping touches (existing targets,
    new device names, clone sources). Scoped to FILE devices so
    email/FTP/printer/etc. devices are never touched."""
    used_dev = {r.get('device_id', '').strip()
                for r in data[SHEET_SUBS] if r.get('device_id', '').strip()}

    protected_names = set()
    for r in data[SHEET_MAP]:
        for col in ('Device', 'NewDevice'):
            v = r.get(col, '').strip()
            if v:
                protected_names.add(v.lower())

    logger.info('Used device_ids: {u} | protected names: {p}',
                u=len(used_dev), p=len(protected_names))

    results = []
    for did, d in snap_by_id.items():
        name = d.get('name') or ''
        if not _is_file_device(d):
            continue  # FILE-only scope
        if did in used_dev:
            continue
        if name.lower() in protected_names:
            results.append({
                'device_id': did, 'device_name': name, 'status': 'kept',
                'details': 'protected (mapping target/clone source)',
            })
            continue
        status, details = 'dry-run', 'would remove (no active subscription)'
        if apply:
            try:
                Device(conn, id=did).delete(force=True)
                status, details = 'removed', 'removed (no active subscription)'
            except Exception as exc:
                status, details = 'error', str(exc)
        results.append({
            'device_id': did, 'device_name': name,
            'status': status, 'details': details,
        })
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  STEP: admin-subs
# ══════════════════════════════════════════════════════════════════════════════

def _content_filename(delivery, kind_attr):
    sub = getattr(delivery, kind_attr, None)
    return getattr(sub, 'filename', None) if sub else None


def step_admin_subs(conn, src_conn, data, apply: bool) -> list[dict]:
    """Validate which Administrator subscriptions from the workbook are missing
    on the target, and (with a source connection) recreate them under
    subscription_admin keeping the original recipients."""
    # Distinct Administrator subscriptions from the workbook
    admin_subs: dict[str, dict] = {}
    for r in data[SHEET_SUBS]:
        if (r.get('owner_name', '') or '').strip().lower() != 'administrator':
            continue
        sid = r.get('subscription_id', '').strip()
        if sid and sid not in admin_subs:
            admin_subs[sid] = {
                'subscription_id': sid,
                'subscription_name': r.get('subscription_name', '').strip(),
                'project_id': r.get('project_id', '').strip(),
                'project_name': r.get('project_name', '').strip(),
                'delivery_mode': (r.get('delivery_mode', '') or '').strip().upper(),
            }
    logger.info('{n} distinct Administrator subscription(s) in workbook', n=len(admin_subs))

    # Target existing subscription names per project
    target_names: dict[str, set] = {}
    for proj in Environment(conn).list_projects():
        try:
            subs = list_subscriptions(conn, project_id=proj.id, to_dictionary=True)
            target_names[proj.id] = {(s.get('name') or '').strip().lower() for s in subs}
        except Exception as exc:
            logger.warning('Target list_subscriptions failed for {p}: {e}', p=proj.name, e=exc)
            target_names[proj.id] = set()

    results = []
    for sid, info in admin_subs.items():
        pid, name, mode = info['project_id'], info['subscription_name'], info['delivery_mode']
        exists = name.lower() in target_names.get(pid, set())

        if exists:
            results.append({**info, 'status': 'present', 'details': 'already on target'})
            continue

        if mode not in SUPPORTED_RECREATE_MODES:
            results.append({**info, 'status': 'unsupported',
                            'details': f'delivery mode {mode} not auto-recreated'})
            continue

        if not apply or src_conn is None:
            detail = ('would create under subscription_admin'
                      if src_conn is not None
                      else 'MISSING — supply --source-env to recreate (needs source definition)')
            results.append({**info, 'status': 'dry-run' if src_conn is not None else 'missing',
                            'details': detail})
            continue

        # Apply: read source definition and recreate
        try:
            src = Subscription(src_conn, id=sid, project_id=pid)
            recipients = [{'id': rec['id']} for rec in (src.recipients or []) if rec.get('id')]
            schedules  = [s.id for s in (src.schedules or [])]
            new_id = _recreate_subscription(conn, src, info, recipients, schedules)
            results.append({**info, 'status': 'created',
                            'details': f'new subscription id {new_id}'})
        except Exception as exc:
            results.append({**info, 'status': 'error', 'details': str(exc)})

    return results


def _recreate_subscription(conn, src, info, recipients, schedules) -> str:
    """Recreate one subscription on the target under subscription_admin."""
    mode = info['delivery_mode']
    pid  = info['project_id']
    name = info['subscription_name']
    delivery = src.delivery
    common = dict(
        connection=conn, name=name, project_id=pid,
        owner_id=SUBSCRIPTION_ADMIN_ID, schedules=schedules,
        contents=src.contents, recipients=recipients,
    )

    if mode == 'EMAIL':
        email = getattr(delivery, 'email', None)
        created = EmailSubscription.create(
            **common,
            email_subject=getattr(email, 'subject', None) or name,
            email_message=getattr(email, 'message', None),
            filename=getattr(email, 'filename', None),
        )
    elif mode == 'FILE':
        created = FileSubscription.create(
            **common, delivery=delivery,
            filename=_content_filename(delivery, 'file') or name,
        )
    elif mode == 'FTP':
        created = FTPSubscription.create(
            **common, delivery=delivery,
            filename=_content_filename(delivery, 'ftp') or name,
        )
    elif mode == 'HISTORY_LIST':
        created = HistoryListSubscription.create(**common, delivery=delivery)
    elif mode == 'CACHE':
        created = CacheUpdateSubscription.create(**common, delivery=delivery)
    else:
        raise ValueError(f'unsupported delivery mode {mode}')

    return created.id


# ══════════════════════════════════════════════════════════════════════════════
#  STEP: json-map
# ══════════════════════════════════════════════════════════════════════════════

def step_json_map(data, snap_by_name, out_dir: Path, env: str) -> tuple[list[dict], Path]:
    """Build the device → GoAnywhere mapping JSON, grouped by GoAnywhereSyncBase."""
    grouped: dict[str, dict] = {}

    for r in data[SHEET_MAP]:
        sync   = r.get('GoAnywhereSyncBase', '').strip() or '(none)'
        new    = r.get('NewDevice', '').strip()
        src    = r.get('Device', '').strip()
        dbase  = r.get('DeviceBase', '').strip()
        append = r.get('SubscriptionsUserAppend', '').strip()
        loc    = r.get('Distinct Subscription Locations', '').strip()
        dev_name = new or src
        linux_path = _linux_path(dbase)

        g = grouped.setdefault(sync, {
            'sync_base': sync,
            'goanywhere_sync_root': f'{LINUX_BASE}/{sync}' if sync != '(none)' else LINUX_BASE,
            'devices': {},
        })
        dev = g['devices'].setdefault(dev_name, {
            'device_name': dev_name,
            'device_id': _resolve_device_id(dev_name, snap_by_name) or '',
            'is_new_device': bool(new),
            'device_base': dbase,
            'linux_path': linux_path,
            'locations': [],
        })
        dev['locations'].append({
            'subscription_user_append': append,
            'source_unc': loc,
            'final_linux_path': (linux_path + (append if append else '')),
        })

    # Convert nested device dicts → lists
    mapping = []
    rows = []
    for sync, g in sorted(grouped.items()):
        g['devices'] = list(g['devices'].values())
        mapping.append(g)
        for dev in g['devices']:
            for locn in dev['locations']:
                rows.append({
                    'sync_base': sync, 'device_name': dev['device_name'],
                    'device_id': dev['device_id'], 'is_new_device': dev['is_new_device'],
                    'linux_path': dev['linux_path'],
                    'source_unc': locn['source_unc'],
                    'final_linux_path': locn['final_linux_path'],
                })

    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / JSON_MAP_FILENAME.format(env=env)
    json_path.write_text(json.dumps(mapping, indent=2, ensure_ascii=False), encoding='utf-8')
    logger.success('JSON mapping written → {p}', p=json_path)
    return rows, json_path


# ══════════════════════════════════════════════════════════════════════════════
#  REPORT
# ══════════════════════════════════════════════════════════════════════════════

def _write_report(results: dict[str, list[dict]], out_dir: Path, env: str,
                  apply: bool) -> Path:
    mode = 'apply' if apply else 'dryrun'
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f'subscription_migration_{mode}_{env}_{ts}.xlsx'

    summary = []
    for step, rows in results.items():
        counts: dict[str, int] = {}
        for r in rows:
            counts[r.get('status', '')] = counts.get(r.get('status', ''), 0) + 1
        summary.append({
            'step': step, 'rows': len(rows),
            **{f'status:{k}': v for k, v in sorted(counts.items())},
        })

    with pd.ExcelWriter(path, engine='openpyxl') as xw:
        pd.DataFrame(summary).to_excel(xw, sheet_name='Summary', index=False)
        for step, rows in results.items():
            df = pd.DataFrame(rows) if rows else pd.DataFrame([{'info': 'no rows'}])
            df.to_excel(xw, sheet_name=step[:31], index=False)

    logger.success('Report written → {p}', p=path)
    return path


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main(env, steps, apply, workbook, source_env, output_dir, concurrency) -> int:
    config  = MstrConfig(environment=MstrEnvironment(env))
    out_dir = output_dir or config.output_dir

    wb_path = Path(workbook)
    if not wb_path.exists():
        logger.error('Workbook not found: {p}', p=wb_path)
        return 1
    logger.info('Loading workbook {p}', p=wb_path)
    data = _load_workbook(wb_path)

    # ── CONNECTION (target) ───────────────────────────────────────────────────
    conn = get_mstrio_connection(config=config)
    # Workstation alternative:  conn = get_connection(workstationData)

    # ── CONNECTION (source, for admin-subs recreation) ────────────────────────
    src_conn = None
    if source_env:
        src_config = MstrConfig(environment=MstrEnvironment(source_env))
        src_conn = get_mstrio_connection(config=src_config)
        logger.info('Source connection established ({e})', e=source_env)

    # Single device snapshot reused across steps
    snap_by_id, snap_by_name = _device_snapshot(conn)
    logger.info('Target device snapshot: {n} device(s)', n=len(snap_by_id))

    results: dict[str, list[dict]] = {}

    if STEP_VALIDATE in steps:
        logger.info('=== {s} ===', s=STEP_VALIDATE)
        results[STEP_VALIDATE] = step_validate(data)

    if STEP_UPDATE_DEVICES in steps:
        logger.info('=== {s} ===', s=STEP_UPDATE_DEVICES)
        results[STEP_UPDATE_DEVICES] = step_update_devices(conn, data, snap_by_name, apply)

    if STEP_CREATE_DEVICES in steps:
        logger.info('=== {s} ===', s=STEP_CREATE_DEVICES)
        results[STEP_CREATE_DEVICES] = step_create_devices(conn, data, snap_by_name, apply)
        # refresh snapshot so update-addresses / json-map see new ids
        snap_by_id, snap_by_name = _device_snapshot(conn)

    if STEP_UPDATE_ADDRESSES in steps:
        logger.info('=== {s} ===', s=STEP_UPDATE_ADDRESSES)
        results[STEP_UPDATE_ADDRESSES] = step_update_addresses(
            conn, data, snap_by_id, snap_by_name, apply)

    if STEP_CLEAN_ADDRESSES in steps:
        logger.info('=== {s} ===', s=STEP_CLEAN_ADDRESSES)
        results[STEP_CLEAN_ADDRESSES] = step_clean_addresses(conn, data, concurrency, apply)

    if STEP_CLEAN_DEVICES in steps:
        logger.info('=== {s} ===', s=STEP_CLEAN_DEVICES)
        results[STEP_CLEAN_DEVICES] = step_clean_devices(conn, data, snap_by_id, apply)

    if STEP_ADMIN_SUBS in steps:
        logger.info('=== {s} ===', s=STEP_ADMIN_SUBS)
        results[STEP_ADMIN_SUBS] = step_admin_subs(conn, src_conn, data, apply)

    if STEP_JSON_MAP in steps:
        logger.info('=== {s} ===', s=STEP_JSON_MAP)
        rows, _ = step_json_map(data, snap_by_name, out_dir, env)
        results[STEP_JSON_MAP] = rows

    report = _write_report(results, out_dir, env, apply)

    mode = 'APPLY' if apply else 'DRY RUN'
    print(f'\n{mode} complete.  Steps: {", ".join(steps)}')
    for step, rows in results.items():
        print(f'  {step:18s} {len(rows)} row(s)')
    print(f'\nReview report: {report}')
    return 0


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='One-time MicroStrategy file-subscription cloud migration orchestrator.'
    )
    parser.add_argument('env', choices=[e.value for e in MstrEnvironment],
                        help='Target environment for all writes.')
    parser.add_argument('--steps', default='all',
                        help=f"Comma list of steps, or 'all'. Steps: {', '.join(ALL_STEPS)}")
    parser.add_argument('--apply', action='store_true',
                        help='Execute changes (default: dry run).')
    parser.add_argument('--workbook', default=DEFAULT_WORKBOOK, metavar='XLSX',
                        help=f'Mapping workbook (default: {DEFAULT_WORKBOOK}).')
    parser.add_argument('--source-env', default=None, choices=[e.value for e in MstrEnvironment],
                        help='Source environment to read Administrator sub definitions from '
                             '(required to recreate them under subscription_admin).')
    parser.add_argument('--output-dir', type=Path, default=None, metavar='DIR',
                        help='Output directory for report + JSON (default: MSTR_OUTPUT_DIR).')
    parser.add_argument('--concurrency', type=int, default=DEFAULT_CONCURRENCY, metavar='N',
                        help=f'Parallel workers for address scan (default: {DEFAULT_CONCURRENCY}).')

    args = parser.parse_args()

    if args.steps.strip().lower() == 'all':
        steps = ALL_STEPS
    else:
        steps = [s.strip() for s in args.steps.split(',') if s.strip()]
        bad = [s for s in steps if s not in ALL_STEPS]
        if bad:
            parser.error(f'Unknown step(s): {bad}. Valid: {ALL_STEPS}')
        # preserve canonical execution order
        steps = [s for s in ALL_STEPS if s in steps]

    raise SystemExit(main(
        env=args.env, steps=steps, apply=args.apply, workbook=args.workbook,
        source_env=args.source_env, output_dir=args.output_dir,
        concurrency=args.concurrency,
    ))
