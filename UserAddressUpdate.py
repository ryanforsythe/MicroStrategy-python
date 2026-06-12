"""
UserAddressUpdate.py

Add new ContactAddresses or update existing ones for MicroStrategy users,
driven by an Excel input file.

Input Excel columns (flexible header matching, case-insensitive):
  UserID / user_id / user id   – User GUID
  Name / address_name          – Address name
  Physical Address / address   – Physical delivery path (email or file path)
  Delivery Type / delivery     – Email | File | FTP | Printer | Unknown
  Device / device_name         – Device name (resolved to device ID at runtime)

Matching logic
──────────────
For each input row, the user's existing addresses are checked for a match on
(name, device_id) case-insensitively.
  0 matches → add new address
  1 match  + same physical_address → skip (already current)
  1 match  + different physical_address → update
  2+ matches → log ambiguity, skip (update would be arbitrary)

Rows with an unresolvable device name or unrecognised delivery type are
written to the output with status=error.

Dry-run by default; --apply to commit adds and updates.

Usage
─────
  python UserAddressUpdate.py dev --input addresses.xlsx
  python UserAddressUpdate.py dev --input addresses.xlsx --apply
  python UserAddressUpdate.py dev --input addresses.xlsx --apply --concurrency 20
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from loguru import logger
from mstrio.distribution_services.device import list_devices
from mstrio.users_and_groups import User
from mstrio.users_and_groups.contact import ContactDeliveryType

from mstrio_core import MstrConfig, get_mstrio_connection, write_csv
from mstrio_core.config import MstrEnvironment

# ── Workstation import (uncomment when running from Workstation) ──────────────
# from mstrio.connection import get_connection

# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

DEFAULT_CONCURRENCY = 10
OUTPUT_FILENAME = 'user_address_update_{env}_{ts}.csv'

# Delivery type values accepted by add_address / update_address
_DELIVERY_TYPE_MAP: dict[str, str] = {
    'email':        'email',
    'file':         'file',
    'ftp':          'ftp',
    'printer':      'printer',
    'mobile':       'mobile_android',
    'android':      'mobile_android',
    'iphone':       'mobile_iphone',
    'ipad':         'mobile_ipad',
    'onedrive':     'onedrive',
    'sharepoint':   'sharepoint',
    's3':           's3',
    'googledrive':  'googledrive',
    'unknown':      'unsupported',
    '':             'unsupported',
}

OUTPUT_COLUMNS = [
    'user_id',
    'name',
    'physical_address',
    'delivery_type',
    'device',
    'status',
    'status_details',
]

STATUS_ADDED     = 'added'
STATUS_UPDATED   = 'updated'
STATUS_SKIPPED   = 'skipped'
STATUS_AMBIGUOUS = 'ambiguous'
STATUS_ERROR     = 'error'
STATUS_DRY       = 'dry-run'


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _find_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    """Return the first column name (case-insensitive) that matches a candidate."""
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def _read_input(path: Path) -> list[dict]:
    """
    Read the Excel file and return a list of row dicts with normalised keys:
        user_id, name, physical_address, delivery_type, device
    """
    df = pd.read_excel(path, dtype=str).fillna('')

    col_user     = _find_col(df, 'UserID',          'user_id',       'user id')
    col_name     = _find_col(df, 'Name',             'address_name',  'addr_name')
    col_phys     = _find_col(df, 'Physical Address', 'physical_address', 'address')
    col_delivery = _find_col(df, 'Delivery Type',    'delivery_type', 'delivery')
    col_device   = _find_col(df, 'Device',           'device_name',   'device name')

    missing = [
        label for label, col in [
            ('UserID',          col_user),
            ('Name',            col_name),
            ('Physical Address', col_phys),
            ('Delivery Type',   col_delivery),
            ('Device',          col_device),
        ]
        if col is None
    ]
    if missing:
        raise ValueError(
            f'Required column(s) not found in input file: {", ".join(missing)}. '
            f'Found columns: {list(df.columns)}'
        )

    rows = []
    for _, row in df.iterrows():
        rows.append({
            'user_id':          str(row[col_user]).strip(),
            'name':             str(row[col_name]).strip(),
            'physical_address': str(row[col_phys]).strip(),
            'delivery_type':    str(row[col_delivery]).strip(),
            'device':           str(row[col_device]).strip(),
        })

    return rows


def _normalize_delivery_type(raw: str) -> Optional[str]:
    """Map an Excel delivery type string to the mstrio-py string value.
    Returns None when the value is unrecognised."""
    key = raw.lower().strip()
    return _DELIVERY_TYPE_MAP.get(key)


def _build_device_lookup(conn) -> dict[str, str]:
    """
    Load all devices and return a case-insensitive name → device_id dict.
    Uses to_dictionary=True to avoid DeviceType enum errors for unknown types
    (e.g. 'gcs') that may exist on the server but not in the installed mstrio-py.
    When multiple devices share the same name, the first one wins.
    """
    devices = list_devices(conn, to_dictionary=True)
    lookup: dict[str, str] = {}
    for dev in devices:
        name = dev.get('name') or ''
        did  = dev.get('id')   or ''
        if not name or not did:
            continue
        key = name.lower()
        if key in lookup:
            logger.warning(
                'Multiple devices named {name!r} — using first match', name=name,
            )
        else:
            lookup[key] = did
    logger.info('Loaded {n} device(s)', n=len(lookup))
    return lookup


def _fetch_user(conn, user_id: str) -> Optional[User]:
    """Fetch a User object by ID; return None on failure."""
    try:
        return User(conn, id=user_id)
    except Exception as exc:
        logger.warning('Could not fetch user {uid}: {err}', uid=user_id, err=exc)
        return None


def _addr_key(name: str, device_id: str) -> tuple[str, str]:
    return name.lower(), device_id.upper()


def _build_addr_index(user: User) -> dict[tuple[str, str], list]:
    """Build {(name_lower, device_id_upper): [ContactAddress, ...]} for a user."""
    index: dict[tuple[str, str], list] = {}
    for addr in (user.addresses or []):
        dev_id = addr.device.id if addr.device else ''
        key = _addr_key(addr.name, dev_id)
        index.setdefault(key, []).append(addr)
    return index


# ══════════════════════════════════════════════════════════════════════════════
#  CORE PROCESSING
# ══════════════════════════════════════════════════════════════════════════════

def _process_row(
    row: dict,
    user: User,
    addr_index: dict,
    device_lookup: dict[str, str],
    apply: bool,
) -> tuple[str, str]:
    """
    Process one Excel row against a User's existing addresses.
    Returns (status, details) — does NOT mutate addr_index (caller must rebuild
    after any write if needed, but User.add/update_address() refreshes user.addresses).
    """
    device_key = row['device'].lower()
    if device_key not in device_lookup:
        return STATUS_ERROR, f'Device not found: {row["device"]!r}'

    device_id = device_lookup[device_key]

    dt_str = _normalize_delivery_type(row['delivery_type'])
    if dt_str is None:
        return STATUS_ERROR, f'Unrecognised delivery type: {row["delivery_type"]!r}'

    new_phys = row['physical_address']
    addr_name = row['name']
    key = _addr_key(addr_name, device_id)
    matches = addr_index.get(key, [])

    if len(matches) == 0:
        # ── ADD ──────────────────────────────────────────────────────────────
        if apply:
            try:
                user.add_address(
                    name=addr_name,
                    address=new_phys,
                    default=False,
                    delivery_type=dt_str,
                    device_id=device_id,
                )
            except Exception as exc:
                return STATUS_ERROR, str(exc)
        return STATUS_ADDED if apply else STATUS_DRY, 'would add' if not apply else ''

    if len(matches) > 1:
        # ── AMBIGUOUS ─────────────────────────────────────────────────────────
        return (
            STATUS_AMBIGUOUS,
            f'{len(matches)} existing addresses match name+device; skipped',
        )

    # ── SINGLE MATCH ─────────────────────────────────────────────────────────
    existing = matches[0]
    old_phys = existing.physical_address or ''

    if old_phys == new_phys:
        return STATUS_SKIPPED, 'no change'

    # UPDATE
    if apply:
        try:
            user.update_address(id=existing.id, address=new_phys)
        except Exception as exc:
            return STATUS_ERROR, str(exc)
    details = f'{old_phys!r} → {new_phys!r}'
    if not apply:
        details = 'would update: ' + details
    return STATUS_UPDATED if apply else STATUS_DRY, details


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main(
    env: str,
    input_path: Path,
    apply: bool,
    output_dir: Optional[Path],
    concurrency: int,
) -> int:

    config  = MstrConfig(environment=MstrEnvironment(env))
    out_dir = output_dir or config.output_dir

    # ── CONNECTION ────────────────────────────────────────────────────────────
    # Option A: mstrio_core / .env
    conn = get_mstrio_connection(config=config)
    # Option B: Workstation (WORKSTATION)
    # conn = get_connection(workstationData)  # noqa: F821

    # ── READ INPUT ────────────────────────────────────────────────────────────
    logger.info('Reading input: {p}', p=input_path)
    rows = _read_input(input_path)
    logger.info('{n} row(s) loaded', n=len(rows))

    # ── DEVICE LOOKUP ─────────────────────────────────────────────────────────
    device_lookup = _build_device_lookup(conn)

    # ── FETCH USERS IN PARALLEL ───────────────────────────────────────────────
    unique_user_ids = {r['user_id'] for r in rows if r['user_id']}
    logger.info('Fetching {n} unique user(s)...', n=len(unique_user_ids))

    user_map: dict[str, Optional[User]] = {}
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(_fetch_user, conn, uid): uid for uid in unique_user_ids}
        for future in as_completed(futures):
            uid = futures[future]
            user_map[uid] = future.result()

    # ── PROCESS ROWS ──────────────────────────────────────────────────────────
    # Build address index per user (rebuilt after each write so state stays fresh)
    addr_indices: dict[str, dict] = {}
    for uid, user in user_map.items():
        if user is not None:
            addr_indices[uid] = _build_addr_index(user)

    counters = {
        STATUS_ADDED: 0, STATUS_UPDATED: 0, STATUS_SKIPPED: 0,
        STATUS_AMBIGUOUS: 0, STATUS_ERROR: 0, STATUS_DRY: 0,
    }
    result_rows: list[list] = []

    for row in rows:
        uid = row['user_id']
        user = user_map.get(uid)

        if user is None:
            status, details = STATUS_ERROR, f'User not found or fetch failed: {uid}'
        else:
            addr_index = addr_indices[uid]
            status, details = _process_row(row, user, addr_index, device_lookup, apply)

            # Rebuild index after any write so subsequent rows for the same user
            # see the updated address list
            if status in (STATUS_ADDED, STATUS_UPDATED):
                addr_indices[uid] = _build_addr_index(user)

        counters[status] = counters.get(status, 0) + 1
        result_rows.append([
            row['user_id'],
            row['name'],
            row['physical_address'],
            row['delivery_type'],
            row['device'],
            status,
            details,
        ])

        verb = 'DRY' if status == STATUS_DRY else status.upper()
        logger.debug(
            '{verb} user={uid} addr={name!r}: {details}',
            verb=verb, uid=uid, name=row['name'], details=details,
        )

    # ── OUTPUT ────────────────────────────────────────────────────────────────
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / OUTPUT_FILENAME.format(env=env, ts=ts)
    write_csv(result_rows, columns=OUTPUT_COLUMNS, path=out_file)

    mode = 'APPLIED' if apply else 'DRY RUN'
    logger.success(
        '{mode} complete — added:{a} updated:{u} skipped:{s} '
        'ambiguous:{amb} error:{e} dry:{d}  → {p}',
        mode=mode,
        a=counters[STATUS_ADDED],
        u=counters[STATUS_UPDATED],
        s=counters[STATUS_SKIPPED],
        amb=counters[STATUS_AMBIGUOUS],
        e=counters[STATUS_ERROR],
        d=counters[STATUS_DRY],
        p=out_file,
    )
    print(
        f'\n{mode}: added={counters[STATUS_ADDED]}'
        f'  updated={counters[STATUS_UPDATED]}'
        f'  skipped={counters[STATUS_SKIPPED]}'
        f'  ambiguous={counters[STATUS_AMBIGUOUS]}'
        f'  error={counters[STATUS_ERROR]}'
        f'  dry={counters[STATUS_DRY]}'
        f'\nResults: {out_file}'
    )
    return 0


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=(
            'Add or update MicroStrategy user ContactAddresses from an Excel file.'
        )
    )
    parser.add_argument(
        'env',
        choices=[e.value for e in MstrEnvironment],
        help='MicroStrategy environment (dev / qa / prod).',
    )
    parser.add_argument(
        '--input', '-i',
        type=Path,
        required=True,
        metavar='XLSX',
        help='Input Excel file.',
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Execute adds and updates (default: dry run).',
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=None,
        metavar='DIR',
        help='Directory for results CSV (default: MSTR_OUTPUT_DIR).',
    )
    parser.add_argument(
        '--concurrency',
        type=int,
        default=DEFAULT_CONCURRENCY,
        metavar='N',
        help=f'Parallel workers for user fetching (default: {DEFAULT_CONCURRENCY}).',
    )

    args = parser.parse_args()

    raise SystemExit(
        main(
            env=args.env,
            input_path=args.input,
            apply=args.apply,
            output_dir=args.output_dir,
            concurrency=args.concurrency,
        )
    )
