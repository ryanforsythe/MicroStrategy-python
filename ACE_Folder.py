"""
ACE_Folder.py — Audit and synchronize FOLDER ACLs (access control entries)
without touching the underlying objects.

All operations set inheritable=False and never propagate to children, so they
change only the folder's own permission entries — not the objects inside it.

Subcommands
───────────
  export  Output every folder (excluding profile folders) → one row per ACE:
          folder GUID, name, full path (project name + folder name excluded),
          trustee id/name/type, deny flag, rights (int + decoded), inheritable.

  copy    Use an existing folder as the ACL source and apply the *same* ACL to
          a list of target folders (uploaded by CSV/Excel). Fully synchronizes:
          sets each source entry on the target and REMOVES target trustees that
          are not on the source.

  apply   Apply ACEs from a CSV/Excel file (the export's shape). A `remove`
          column removes a listed trustee; `--remove-unlisted` also removes any
          trustee on the folder that is not present in the input for that folder.

Default is DRY RUN for copy/apply — pass --apply to execute.

Connection
──────────
  Default     : mstrio_core / .env  (CLI / PyCharm)
  Workstation : search "WORKSTATION" below to swap.

Usage
─────
  python ACE_Folder.py export dev --project E28640CB464968A376CD7C8DE3F9ADE3
  python ACE_Folder.py export dev --project "PacMan" --format excel

  python ACE_Folder.py copy  dev --project ID --source-folder FID --targets targets.xlsx
  python ACE_Folder.py copy  dev --project ID --source-folder FID --targets targets.csv --apply

  python ACE_Folder.py apply dev --project ID --input aces.xlsx
  python ACE_Folder.py apply dev --project ID --input aces.csv --remove-unlisted --apply

Input file columns
──────────────────
  copy  --targets : a column holding folder GUIDs (folder_id / folderid / id / guid).
  apply --input   : folder_id, trustee_id, rights, deny, inheritable, remove
                    (case-insensitive; trustee_name ignored; rights is the int).
"""

import argparse
import csv
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from loguru import logger
from mstrio.helpers import Rights
from mstrio.object_management import Folder, full_search
from mstrio.server import Environment
from mstrio.types import ObjectTypes

from mstrio_core import MstrConfig, get_mstrio_connection, write_csv, write_excel, read_excel
from mstrio_core.config import MstrEnvironment

# ── Workstation import (uncomment when running from Workstation) ──────────────
# from mstrio.connection import get_connection

# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

DEFAULT_CONCURRENCY = 10
FOLDER_TYPE = ObjectTypes.FOLDER.value  # 8
REMOVE_ALL_RIGHTS = 255                 # full bitmask, for fully clearing an entry

# Folder names whose subtree is excluded from `export` (profile folders).
PROFILE_FOLDER_NAMES = {'profiles'}

# Rights bit flags, low→high, for decoding the integer rights value.
_RIGHTS_FLAGS = [
    (Rights.BROWSE,      'BROWSE'),
    (Rights.USE_EXECUTE, 'USE_EXECUTE'),
    (Rights.READ,        'READ'),
    (Rights.WRITE,       'WRITE'),
    (Rights.DELETE,      'DELETE'),
    (Rights.CONTROL,     'CONTROL'),
    (Rights.USE,         'USE'),
    (Rights.EXECUTE,     'EXECUTE'),
]

EXPORT_COLUMNS = [
    'folder_id', 'folder_name', 'folder_path',
    'trustee_id', 'trustee_name', 'trustee_type', 'trustee_subtype',
    'deny', 'rights', 'rights_names', 'inheritable',
]

RESULT_COLUMNS = [
    'folder_id', 'trustee_id', 'trustee_name', 'deny', 'rights',
    'action', 'status', 'details',
]


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _decode_rights(rights: int) -> str:
    """Decode an integer rights bitmask into a pipe-joined list of flag names."""
    try:
        r = int(rights)
    except (TypeError, ValueError):
        return ''
    if r == REMOVE_ALL_RIGHTS:
        return 'FULL_CONTROL'
    return '|'.join(name for flag, name in _RIGHTS_FLAGS if r & int(flag))


def _is_profile_folder(name: str, ancestors: list[dict]) -> bool:
    """A folder is a profile folder if it (or any ancestor) is the 'Profiles'
    system folder."""
    names = {(name or '').strip().lower()}
    names |= {(a.get('name') or '').strip().lower() for a in (ancestors or [])}
    return bool(names & PROFILE_FOLDER_NAMES)


def _folder_path(ancestors: list[dict]) -> str:
    """Path excluding the project root (ancestors[0]) and the folder's own name
    (which is never in `ancestors`)."""
    named = [a['name'] for a in (ancestors or []) if 'name' in a]
    return '/' + '/'.join(named[1:]) if len(named) > 1 else '/'


def _parse_bool(val, default: bool = False) -> bool:
    if val is None or val == '':
        return default
    return str(val).strip().lower() in {'1', 'true', 'yes', 'y', 't', 'x', 'remove', 'deny'}


def _resolve_project_id(conn, value: str) -> str:
    """Accept a project GUID (32-hex) or a project name and return the GUID."""
    v = (value or '').strip()
    if len(v) == 32 and all(c in '0123456789abcdefABCDEF' for c in v):
        return v.upper()
    for p in Environment(conn).list_projects():
        if p.name.lower() == v.lower():
            return p.id
    raise ValueError(f'Project not found by name: {value!r}')


def _read_table(path: Path) -> list[dict]:
    """Read a CSV or Excel file into a list of row dicts (string values)."""
    ext = path.suffix.lower()
    if ext in ('.xlsx', '.xls'):
        df = read_excel(path).fillna('')
        return [{str(k): ('' if v is None else str(v)) for k, v in row.items()}
                for row in df.to_dict(orient='records')]
    text = path.read_text(encoding='utf-8-sig')
    delimiter = ';' if text.count(';') > text.count(',') else ','
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    return [{(k or '').strip(): (v or '') for k, v in row.items()} for row in reader]


def _col(row: dict, *names: str) -> str:
    """Case-insensitive column lookup."""
    lower = {k.lower(): v for k, v in row.items()}
    for n in names:
        if n.lower() in lower:
            return str(lower[n.lower()]).strip()
    return ''


# ══════════════════════════════════════════════════════════════════════════════
#  ACL primitives (inheritable=False, no child propagation)
# ══════════════════════════════════════════════════════════════════════════════

def _ace_index(folder: Folder) -> dict[tuple, object]:
    """{(trustee_id, deny): ACE} for a folder's current ACL."""
    return {(a.trustee_id, bool(a.deny)): a for a in (folder.acl or [])}


def _set_ace(folder: Folder, trustee_id: str, rights: int, deny: bool,
             inheritable: bool) -> None:
    """REPLACE (upsert) a trustee's entry to exactly `rights`."""
    folder.acl_alter(rights=int(rights), trustees=[trustee_id], denied=deny,
                     inheritable=inheritable)


def _remove_ace(folder: Folder, trustee_id: str, rights: int, deny: bool) -> None:
    """Remove a trustee's entry by removing all its rights bits."""
    folder.acl_remove(rights=int(rights) or REMOVE_ALL_RIGHTS,
                      trustees=[trustee_id], denied=deny, inheritable=False)


# ══════════════════════════════════════════════════════════════════════════════
#  EXPORT
# ══════════════════════════════════════════════════════════════════════════════

def cmd_export(conn, project_id: str, fmt: str, out_dir: Path,
               concurrency: int, include_profiles: bool) -> int:
    logger.info('Searching folders in project {p}...', p=project_id)
    found = full_search(conn, project=project_id, object_types=FOLDER_TYPE,
                        to_dictionary=True)
    logger.info('{n} folder object(s) found', n=len(found))

    folder_ids = [f.get('id') for f in found if f.get('id')]

    def _fetch(fid: str):
        try:
            folder = Folder(conn, id=fid)
            return fid, folder, None
        except Exception as exc:
            return fid, None, str(exc)

    rows: list[dict] = []
    skipped_profiles = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for fut in as_completed([pool.submit(_fetch, fid) for fid in folder_ids]):
            fid, folder, err = fut.result()
            if err or folder is None:
                errors += 1
                logger.warning('Could not read folder {f}: {e}', f=fid, e=err)
                continue

            ancestors = folder.ancestors or []
            if not include_profiles and _is_profile_folder(folder.name, ancestors):
                skipped_profiles += 1
                continue

            path = _folder_path(ancestors)
            aces = folder.acl or []
            if not aces:
                rows.append({
                    'folder_id': fid, 'folder_name': folder.name, 'folder_path': path,
                    'trustee_id': '', 'trustee_name': '', 'trustee_type': '',
                    'trustee_subtype': '', 'deny': '', 'rights': '',
                    'rights_names': '', 'inheritable': '',
                })
                continue
            for a in aces:
                rows.append({
                    'folder_id': fid, 'folder_name': folder.name, 'folder_path': path,
                    'trustee_id': a.trustee_id, 'trustee_name': a.trustee_name,
                    'trustee_type': a.trustee_type, 'trustee_subtype': a.trustee_subtype,
                    'deny': bool(a.deny), 'rights': int(a.rights),
                    'rights_names': _decode_rights(a.rights),
                    'inheritable': bool(a.inheritable),
                })

    logger.info('Folders exported: {f} | profile folders skipped: {s} | errors: {e}',
                f=len({r["folder_id"] for r in rows}), s=skipped_profiles, e=errors)

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_dir.mkdir(parents=True, exist_ok=True)
    if fmt == 'excel':
        out_file = out_dir / f'folder_aces_{project_id}_{ts}.xlsx'
        write_excel([[r[c] for c in EXPORT_COLUMNS] for r in rows],
                    columns=EXPORT_COLUMNS, path=out_file)
    else:
        out_file = out_dir / f'folder_aces_{project_id}_{ts}.csv'
        write_csv([[r[c] for c in EXPORT_COLUMNS] for r in rows],
                  columns=EXPORT_COLUMNS, path=out_file)

    logger.success('Exported {n} ACE row(s) → {p}', n=len(rows), p=out_file)
    print(f'\nExported {len(rows)} ACE row(s) → {out_file}')
    return 0


# ══════════════════════════════════════════════════════════════════════════════
#  COPY  (source folder ACL → target folders, full sync incl. removals)
# ══════════════════════════════════════════════════════════════════════════════

def cmd_copy(conn, project_id: str, source_folder: str, targets_path: Path,
             apply: bool, out_dir: Path, concurrency: int) -> int:
    src = Folder(conn, id=source_folder)
    src_aces = src.acl or []
    src_trustees = {a.trustee_id for a in src_aces}
    logger.info('Source folder {n!r} has {a} ACE(s), {t} trustee(s)',
                n=src.name, a=len(src_aces), t=len(src_trustees))

    target_rows = _read_table(targets_path)
    target_ids = []
    for r in target_rows:
        fid = _col(r, 'folder_id', 'folderid', 'id', 'guid')
        if fid:
            target_ids.append(fid)
    logger.info('{n} target folder(s) loaded', n=len(target_ids))

    def _process(fid: str) -> list[dict]:
        out = []
        try:
            tgt = Folder(conn, id=fid)
            current = _ace_index(tgt)
        except Exception as exc:
            return [{'folder_id': fid, 'trustee_id': '', 'trustee_name': '',
                     'deny': '', 'rights': '', 'action': 'load',
                     'status': 'error', 'details': str(exc)}]

        # 1) set/replace every source ACE on the target
        for a in src_aces:
            action = 'set'
            status, details = 'dry-run', f'set {_decode_rights(a.rights)}'
            if apply:
                try:
                    _set_ace(tgt, a.trustee_id, a.rights, bool(a.deny), inheritable=False)
                    status, details = 'done', f'set {_decode_rights(a.rights)}'
                except Exception as exc:
                    status, details = 'error', str(exc)
            out.append({'folder_id': fid, 'trustee_id': a.trustee_id,
                        'trustee_name': a.trustee_name, 'deny': bool(a.deny),
                        'rights': int(a.rights), 'action': action,
                        'status': status, 'details': details})

        # 2) remove target trustees not present on the source
        for (tid, deny), a in current.items():
            if tid in src_trustees:
                continue
            status, details = 'dry-run', 'remove (not on source)'
            if apply:
                try:
                    _remove_ace(tgt, tid, a.rights, deny)
                    status, details = 'done', 'removed (not on source)'
                except Exception as exc:
                    status, details = 'error', str(exc)
            out.append({'folder_id': fid, 'trustee_id': tid,
                        'trustee_name': a.trustee_name, 'deny': deny,
                        'rights': int(a.rights), 'action': 'remove',
                        'status': status, 'details': details})
        return out

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for fut in as_completed([pool.submit(_process, fid) for fid in target_ids]):
            results.extend(fut.result())

    return _write_results(results, out_dir, 'copy', project_id, apply)


# ══════════════════════════════════════════════════════════════════════════════
#  APPLY  (ACE rows from file)
# ══════════════════════════════════════════════════════════════════════════════

def cmd_apply(conn, project_id: str, input_path: Path, remove_unlisted: bool,
              apply: bool, out_dir: Path, concurrency: int) -> int:
    rows = _read_table(input_path)

    # Group input rows by folder
    by_folder: dict[str, list[dict]] = {}
    for r in rows:
        fid = _col(r, 'folder_id', 'folderid', 'id', 'guid')
        if not fid:
            continue
        by_folder.setdefault(fid, []).append(r)
    logger.info('{n} folder(s) in input, {r} ACE row(s)', n=len(by_folder), r=len(rows))

    def _process(fid: str, frows: list[dict]) -> list[dict]:
        out = []
        try:
            folder = Folder(conn, id=fid)
            current = _ace_index(folder)
        except Exception as exc:
            return [{'folder_id': fid, 'trustee_id': '', 'trustee_name': '',
                     'deny': '', 'rights': '', 'action': 'load',
                     'status': 'error', 'details': str(exc)}]

        listed_trustees: set[str] = set()
        for r in frows:
            tid   = _col(r, 'trustee_id', 'trusteeid', 'id')
            tname = _col(r, 'trustee_name', 'trusteename', 'name')
            if not tid:
                continue
            listed_trustees.add(tid)
            deny   = _parse_bool(_col(r, 'deny'))
            inher  = _parse_bool(_col(r, 'inheritable'), default=False)
            remove = _parse_bool(_col(r, 'remove'))
            rights_raw = _col(r, 'rights')
            rights = int(rights_raw) if rights_raw not in ('', None) else REMOVE_ALL_RIGHTS

            if remove:
                existing = current.get((tid, deny))
                if existing is None:
                    out.append({'folder_id': fid, 'trustee_id': tid, 'trustee_name': tname,
                                'deny': deny, 'rights': rights, 'action': 'remove',
                                'status': 'skipped', 'details': 'trustee not present'})
                    continue
                status, details = 'dry-run', 'remove'
                if apply:
                    try:
                        _remove_ace(folder, tid, existing.rights, deny)
                        status, details = 'done', 'removed'
                    except Exception as exc:
                        status, details = 'error', str(exc)
                out.append({'folder_id': fid, 'trustee_id': tid, 'trustee_name': tname,
                            'deny': deny, 'rights': int(existing.rights), 'action': 'remove',
                            'status': status, 'details': details})
            else:
                status, details = 'dry-run', f'set {_decode_rights(rights)}'
                if apply:
                    try:
                        _set_ace(folder, tid, rights, deny, inheritable=inher)
                        status, details = 'done', f'set {_decode_rights(rights)}'
                    except Exception as exc:
                        status, details = 'error', str(exc)
                out.append({'folder_id': fid, 'trustee_id': tid, 'trustee_name': tname,
                            'deny': deny, 'rights': rights, 'action': 'set',
                            'status': status, 'details': details})

        # Optionally remove trustees on the folder not present in the input
        if remove_unlisted:
            for (tid, deny), a in current.items():
                if tid in listed_trustees:
                    continue
                status, details = 'dry-run', 'remove (unlisted)'
                if apply:
                    try:
                        _remove_ace(folder, tid, a.rights, deny)
                        status, details = 'done', 'removed (unlisted)'
                    except Exception as exc:
                        status, details = 'error', str(exc)
                out.append({'folder_id': fid, 'trustee_id': tid, 'trustee_name': a.trustee_name,
                            'deny': deny, 'rights': int(a.rights), 'action': 'remove-unlisted',
                            'status': status, 'details': details})
        return out

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(_process, fid, frows) for fid, frows in by_folder.items()]
        for fut in as_completed(futures):
            results.extend(fut.result())

    return _write_results(results, out_dir, 'apply', project_id, apply)


# ══════════════════════════════════════════════════════════════════════════════
#  RESULT REPORT
# ══════════════════════════════════════════════════════════════════════════════

def _write_results(results: list[dict], out_dir: Path, verb: str,
                   project_id: str, apply: bool) -> int:
    counts: dict[str, int] = {}
    for r in results:
        counts[r['status']] = counts.get(r['status'], 0) + 1

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    mode = 'apply' if apply else 'dryrun'
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f'folder_ace_{verb}_{mode}_{project_id}_{ts}.csv'
    write_csv([[r.get(c, '') for c in RESULT_COLUMNS] for r in results],
              columns=RESULT_COLUMNS, path=out_file)

    logger.success('{verb} {mode}: {c} → {p}', verb=verb, mode=mode.upper(),
                   c=counts, p=out_file)
    print(f'\n{verb.upper()} {mode.upper()} — {counts}\nResults: {out_file}')
    return 0


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN / CLI
# ══════════════════════════════════════════════════════════════════════════════

def _connect(env: str):
    config = MstrConfig(environment=MstrEnvironment(env))
    # Option A: mstrio_core / .env
    conn = get_mstrio_connection(config=config)
    # Option B: Workstation (WORKSTATION)
    # conn = get_connection(workstationData)  # noqa: F821
    return config, conn


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Audit and synchronize folder ACLs without impacting underlying objects.')
    sub = parser.add_subparsers(dest='command', required=True)

    envs = [e.value for e in MstrEnvironment]

    def _common(sp):
        sp.add_argument('env', choices=envs)
        sp.add_argument('--project', required=True, metavar='ID_OR_NAME',
                        help='Project GUID or name.')
        sp.add_argument('--output-dir', type=Path, default=None, metavar='DIR')
        sp.add_argument('--concurrency', type=int, default=DEFAULT_CONCURRENCY, metavar='N')

    p_exp = sub.add_parser('export', help='Export all folder ACLs (one row per ACE).')
    _common(p_exp)
    p_exp.add_argument('--format', choices=['csv', 'excel'], default='csv')
    p_exp.add_argument('--include-profiles', action='store_true',
                       help='Include profile folders (excluded by default).')

    p_cp = sub.add_parser('copy', help='Copy one folder ACL to many target folders.')
    _common(p_cp)
    p_cp.add_argument('--source-folder', required=True, metavar='FID')
    p_cp.add_argument('--targets', required=True, type=Path, metavar='CSV_OR_XLSX',
                      help='File with a folder-id column.')
    p_cp.add_argument('--apply', action='store_true', help='Execute (default: dry run).')

    p_ap = sub.add_parser('apply', help='Apply ACE rows from a CSV/Excel file.')
    _common(p_ap)
    p_ap.add_argument('--input', required=True, type=Path, metavar='CSV_OR_XLSX')
    p_ap.add_argument('--remove-unlisted', action='store_true',
                      help='Remove trustees on a folder not present in the input.')
    p_ap.add_argument('--apply', action='store_true', help='Execute (default: dry run).')

    args = parser.parse_args()

    config, conn = _connect(args.env)
    out_dir = args.output_dir or config.output_dir
    project_id = _resolve_project_id(conn, args.project)
    try:
        conn.select_project(project_id=project_id)
    except Exception:
        pass  # full_search / Folder still accept explicit project/id

    if args.command == 'export':
        return cmd_export(conn, project_id, args.format, out_dir,
                          args.concurrency, args.include_profiles)
    if args.command == 'copy':
        return cmd_copy(conn, project_id, args.source_folder, args.targets,
                        args.apply, out_dir, args.concurrency)
    if args.command == 'apply':
        return cmd_apply(conn, project_id, args.input, args.remove_unlisted,
                         args.apply, out_dir, args.concurrency)
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
