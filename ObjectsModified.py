"""
ObjectsModified.py

List every object modified between two dates, across all loaded projects.

Search
──────
One project-wide metadata search per project (run in parallel), via
`start_full_search` → paged `get_search_results`:
  * domain = PROJECT
  * object_types = every mstrio ObjectTypes value except Folder (8). A type
             criterion is REQUIRED — with only a date range the I-Server
             answers ERR006 "Please specify at least one object search
             criteria". Config-level types just match nothing in PROJECT.
             Folders are also dropped client-side as a safety net.
  * begin/end_modification_time             (server-side date filter, UTC)
  v2 only (I-Server ≥ 11.4.1200):
  * scope  = SearchScope.NOT_MANAGED_ONLY   (must be the enum — mstrio's
             `_scope_to_search_rest` calls `.name` on anything it doesn't
             match, so the string 'not_managed_only' raises AttributeError)
  * include_hidden = True                   (visibility=all)
  * exclude_folders                         (area narrowing, see Areas)
On older servers (e.g. 11.3.x) the v1 search has none of these: hidden
objects come back unfiltered, system-managed objects are NOT excluded, and
areas are filtered client-side only. Logged once at startup; the v2 args are
not passed at all, so mstrio doesn't warn on every search.

Dates
─────
--start / --end are LOCAL dates; --end is inclusive and defaults to today.
They are sent as UTC: start 00:00 local → UTC, end+1 day 00:00 local → UTC.
Rows are re-checked against the same window client-side.

Areas
─────
Each object is classified by its top-level folder (ancestors[1]) GUID, resolved
per project via GET /folders/preDefined/{type}/id:
  Public  = PUBLIC_OBJECTS (1)     Schema = SCHEMA_OBJECTS (24)
  Profile = parent of PROFILE_OBJECTS (19) — that type is the *login user's*
            profile folder; the Profiles container has no predefined type
  Other   = anything else (e.g. hidden system folders)
If a GUID can't be resolved (e.g. the login user has no profile folder in that
project) that area falls back to matching the folder name, with a warning.

Default: all areas. `--areas Public Schema` limits output; `--exclude-profiles`
drops Profile. Library shortcuts (type 18 / subtype 4609, one per user Library
item, live in Profiles) can be dropped with `--exclude-library-shortcuts`.

When the areas are narrowed (v2), the unselected area folders go into
`exclude_folders`, so the server never scans them (Profiles is the big one).
`root_id` is deliberately NOT used: mstrio validates a root via
`conn.temporary_project_change()`, which switches the shared connection's
project and races with the other project threads. 'Other' has no folder to
exclude, so it is dropped client-side; every row is area-checked anyway.

Output columns
──────────────
  project_id / project_name
  object_id / object_name
  object_type / object_type_name         e.g. 3 / REPORT_DEFINITION
  object_subtype / object_subtype_name   e.g. 776 / OLAP_CUBE (type alone
                                         doesn't split report vs cube, doc vs dossier)
  ext_type
  area                                   Public / Schema / Profile / Other
  location                               folder path, project root excluded
  date_modified                          local time
  date_modified_utc
  date_created                           local time
  owner_id / owner_name
  hidden

Not available from search: who last modified an object; deleted objects.

Usage
─────
  python ObjectsModified.py dev --start 2026-10-01
  python ObjectsModified.py dev --start 2026-10-01 --end 2026-10-15
  python ObjectsModified.py dev --start 2026-10-01 --areas Public Schema
  python ObjectsModified.py dev --start 2026-10-01 --exclude-profiles
  python ObjectsModified.py dev --start 2026-10-01 --exclude-library-shortcuts
  python ObjectsModified.py dev --start 2026-10-01 --project-id GUID1 "Project Name"
  python ObjectsModified.py dev --start 2026-10-01 --format excel --concurrency 5
"""

import argparse
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Optional

from loguru import logger
# mstrio.api wrappers take project_id per call — the OO classes (Folder, Entity)
# read conn.project_id, which isn't safe with projects searched in parallel.
from mstrio.api import folders as folders_api, objects as objects_api
from mstrio.object_management.search_operations import (
    SearchDomain,
    SearchScope,
    get_search_results,
    start_full_search,
)
from mstrio.object_management.predefined_folders import PredefinedFolders
from mstrio.server import Environment
from mstrio.types import ObjectSubTypes, ObjectTypes
from mstrio.utils.version_helper import meets_minimal_version

from mstrio_core import (
    MstrConfig,
    OBJECT_TYPE_ID_MAP,
    get_mstrio_connection,
    object_location,
    write_csv,
    write_excel,
)
from mstrio_core.config import MstrEnvironment

# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

OUTPUT_FILENAME = 'objects_modified_{env}_{start}_{end}_{ts}.{ext}'
DEFAULT_CONCURRENCY = 5
PAGE_SIZE = 1000

FOLDER_TYPE = ObjectTypes.FOLDER.value                         # 8
LIBRARY_SHORTCUT_SUBTYPE = ObjectSubTypes.LIBRARY_SHORTCUT.value  # 4609

# The I-Server rejects a search with no name/type criterion (ERR006 "Please
# specify at least one object search criteria") — a date range alone doesn't
# count. So every known type except Folder is sent; configuration-level types
# simply match nothing in the PROJECT domain.
SEARCH_TYPES = sorted(t.value for t in ObjectTypes
                      if t.value is not None and t.value > 0 and t.value != FOLDER_TYPE)

V2_SEARCH_VERSION = '11.4.1200'  # scope / visibility / excludedFolders / includeSubfolders

AREAS = ['Public', 'Schema', 'Profile', 'Other']

# Area → predefined folder type (EnumDSSXMLFolderNames). PROFILE_OBJECTS (19) is
# the *login user's* profile folder (Profiles\<user>); the Profiles container
# has no predefined type, so its GUID is taken from that folder's ancestors.
AREA_PREDEFINED_FOLDER = {
    'Public':  PredefinedFolders.PUBLIC_OBJECTS.value,   # 1
    'Schema':  PredefinedFolders.SCHEMA_OBJECTS.value,   # 24
    'Profile': PredefinedFolders.PROFILE_OBJECTS.value,  # 19 → parent
}

# Fallback when a GUID can't be resolved: top-level folder name (lowercase) → area
AREA_BY_ROOT_FOLDER_NAME = {
    'public objects': 'Public',
    'schema objects': 'Schema',
    'profiles':       'Profile',
}

COLUMNS = [
    'project_id',
    'project_name',
    'object_id',
    'object_name',
    'object_type',
    'object_type_name',
    'object_subtype',
    'object_subtype_name',
    'ext_type',
    'area',
    'location',
    'date_modified',
    'date_modified_utc',
    'date_created',
    'owner_id',
    'owner_name',
    'hidden',
]


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _local_midnight_utc(d: date) -> datetime:
    """Local midnight of `d`, as an aware UTC datetime."""
    return datetime.combine(d, time.min).astimezone().astimezone(timezone.utc)


def _search_window(start: date, end: date) -> tuple[datetime, datetime]:
    """[start 00:00 local, end+1 00:00 local) in UTC — end date inclusive."""
    return _local_midnight_utc(start), _local_midnight_utc(end + timedelta(days=1))


def _iso_z(dt: datetime) -> str:
    """Format for begin/end_modification_time: yyyy-MM-ddTHH:mm:ssZ."""
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


_TZ_NO_COLON = re.compile(r'([+-]\d{2})(\d{2})$')


def _parse_ts(val) -> Optional[datetime]:
    """Parse a search timestamp ('2026-10-01T12:34:56.000+0000') → aware UTC."""
    if not val:
        return None
    if isinstance(val, datetime):
        dt = val
    else:
        s = str(val).strip().replace('Z', '+00:00')
        s = _TZ_NO_COLON.sub(r'\1:\2', s)
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fmt(dt: Optional[datetime], local: bool) -> str:
    if dt is None:
        return ''
    return (dt.astimezone() if local else dt).strftime('%Y-%m-%d %H:%M:%S')


def _int(val) -> Optional[int]:
    """Search results may carry enum-ish values; normalise to int."""
    val = getattr(val, 'value', val)
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _type_name(type_id: Optional[int]) -> str:
    try:
        return ObjectTypes(type_id).name
    except ValueError:
        return OBJECT_TYPE_ID_MAP.get(type_id, '')


def _subtype_name(subtype_id: Optional[int]) -> str:
    try:
        return ObjectSubTypes(subtype_id).name
    except ValueError:
        return ''


def _predefined_folder_id(conn, project_id: str, folder_type: int) -> Optional[str]:
    """Predefined folder GUID (GET /folders/preDefined/{type}/id), or None."""
    try:
        r = folders_api.get_predefined_folder_id(conn, folder_type=folder_type,
                                                 project_id=project_id)
        logger.debug('Predefined folder {t} → {status}', t=folder_type, status=r.status_code)
        return r.json().get('id')
    except Exception as exc:
        logger.debug('Predefined folder {t} lookup failed: {e}', t=folder_type, e=exc)
    return None


def _resolve_area_roots(conn, project) -> dict[str, str]:
    """Area → top-level folder GUID for this project. Missing areas are left
    out (classified by name instead)."""
    roots: dict[str, str] = {}
    for area, folder_type in AREA_PREDEFINED_FOLDER.items():
        fid = _predefined_folder_id(conn, project.id, folder_type)
        if fid and area == 'Profile':
            # Profiles\<user> → its top-level ancestor is the Profiles folder
            anc = _fetch_ancestors(conn, project.id, fid, FOLDER_TYPE)
            fid = anc[1].get('id') if len(anc) >= 2 else None
        if fid:
            roots[area] = fid
        else:
            logger.warning('[{p}] {a} folder GUID not resolved — matching by name',
                           p=project.name, a=area)
    logger.debug('[{p}] area roots: {r}', p=project.name, r=roots)
    return roots


def _area(ancestors: list[dict], roots: dict[str, str]) -> str:
    """Area from the top-level folder (ancestors[0] is the project root):
    GUID match first, then name for any area whose GUID wasn't resolved."""
    if len(ancestors) < 2:
        return 'Other'
    top = ancestors[1]
    top_id = (top.get('id') or '').upper()
    for area, fid in roots.items():
        if fid.upper() == top_id:
            return area
    by_name = AREA_BY_ROOT_FOLDER_NAME.get((top.get('name') or '').strip().lower())
    if by_name and by_name not in roots:
        return by_name
    return 'Other'


def _excluded_folders(selected: set[str], roots: dict[str, str]) -> list[str]:
    """Top-level folder GUIDs of the unselected areas, for `exclude_folders`.

    Narrowing is done by exclusion only, never `root_id`: given a root, mstrio
    validates it via `conn.temporary_project_change()`, which switches the
    shared connection's project and races with the other project threads.
    'Other' has no folder to exclude, so its rows are dropped client-side.
    """
    return [roots[a] for a in AREAS if a not in selected and a in roots]


def _fetch_ancestors(conn, project_id: str, object_id: str, type_id: int) -> list[dict]:
    """Ancestors of one object (GET /objects/{id}) — fallback when a search row
    has none, and to find the Profiles folder."""
    try:
        r = objects_api.get_object_info(conn, id=object_id, object_type=type_id,
                                        project_id=project_id)
        logger.debug('Object info {id} → {status}', id=object_id, status=r.status_code)
        if r.ok:
            return r.json().get('ancestors') or []
    except Exception as exc:
        logger.warning('Ancestor lookup failed for {id}: {e}', id=object_id, e=exc)
    return []


def _resolve_projects(conn, filters: Optional[list[str]]) -> list:
    """Loaded projects only; warn about unloaded ones and unmatched filters."""
    env = Environment(conn)
    loaded = env.list_loaded_projects()
    loaded_ids = {p.id for p in loaded}
    unloaded = [p for p in env.list_projects() if p.id not in loaded_ids]
    for p in unloaded:
        logger.warning('Project not loaded, skipped: {name} ({id})', name=p.name, id=p.id)

    if not filters:
        return loaded

    wanted = {f.strip().lower() for f in filters}
    picked = [p for p in loaded if p.id.lower() in wanted or p.name.lower() in wanted]
    matched = {p.id.lower() for p in picked} | {p.name.lower() for p in picked}
    for f in sorted(wanted - matched):
        logger.warning('--project-id {f} matched no loaded project', f=f)
    return picked


# ══════════════════════════════════════════════════════════════════════════════
#  SEARCH
# ══════════════════════════════════════════════════════════════════════════════

def _run_search(
    conn,
    project,
    begin_utc: datetime,
    end_utc: datetime,
    exclude: list[str],
    v2: bool,
):
    """Start one project-wide metadata search and yield its result dicts page
    by page."""
    # v2-only args are passed only when the server has v2 search; otherwise
    # mstrio drops them and warns on every call (the gap is logged once in main).
    v2_args = dict(
        exclude_folders=exclude or None,
        scope=SearchScope.NOT_MANAGED_ONLY,
        include_hidden=True,
    ) if v2 else {}
    search = start_full_search(
        conn,
        project=project.id,
        domain=SearchDomain.PROJECT,
        object_types=SEARCH_TYPES,
        begin_modification_time=_iso_z(begin_utc),
        end_modification_time=_iso_z(end_utc),
        **v2_args,
    )
    search_id = search['id']
    logger.debug('[{p}] search {sid} started (excluded={x})',
                 p=project.name, sid=search_id, x=exclude)

    offset = 0
    while True:
        page = get_search_results(
            conn,
            search_id=search_id,
            project=project.id,
            limit=PAGE_SIZE,
            offset=offset,
            to_dictionary=True,
        ) or []
        yield from page
        if len(page) < PAGE_SIZE:
            return
        offset += PAGE_SIZE


def _search_project(
    conn,
    project,
    begin_utc: datetime,
    end_utc: datetime,
    areas: set[str],
    exclude_library_shortcuts: bool,
    v2: bool,
) -> tuple[list[dict], dict]:
    """Search one project; return (rows, counts)."""
    counts = {'found': 0, 'folders': 0, 'shortcuts': 0, 'area': 0, 'window': 0, 'kept': 0}

    roots = _resolve_area_roots(conn, project)
    # v1 search has no excludedFolders — areas are then filtered client-side only
    exclude = _excluded_folders(areas, roots) if v2 else []
    logger.debug('[{p}] excluded folders: {x}', p=project.name, x=exclude)

    rows: list[dict] = []
    for obj in _run_search(conn, project, begin_utc, end_utc, exclude, v2):
        counts['found'] += 1
        type_id = _int(obj.get('type'))
        subtype_id = _int(obj.get('subtype'))
        if type_id == FOLDER_TYPE:
            counts['folders'] += 1
            continue
        if exclude_library_shortcuts and subtype_id == LIBRARY_SHORTCUT_SUBTYPE:
            counts['shortcuts'] += 1
            continue

        modified = _parse_ts(obj.get('date_modified'))
        if modified is not None and not (begin_utc <= modified < end_utc):
            counts['window'] += 1
            continue

        ancestors = obj.get('ancestors')
        if ancestors is None:
            ancestors = _fetch_ancestors(conn, project.id, obj.get('id'), type_id)
        area = _area(ancestors, roots)
        if area not in areas:
            counts['area'] += 1
            continue

        owner = obj.get('owner') or {}
        ext_type = _int(obj.get('ext_type'))
        rows.append({
            'project_id':          project.id,
            'project_name':        project.name,
            'object_id':           obj.get('id', ''),
            'object_name':         obj.get('name', ''),
            'object_type':         type_id if type_id is not None else '',
            'object_type_name':    _type_name(type_id),
            'object_subtype':      subtype_id if subtype_id is not None else '',
            'object_subtype_name': _subtype_name(subtype_id),
            'ext_type':            ext_type if ext_type is not None else '',
            'area':                area,
            'location':            object_location(ancestors) if ancestors else '',
            'date_modified':       _fmt(modified, local=True),
            'date_modified_utc':   _fmt(modified, local=False),
            'date_created':        _fmt(_parse_ts(obj.get('date_created')), local=True),
            'owner_id':            owner.get('id', ''),
            'owner_name':          owner.get('name', ''),
            'hidden':              bool(obj.get('hidden')),
        })

    counts['kept'] = len(rows)
    return rows, counts


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main(
    env: str,
    start: date,
    end: date,
    areas: set[str],
    exclude_library_shortcuts: bool,
    fmt: str,
    output_dir: Optional[Path],
    filter_projects: Optional[list[str]],
    concurrency: int,
) -> int:
    if end < start:
        logger.error('--end {e} is before --start {s}', e=end, s=start)
        return 1

    config = MstrConfig(environment=MstrEnvironment(env))
    out_dir = output_dir or config.output_dir

    begin_utc, end_utc = _search_window(start, end)
    logger.info(
        'Window (local, end inclusive): {s} → {e}  |  UTC: {bu} → {eu} (exclusive)',
        s=start, e=end, bu=_iso_z(begin_utc), eu=_iso_z(end_utc),
    )
    logger.info('Areas: {a}  |  library shortcuts: {ls}',
                a=', '.join(a for a in AREAS if a in areas),
                ls='excluded' if exclude_library_shortcuts else 'included')

    conn = get_mstrio_connection(config=config)
    v2 = meets_minimal_version(conn.iserver_version, V2_SEARCH_VERSION)
    logger.info('I-Server version {v} ({s} search)', v=conn.iserver_version,
                s='v2' if v2 else 'v1')
    if not v2:
        logger.warning(
            'I-Server {v} predates {req}: the v1 search has no scope or visibility '
            'filter, so system-managed objects are NOT excluded and hidden objects '
            'come back unfiltered; area narrowing runs client-side only',
            v=conn.iserver_version, req=V2_SEARCH_VERSION,
        )

    projects = _resolve_projects(conn, filter_projects)
    if not projects:
        logger.error('No loaded projects to search')
        return 1
    logger.info('{n} project(s) to search, {c} at a time', n=len(projects), c=concurrency)

    all_rows: list[dict] = []
    failed = 0
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {
            pool.submit(_search_project, conn, p, begin_utc, end_utc,
                        areas, exclude_library_shortcuts, v2): p
            for p in projects
        }
        for fut in as_completed(futures):
            proj = futures[fut]
            try:
                rows, c = fut.result()
            except Exception as exc:
                failed += 1
                logger.error('[{name}] ({id}) search failed: {e}',
                             name=proj.name, id=proj.id, e=exc)
                continue
            logger.info(
                '[{name}] {kept} kept of {found} found '
                '(folders {f}, library shortcuts {s}, other areas {a}, outside window {w})',
                name=proj.name, kept=c['kept'], found=c['found'], f=c['folders'],
                s=c['shortcuts'], a=c['area'], w=c['window'],
            )
            all_rows.extend(rows)

    all_rows.sort(key=lambda r: (r['project_name'], r['date_modified_utc'], r['object_name']))

    ts = datetime.now().strftime('%Y%m%d_%H%M')
    ext = {'csv': 'csv', 'json': 'json', 'excel': 'xlsx'}[fmt]
    out_file = Path(out_dir) / OUTPUT_FILENAME.format(
        env=env, start=start.strftime('%Y%m%d'), end=end.strftime('%Y%m%d'), ts=ts, ext=ext)

    if fmt == 'json':
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(json.dumps(all_rows, ensure_ascii=False, indent=2), encoding='utf-8')
    elif fmt == 'excel':
        write_excel([[r[c] for c in COLUMNS] for r in all_rows], path=out_file,
                    columns=COLUMNS, sheet_name='ObjectsModified')
    else:
        write_csv([[r[c] for c in COLUMNS] for r in all_rows], columns=COLUMNS, path=out_file)

    logger.success('Exported {n} object(s) from {p} project(s) → {f}',
                   n=len(all_rows), p=len(projects) - failed, f=out_file)
    if failed:
        logger.error('{n} project(s) failed — see log', n=failed)
    return 1 if failed else 0


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def _parse_date(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f'expected YYYY-MM-DD, got {s!r}')


def _parse_area(s: str) -> str:
    for a in AREAS:
        if a.lower() == s.strip().lower():
            return a
    raise argparse.ArgumentTypeError(f'area must be one of {AREAS}, got {s!r}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='List objects modified between two dates across all loaded projects '
                    '(not-managed only, hidden included, folders excluded).'
    )
    parser.add_argument('env', choices=[e.value for e in MstrEnvironment],
                        help='MicroStrategy environment (dev / qa / prod).')
    parser.add_argument('--start', required=True, type=_parse_date, metavar='YYYY-MM-DD',
                        help='First local date to include.')
    parser.add_argument('--end', type=_parse_date, default=None, metavar='YYYY-MM-DD',
                        help='Last local date to include (inclusive; default: today).')
    parser.add_argument('--areas', nargs='+', type=_parse_area, default=None, metavar='AREA',
                        help=f'Limit to areas: {" ".join(AREAS)} (default: all).')
    parser.add_argument('--exclude-profiles', action='store_true',
                        help='Drop objects under Profiles (same as leaving Profile out of --areas).')
    parser.add_argument('--exclude-library-shortcuts', action='store_true',
                        help='Drop Library shortcuts (type 18 / subtype 4609).')
    parser.add_argument('--format', '-f', choices=['csv', 'json', 'excel'], default='csv',
                        dest='fmt', help='Output format (default: csv).')
    parser.add_argument('--output-dir', type=Path, default=None, metavar='DIR',
                        help='Output directory (default: MSTR_OUTPUT_DIR).')
    parser.add_argument('--project-id', nargs='+', metavar='GUID_OR_NAME', dest='filter_projects',
                        help='Limit to specific loaded projects (GUID or name).')
    parser.add_argument('--concurrency', type=int, default=DEFAULT_CONCURRENCY, metavar='N',
                        help=f'Projects searched in parallel (default: {DEFAULT_CONCURRENCY}).')

    args = parser.parse_args()

    selected_areas = set(args.areas or AREAS)
    if args.exclude_profiles:
        selected_areas.discard('Profile')

    raise SystemExit(
        main(
            env=args.env,
            start=args.start,
            end=args.end or date.today(),
            areas=selected_areas,
            exclude_library_shortcuts=args.exclude_library_shortcuts,
            fmt=args.fmt,
            output_dir=args.output_dir,
            filter_projects=args.filter_projects,
            concurrency=max(1, args.concurrency),
        )
    )
