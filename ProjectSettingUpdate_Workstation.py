"""ProjectSettingUpdate — update one setting across one or many projects.

MicroStrategy Workstation script. Uses the official mstrio-py connection; no
custom libraries. Runtime inputs follow the Workstation `$`-variable pattern
(text -> quoted string on substitution). Define these in the variable panel.

Workstation requires every variable to have a value. To mark one as "unset"
(so its coded default applies), enter EMPTY or any of these placeholder tokens,
case-insensitive: '-', 'null', 'blank', 'empty'. This applies to every variable
below AND to per-project values inside $target_projects_json.

    Informational
    -------------
    $list_projects     bool   Default False. True -> print 'name;GUID' per line
    $list_settings     bool   Default False. True -> list settings of $sample_project
    $sample_project    text   A project NAME or GUID to sample settings from
    $settings_search   text   Optional wildcard to filter settings by NAME or
                              DESCRIPTION, e.g. '*cache*' (a bare word is treated
                              as *word*). Matched settings show their description.
                              Blank = all
    $settings_detail   bool   Default False. True -> include each setting's
                              description (list_properties(show_description=True))

    Action (update)
    ---------------
    $setting_name      text   Exact setting key to update, e.g. 'maxReportExecutionTime'
                              (use the informational listing to find the exact key)
    $setting_value     text   Value to set (the default for every target project)
    $target_projects_json text The target projects. Each identifier may be a
                              project NAME or a 32-hex GUID (auto-detected).
                              Three accepted shapes:
                                object {proj: value}  -> per-project value; null/""
                                                          falls back to $setting_value
                                array  [proj, ...]     -> identifiers only, all use
                                                          $setting_value
                                set    {proj, ...}     -> identifiers only, all use
                                                          $setting_value
    $apply             bool   Default False (dry-run: show old -> new only).
                              True commits the update via ProjectSettings.update().

Example $target_projects_json (per-project values — object form):
    {
      "Crop Canada": null,
      "3614601D9F9144D384FD3ABBFE2AC0F2": 600,
      "Consumer Analytics": 300
    }

Example $target_projects_json (identifiers only — all use $setting_value):
    ["E28640CB464968A376CD7C8DE3F9ADE3", "Crop US", "Crop Canada"]
  or the set form:
    {"E28640CB464968A376CD7C8DE3F9ADE3", "Crop US", "Crop Canada"}

mstrio-py used:
    Environment(conn).list_projects()                 -> Project objects (.name/.id)
    Project(conn, id=GUID) | Project(conn, name=NAME) -> resolve either identifier
    project.settings                                  -> ProjectSettings
        .list_properties(show_names=True, show_description=<detail>)
        .alter(**{setting_name: value}) then .update()
"""

import ast
import fnmatch
import json
import re

from mstrio.connection import get_connection
from mstrio.server import Environment, Project

# Workstation requires every variable to have a value. To mark an optional /
# not-applicable variable as "unset", enter EMPTY or any of these placeholder
# tokens (case-insensitive) in the panel — all collapse to the coded default:
_EMPTY_SENTINELS = {'', '-', 'null', 'blank', 'empty'}


def _blank(value, default):
    """Return `default` if `value` is unset (None / empty / a placeholder
    token from _EMPTY_SENTINELS, case-insensitive); otherwise `value`."""
    if value is None:
        return default
    if isinstance(value, str) and value.strip().lower() in _EMPTY_SENTINELS:
        return default
    return value


# --- Substituted by Workstation (text -> 'quoted') --------------------------
# _blank() applies the default; the trailing normalizers (_as_bool / _coerce /
# _load_targets) then handle the string-vs-native typing Workstation gives you.
LIST_PROJECTS = _blank($list_projects, False)
LIST_SETTINGS = _blank($list_settings, False)
SAMPLE_PROJECT = _blank($sample_project, None)
SETTINGS_SEARCH = _blank($settings_search, '')
SETTINGS_DETAIL = _blank($settings_detail, False)

SETTING_NAME = _blank($setting_name, None)
SETTING_VALUE = _blank($setting_value, None)
TARGET_PROJECTS_JSON = _blank($target_projects_json, '{}')
APPLY = _blank($apply, False)
# ----------------------------------------------------------------------------

GUID_RE = re.compile(r'^[0-9A-Fa-f]{32}$')
BREAK = '-' * 70


# ---------------------------------------------------------------------------
# Input coercion — Workstation substitutes text as strings; normalize them
# ---------------------------------------------------------------------------
def _as_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in ('true', '1', 'yes', 'y')


def _coerce(value):
    """Turn a Workstation string into a native type (bool/int/float/str).

    Booleans and JSON scalars ('true', '600', '1.5', '"txt"') convert; anything
    else stays a plain string. Native (non-str) values pass through untouched so
    per-project JSON values keep their type.
    """
    if not isinstance(value, str):
        return value
    text = value.strip()
    low = text.lower()
    if low in ('true', 'false'):
        return low == 'true'
    try:
        return json.loads(text)
    except Exception:  # noqa: BLE001 - not JSON, keep as-is
        return text


def _is_guid(identifier):
    return bool(GUID_RE.match(str(identifier).strip()))


def resolve_project(conn, identifier):
    """Return a Project resolved by GUID or by name (auto-detected)."""
    identifier = str(identifier).strip()
    if _is_guid(identifier):
        return Project(conn, id=identifier)
    return Project(conn, name=identifier)


# ---------------------------------------------------------------------------
# Informational
# ---------------------------------------------------------------------------
def list_all_projects(conn):
    """Print every project as 'name;GUID', one per line, sorted by name."""
    projects = sorted(
        Environment(conn).list_projects(),
        key=lambda p: (p.name or '').lower(),
    )
    print(f'{len(projects)} project(s):')
    for proj in projects:
        print(f'{proj.name};{proj.id}')
    return projects


def _wildcard(term):
    """A bare word becomes a contains-match; explicit wildcards pass through."""
    term = (term or '').strip()
    if not term:
        return ''
    if any(ch in term for ch in '*?[]'):
        return term
    return f'*{term}*'


def list_project_settings(conn, sample_identifier, search='', detail=False):
    """List (optionally filtered) settings of one sample project.

    The search wildcard matches the setting NAME or its DESCRIPTION (case-
    insensitive). Descriptions are always shown when a search is active (so the
    matched text is visible) or when detail=True.

    detail=False, no search -> 'key: value' per setting.
    detail=True  or search   -> also print each setting's description
                    (list_properties(show_description=True, show_names=True)).
    """
    proj = resolve_project(conn, sample_identifier)
    project_settings = proj.settings
    # Always pull descriptions so the search can match them; display them when
    # detail is on OR a search is active.
    props = project_settings.list_properties(show_names=True, show_description=True)

    pattern = _wildcard(search)
    show_desc = detail or bool(pattern)

    keys = sorted(props)
    if pattern:
        pat = pattern.lower()
        keys = [
            k for k in keys
            if fnmatch.fnmatch(k.lower(), pat)
            or fnmatch.fnmatch(str(props[k]['description'] or '').lower(), pat)
        ]

    print(f'Settings for project {proj.name} ({proj.id}) — '
          f'{len(keys)} of {len(props)} shown'
          + (f", filter '{search}' (name + description)" if search else ''))
    for key in keys:
        entry = props[key]               # {'value': ..., 'description': ...}
        print(BREAK)
        print(f'{key}: {entry["value"]}')
        if show_desc:
            print(f'    description: {entry["description"]}')
    print(BREAK)
    return {k: props[k] for k in keys}


# ---------------------------------------------------------------------------
# Action — update one setting across one or many projects
# ---------------------------------------------------------------------------
def _load_targets(raw):
    """Parse the target-projects input into a {identifier: value_or_None} dict.

    Accepts three shapes (identifier = project NAME or 32-hex GUID):
      * object   {"proj": value, ...}   -> per-project value overrides
      * array    ["projA", "projB"]     -> identifiers only, all use the default
      * set      {"projA", "projB"}     -> identifiers only, all use the default

    Arrays/sets map every identifier to None so the shared $setting_value
    applies. The set form ({"a","b"}) is not valid JSON, so it is parsed as a
    Python literal.
    """
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, (list, set, tuple)):
        return {ident: None for ident in raw}
    if not raw or not str(raw).strip():
        return {}

    text = str(raw).strip()
    try:
        parsed = json.loads(text)          # proper JSON object or array
    except Exception:
        try:
            parsed = ast.literal_eval(text)  # Python literal: {a,b}, [a,b], {a:b}
        except Exception as exc:  # noqa: BLE001
            raise ValueError(
                'target_projects could not be parsed as JSON or a Python '
                f'list/set/dict literal: {exc}'
            )

    if isinstance(parsed, dict):
        return parsed
    if isinstance(parsed, (list, set, tuple)):
        return {ident: None for ident in parsed}
    raise ValueError(
        'target_projects must be an object {proj: value}, an array [proj, ...], '
        'or a set {proj, ...} of project name/GUID identifiers'
    )


def update_setting_in_projects(conn, setting_name, default_value, targets, apply):
    """Set `setting_name` on each target project. Dry-run unless apply=True."""
    if not setting_name:
        raise ValueError('setting_name is required for an update')
    if not targets:
        raise ValueError('target_projects_json is empty — nothing to update')

    default_value = _coerce(default_value)
    mode = 'APPLY' if apply else 'DRY-RUN'
    print(f'\n=== Update {setting_name!r} on {len(targets)} project(s) [{mode}] ===')

    results = []
    for identifier, override in targets.items():
        # A blank/placeholder per-project value falls back to the shared default
        override = _blank(override, None)
        value = default_value if override is None else _coerce(override)
        row = {'identifier': identifier, 'setting': setting_name,
               'new_value': value}
        try:
            proj = resolve_project(conn, identifier)
            before = proj.settings.list_properties().get(setting_name, '<unknown>')
            row.update({'project_name': proj.name, 'project_id': proj.id,
                        'old_value': before})

            if before == '<unknown>':
                # Guard against a typo'd key silently doing nothing
                raise KeyError(
                    f'setting {setting_name!r} not found on this project — '
                    f'check the exact key via the settings listing'
                )

            if apply:
                proj.settings.alter(**{setting_name: value})
                proj.settings.update()
                after = proj.settings.list_properties().get(setting_name)
                row['result_value'] = after
                row['status'] = 'updated'
            else:
                row['status'] = 'dry-run'

            print(f'{proj.name} ({proj.id}): {setting_name} '
                  f'{before!r} -> {value!r} [{row["status"]}]')
        except Exception as exc:  # noqa: BLE001 - record and continue other projects
            row['status'] = 'error'
            row['error'] = str(exc)
            name = row.get('project_name', identifier)
            print(f'{name}: ERROR updating {setting_name} -> {value!r}: {exc}')
        results.append(row)

    ok = sum(1 for r in results if r['status'] in ('updated', 'dry-run'))
    print(f'=== {ok}/{len(results)} project(s) OK ({mode}) ===')
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    conn = get_connection(connectionData)  # noqa: F821  (injected by Workstation)

    # --- Informational ---
    if _as_bool(LIST_PROJECTS):
        list_all_projects(conn)

    if _as_bool(LIST_SETTINGS):
        if not SAMPLE_PROJECT:
            print('list_settings requested but $sample_project is empty — skipping')
        else:
            list_project_settings(
                conn, SAMPLE_PROJECT,
                search=SETTINGS_SEARCH,
                detail=_as_bool(SETTINGS_DETAIL),
            )

    # --- Action ---
    targets = _load_targets(TARGET_PROJECTS_JSON)
    if SETTING_NAME and targets:
        update_setting_in_projects(
            conn, SETTING_NAME, SETTING_VALUE, targets, apply=_as_bool(APPLY)
        )
    else:
        print('\nNo update performed (need both $setting_name and '
              '$target_projects_json).')


main()
