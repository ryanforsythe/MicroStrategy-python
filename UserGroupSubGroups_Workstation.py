"""UserGroupSubGroups — list every sub-group under a base user group (recursive),
optionally renaming each sub-group with a prefix.

MicroStrategy Workstation script. Uses the official mstrio-py connection; no
custom libraries. Runtime inputs follow the Workstation `$`-variable pattern
(text -> quoted string on substitution). Define these in the variable panel.

Workstation requires every variable to have a value. To mark one as "unset"
(so its coded default applies), enter EMPTY or any of these placeholder tokens,
case-insensitive: '-', 'null', 'blank', 'empty'.

    $user_group      text   Base user group NAME or 32-hex GUID (auto-detected)
    $include_users   bool   Default False. True -> also print the users found
                            at each level of the tree
    $rename_prefix   text   Optional. Prefix to add to every sub-group name,
                            e.g. 'DEPRECATE-'. Blank = no rename (list only).
                            The base group itself is never renamed; groups that
                            already start with the prefix are skipped.
    $apply           bool   Default False (dry-run: show old -> new only).
                            True commits the renames via UserGroup.alter(name=).

Recursion walks UserGroup.list_members() down every level. A group reachable
from more than one parent is processed (and renamed) once; repeat sightings
are marked '(seen)' in the tree. The visited set also guards against cycles.

mstrio-py used:
    UserGroup(conn, id=GUID) | UserGroup(conn, name=NAME) -> resolve base group
    UserGroup.list_members()                              -> User / UserGroup objects
    UserGroup.alter(name=)                                -> rename
"""

import json
import re

from mstrio.connection import get_connection
from mstrio.users_and_groups import User, UserGroup

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
USER_GROUP = _blank($user_group, None)
INCLUDE_USERS = _blank($include_users, False)
RENAME_PREFIX = _blank($rename_prefix, '')
APPLY = _blank($apply, False)
# ----------------------------------------------------------------------------

GUID_RE = re.compile(r'^[0-9A-Fa-f]{32}$')
BREAK = '-' * 70


def _as_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in ('true', '1', 'yes', 'y')


def _resolve_group(conn, identifier):
    identifier = str(identifier).strip()
    if GUID_RE.match(identifier):
        return UserGroup(connection=conn, id=identifier)
    return UserGroup(connection=conn, name=identifier)


# ---------------------------------------------------------------------------
# Recursive walk
# ---------------------------------------------------------------------------
def collect_sub_groups(group, include_users=False):
    """Depth-first walk under `group`. Returns (sub_groups, tree_lines).

    sub_groups: list of dicts {id, name, parent_id, parent_name, depth, group}
    in discovery order, each group once (the base group is not included).
    """
    visited = {group.id}
    sub_groups = []
    tree = [f'{group.name}  [{group.id}]']

    def _walk(parent, depth):
        members = parent.list_members() or []
        # Groups first, then users; alphabetical within each for a stable tree
        groups = sorted((m for m in members if isinstance(m, UserGroup)),
                        key=lambda m: m.name.lower())
        users = sorted((m for m in members if isinstance(m, User)),
                       key=lambda m: m.name.lower())
        indent = '    ' * depth

        for child in groups:
            if child.id in visited:
                tree.append(f'{indent}+ {child.name}  [{child.id}]  (seen)')
                continue
            visited.add(child.id)
            tree.append(f'{indent}+ {child.name}  [{child.id}]')
            sub_groups.append({
                'id': child.id,
                'name': child.name,
                'parent_id': parent.id,
                'parent_name': parent.name,
                'depth': depth,
                'group': child,
            })
            _walk(child, depth + 1)

        if include_users:
            for user in users:
                tree.append(f'{indent}- {user.name}  [{user.id}]')

    _walk(group, 1)
    return sub_groups, tree


# ---------------------------------------------------------------------------
# Rename
# ---------------------------------------------------------------------------
def rename_sub_groups(sub_groups, prefix, apply=False):
    """Prefix every sub-group name. Returns a list of result dicts."""
    results = []
    for sg in sub_groups:
        old_name = sg['name']
        if old_name.startswith(prefix):
            results.append({'id': sg['id'], 'old_name': old_name,
                            'new_name': old_name, 'status': 'skipped (has prefix)'})
            continue
        new_name = f'{prefix}{old_name}'
        if not apply:
            results.append({'id': sg['id'], 'old_name': old_name,
                            'new_name': new_name, 'status': 'dry-run'})
            continue
        try:
            sg['group'].alter(name=new_name)
            results.append({'id': sg['id'], 'old_name': old_name,
                            'new_name': new_name, 'status': 'renamed'})
        except Exception as exc:  # noqa: BLE001 - report and keep going
            results.append({'id': sg['id'], 'old_name': old_name,
                            'new_name': new_name, 'status': f'error: {exc}'})
    return results


def main():
    if not USER_GROUP:
        raise ValueError('user_group variable is required (name or GUID).')

    conn = get_connection(connectionData)
    base = _resolve_group(conn, USER_GROUP)

    sub_groups, tree = collect_sub_groups(base, include_users=_as_bool(INCLUDE_USERS))

    print(BREAK)
    print(f'Base group: {base.name}  [{base.id}]')
    print(f'Sub-groups found (all levels): {len(sub_groups)}'
          f'  max depth: {max((s["depth"] for s in sub_groups), default=0)}')
    print(BREAK)
    print('\n'.join(tree))
    print(BREAK)

    output = {
        'base_group': {'id': base.id, 'name': base.name},
        'sub_groups': [{k: v for k, v in s.items() if k != 'group'} for s in sub_groups],
    }

    if RENAME_PREFIX:
        apply = _as_bool(APPLY)
        results = rename_sub_groups(sub_groups, RENAME_PREFIX, apply=apply)
        print(f'Rename with prefix {RENAME_PREFIX!r} — '
              f'{"APPLY" if apply else "DRY-RUN (set apply=True to commit)"}')
        for r in results:
            print(f'  {r["status"]:<22} {r["old_name"]}  ->  {r["new_name"]}  [{r["id"]}]')
        counts = {}
        for r in results:
            key = r['status'].split(':')[0]
            counts[key] = counts.get(key, 0) + 1
        print(f'Summary: {counts}')
        print(BREAK)
        output['rename'] = {'prefix': RENAME_PREFIX, 'apply': apply,
                            'summary': counts, 'results': results}

    print(json.dumps(output, indent=2, default=str))


# Workstation exec()s the script as a string, so a __main__ guard is unreliable
main()
