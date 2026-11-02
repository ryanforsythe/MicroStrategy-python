"""UserGroupMembership — list every user group a user belongs to, including all
parent groups of those groups.

MicroStrategy Workstation script. Uses the official mstrio-py connection; no
custom libraries. Runtime inputs follow the Workstation `$`-variable pattern
(text -> quoted string on substitution). Define these in the variable panel.

Workstation requires every variable to have a value. To mark one as "unset"
(so its coded default applies), enter EMPTY or any of these placeholder tokens,
case-insensitive: '-', 'null', 'blank', 'empty'.

    $user_login      text   User login (case-insensitive)
    $output_format   text   Optional. 'csv' (semicolon-delimited) or 'json'.
                            Blank = plain list. All formats print to the output
                            panel; the script writes no file.

Each group is marked with its membership, using the mstrio-py term "direct
parent" for User.memberships:
    Direct     a direct parent group of the user
    Inherited  reached only through another group

The walk starts at the user's direct parent groups and follows each group's own
memberships upward. A group reachable by more than one path is listed once, and
stays Direct if it is also a direct parent; the visited set also guards against
cycles.

mstrio-py used:
    list_users(conn)                         -> find the user by login
    User.memberships                         -> direct parent groups of the user
    mstrio.api.usergroups.get_memberships()  -> direct parent groups of a group
"""

import csv
import io
import json

from mstrio.api import usergroups
from mstrio.connection import get_connection
from mstrio.users_and_groups import list_users

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
USER_LOGIN = _blank($user_login, None)
OUTPUT_FORMAT = _blank($output_format, None)
# ----------------------------------------------------------------------------


def _find_user_by_login(conn, login):
    """Return the User whose login (username) matches, else None."""
    target = login.strip().lower()
    for user in list_users(conn):
        if (user.username or '').strip().lower() == target:
            return user
    return None


def collect_groups(conn, user):
    """Walk upward from the user's direct parent groups.

    Returns [{name, id, membership}] by name. Direct groups are recorded before
    the walk so one that is also an inherited parent stays Direct.
    """
    found = {g['id']: {'name': g['name'], 'id': g['id'], 'membership': 'Direct'}
             for g in user.memberships or []}
    pending = list(found)
    while pending:
        for parent in usergroups.get_memberships(conn, pending.pop()).json():
            if parent['id'] not in found:
                found[parent['id']] = {'name': parent['name'], 'id': parent['id'],
                                       'membership': 'Inherited'}
                pending.append(parent['id'])
    return sorted(found.values(), key=lambda g: g['name'].lower())


def _to_csv(groups):
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=';', lineterminator='\n')
    writer.writerow(['group_name', 'group_guid', 'membership'])
    writer.writerows((g['name'], g['id'], g['membership']) for g in groups)
    return buffer.getvalue().rstrip('\n')


def main():
    if not USER_LOGIN:
        raise ValueError('user_login variable is required.')
    output_format = (OUTPUT_FORMAT or '').strip().lower()
    if output_format not in ('', 'csv', 'json'):
        raise ValueError(f"output_format must be csv, json or blank, not {OUTPUT_FORMAT!r}.")

    conn = get_connection(connectionData)
    user = _find_user_by_login(conn, USER_LOGIN)
    if user is None:
        raise ValueError(f'No user with login {USER_LOGIN!r}.')

    groups = collect_groups(conn, user)

    if output_format == 'csv':
        print(_to_csv(groups))
    elif output_format == 'json':
        print(json.dumps({'user': {'login': user.username, 'name': user.name, 'id': user.id},
                          'groups': groups}, indent=2))
    else:
        print(f'{user.name} ({user.username})  [{user.id}]')
        direct = sum(g['membership'] == 'Direct' for g in groups)
        print(f'Groups: {len(groups)} ({direct} direct, {len(groups) - direct} inherited)')
        for group in groups:
            print(f'  {group["membership"]:<10} {group["name"]}  [{group["id"]}]')


# Workstation exec()s the script as a string, so a __main__ guard is unreliable
main()
