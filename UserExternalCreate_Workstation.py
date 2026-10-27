# Create (or group-add) an external, trusted-auth user. Fill the variables below.
"""UserExternalCreate_Workstation — create an external user, or add an existing
one to a user group.

MicroStrategy Workstation script. Uses the official mstrio-py connection
(`connectionData`, injected by the host). Runtime inputs follow the Workstation
`$`-variable pattern (text -> quoted string on substitution). Define these in
the variable panel.

Inputs
------
    $name              text   Full name of the user (required)
    $email             text   Email = login = trusted-auth id (required)
    $user_group_name   text   Target user group NAME    (provide this OR GUID)
    $user_group_guid   text   Target user group GUID    (provide this OR name)
    $description       text   Optional description

Workstation requires every variable to have a value. To mark an optional /
not-applicable variable as "unset", enter EMPTY or any of these placeholder
tokens (case-insensitive): '-', 'null', 'blank', 'empty'. Supply exactly one of
$user_group_name / $user_group_guid (GUID wins if both are given).

Behavior
--------
  * If a user with this email (login) already exists:
      - already in the target group -> no change;
      - not in the target group     -> added to the target group.
    (Existing users are not otherwise modified.)
  * Otherwise a new user is created with:
      - password  = random 16-character strong password (never used);
      - password_modifiable = False  (user cannot change password);
      - standard_auth       = False  (standard authentication disabled);
      - trust_id            = email  (Trusted Authentication request user id);
      - password_auto_expire / require_new_password = False (required so the
        server accepts password_modifiable=False);
      - membership          = the target group.
  * If creation collides with an existing identity (same trust_id under a
    different username, e.g. a merged/duplicate account), that account is found
    and added to the target group instead of creating a duplicate.
"""

import re
import secrets
import string

from mstrio.connection import get_connection
from mstrio.users_and_groups import User, UserGroup, list_users

# Workstation "unset" tokens — an optional variable set to any of these collapses
# to the coded default.
_EMPTY_SENTINELS = {"", "-", "null", "blank", "empty"}


def _blank(value, default=None):
    """Return `default` when `value` is unset (None / empty / placeholder)."""
    if value is None:
        return default
    if isinstance(value, str) and value.strip().lower() in _EMPTY_SENTINELS:
        return default
    return value


# --- Substituted by Workstation (text -> 'quoted') --------------------------
NAME = _blank($name)                       # noqa: F821 - injected by Workstation
EMAIL = _blank($email)                     # noqa: F821
USER_GROUP_NAME = _blank($user_group_name)  # noqa: F821
USER_GROUP_GUID = _blank($user_group_guid)  # noqa: F821
DESCRIPTION = _blank($description, "")      # noqa: F821
# ----------------------------------------------------------------------------

PASSWORD_LENGTH = 16
_COLLISION_ID_RE = re.compile(r"with ID ([0-9A-Fa-f]{32})")


def _random_password(length=PASSWORD_LENGTH):
    """A cryptographically strong password (never used — standard_auth is off)."""
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*()-_=+"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _resolve_group(conn, name, guid):
    """Resolve the target user group by GUID (preferred) or by name."""
    if guid:
        group = UserGroup(conn, id=str(guid).strip())
    elif name:
        group = UserGroup(conn, name=str(name).strip())
    else:
        raise ValueError("Provide either $user_group_name or $user_group_guid.")
    # Touch id/name to force the lookup so a bad name/GUID fails fast.
    if not group.id:
        raise ValueError("Could not resolve the specified user group.")
    return group


def _find_user_by_login(conn, email):
    """Return the existing User whose login (username) matches email, else None."""
    target = (email or "").strip().lower()
    for u in list_users(conn):
        if (u.username or "").strip().lower() == target:
            return u
    return None


def _member_group_ids(user):
    ids = set()
    for m in user.memberships or []:
        mid = m.get("id") if isinstance(m, dict) else getattr(m, "id", None)
        if mid:
            ids.add(mid)
    return ids


def main():
    if not EMAIL:
        raise ValueError("$email is required.")
    if not NAME:
        raise ValueError("$name is required.")

    conn = get_connection(connectionData)  # noqa: F821 - injected by Workstation

    group = _resolve_group(conn, USER_GROUP_NAME, USER_GROUP_GUID)
    print(f"Target group: {group.name} ({group.id})")

    existing = _find_user_by_login(conn, EMAIL)
    if existing is not None:
        user = User(conn, id=existing.id)
        if group.id in _member_group_ids(user):
            print(f"User already exists and is already in the group — no change: "
                  f"{user.name} ({user.id})")
        else:
            UserGroup(conn, id=group.id).add_users([user])
            print(f"User exists — added to group '{group.name}': {user.name} ({user.id})")
        return

    # ---- Create a new external, trusted-auth user ----
    try:
        user = User.create(
            connection=conn,
            username=EMAIL,
            full_name=NAME,
            password=_random_password(),
            description=(DESCRIPTION or None),
            enabled=True,
            standard_auth=False,
            trust_id=EMAIL,
            password_modifiable=False,
            password_auto_expire=False,
            require_new_password=False,
            memberships=[group.id],
        )
        print(f"Created external user: {user.name} ({user.id}) in group '{group.name}'")
    except Exception as exc:  # noqa: BLE001
        # The identity (trust_id) may already exist under a different username.
        m = _COLLISION_ID_RE.search(str(exc))
        if not m:
            raise
        user = User(conn, id=m.group(1))
        if group.id not in _member_group_ids(user):
            UserGroup(conn, id=group.id).add_users([user])
            print(f"User identity already existed ({user.id}) — added to group "
                  f"'{group.name}'.")
        else:
            print(f"User identity already existed ({user.id}) and is already in the "
                  f"group — no change.")


# The host exec()s this script as a string, so the __main__ guard is unreliable —
# call main() unconditionally.
main()
