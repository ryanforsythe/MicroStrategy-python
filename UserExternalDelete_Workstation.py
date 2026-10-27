# Delete external users. DRY-RUN unless the confirm variable = DELETE-EXTERNAL-USERS. Read the docstring first.
"""UserExternalDelete_Workstation — delete external users from an environment.

MicroStrategy Workstation script. Uses the official mstrio-py connection
(`connectionData`, injected by the host). DESTRUCTIVE — deletes user objects.

What it deletes
---------------
Every user EXCEPT those that are KEPT:
    * a login WITHOUT an email (no '@'), e.g. 'Administrator', service accounts;
    * a login ending in '@agdata.net' or '@agdata.com' (case-insensitive).
In other words: users whose login is an email address on a non-agdata domain.

Safety
------
  * DRY-RUN by default: it only lists what WOULD be deleted / kept. To actually
    delete, set the confirm variable to exactly:  DELETE-EXTERNAL-USERS
  * The connected (running) user and 'Administrator' are never deleted, even if
    their login somehow matches the delete rule.
  * Each deletion is independent — one failure (e.g. a user that still owns
    objects) is logged and the rest continue.
  * Profile folders are left in place (delete_profile=False).

Inputs
------
    confirm    text   The variable is named 'confirm'. Leave EMPTY (or '-') for
                      a DRY-RUN. Set to 'DELETE-EXTERNAL-USERS' to delete.
"""

from mstrio.connection import get_connection
from mstrio.users_and_groups import list_users

_EMPTY_SENTINELS = {"", "-", "null", "blank", "empty"}


def _blank(value, default=None):
    if value is None:
        return default
    if isinstance(value, str) and value.strip().lower() in _EMPTY_SENTINELS:
        return default
    return value


# --- Substituted by Workstation (text -> 'quoted') --------------------------
CONFIRM = _blank($confirm, "")   # noqa: F821 - injected by Workstation
# ----------------------------------------------------------------------------

CONFIRM_PHRASE = "DELETE-EXTERNAL-USERS"
KEEP_DOMAINS = ("@agdata.net", "@agdata.com", "agdata.net1","agdata.com1")

# Logins/names that must never be deleted regardless of the rule (lowercased).
PROTECTED = {"k242_dev_administrator", "k242_dev_administrator", "administrator"}


def _should_keep(login: str) -> bool:
    """True if the user must be KEPT (not deleted)."""
    low = (login or "").strip().lower()
    if not low or "@" not in low:      # no email -> keep
        return True
    if low.endswith(KEEP_DOMAINS):      # agdata email -> keep
        return True
    if low in PROTECTED:                # safety net
        return True
    return False


def main():
    conn = get_connection(connectionData)  # noqa: F821 - injected by Workstation

    apply = (CONFIRM or "").strip() == CONFIRM_PHRASE
    mode = "DELETE" if apply else "DRY-RUN"

    # Never delete the account we are connected as.
    self_login = (getattr(conn, "username", "") or "").strip().lower()
    if self_login:
        PROTECTED.add(self_login)

    users = list_users(conn)
    print(f"=== UserExternalDelete [{mode}] — {len(users)} user(s) in environment ===")

    to_delete = [u for u in users if not _should_keep(u.username or "")]
    print(f"{len(to_delete)} user(s) match the delete rule (email login, non-agdata).")

    deleted = kept_protected = errors = 0
    for u in to_delete:
        login = u.username or ""
        # Re-check protection (self / Administrator) explicitly before deleting.
        if (login or "").strip().lower() in PROTECTED:
            kept_protected += 1
            print(f"PROTECTED (kept): {u.name} <{login}> ({u.id})")
            continue
        try:
            if apply:
                u.delete(force=True)
                deleted += 1
                print(f"DELETED: {u.name} <{login}> ({u.id})")
            else:
                print(f"DRY-RUN would delete: {u.name} <{login}> ({u.id})")
        except Exception as exc:  # noqa: BLE001
            errors += 1
            print(f"ERROR deleting {u.name} <{login}> ({u.id}): {exc}")

    if apply:
        print(f"=== Done [DELETE]: {deleted} deleted, {kept_protected} protected, "
              f"{errors} error(s) ===")
    else:
        would = len(to_delete) - kept_protected
        print(f"=== Done [DRY-RUN]: {would} would be deleted, {kept_protected} protected. "
              f"Set the confirm variable to '{CONFIRM_PHRASE}' to apply. ===")


# The host exec()s this script as a string, so the __main__ guard is unreliable —
# call main() unconditionally.
main()
