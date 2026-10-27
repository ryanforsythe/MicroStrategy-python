# Normalize user default configuration. DRY-RUN unless the apply flag is true.
"""UserDefaultConfiguration_Workstation — check / enforce default user settings.

MicroStrategy Workstation script. Uses the official mstrio-py connection
(`connectionData`, injected by the host). Each fix is an independent flag, so
you run only the processes you want. DRY-RUN by default.

Scope — the user_type variable
------------------------------
    Internal : login contains an agdata domain — '@agdata.com' or '@agdata.net'
               ('.com1' / '.net1' merge variants count as the same domain).
    External : login is an email address on a NON-agdata domain.
    All      : every user (email and non-email logins).

Processes (each a flag; run only the enabled ones)
--------------------------------------------------
    set_trusted_auth            : if the login is an email and the Trusted
                                  Authentication id is empty, set it to the login.
    disable_password_expiration : set password auto-expire OFF.
    disable_must_change_password: set "user must change password at next logon" OFF.
    ensure_email_address        : if the login is an email and the user has no
                                  default email address, add one (login as the
                                  address, device 'Generic Email - Altus').

Inputs
------
    user_type                    text  Internal | External | All  (default All)
    set_trusted_auth             bool  default True
    disable_password_expiration  bool  default True
    disable_must_change_password bool  default True
    ensure_email_address         bool  default True
    apply                        bool  default False (dry-run: report only)
    concurrency                  num   parallel workers (default 10)

Workstation "unset" tokens (case-insensitive) for optional variables:
    '', '-', 'null', 'blank', 'empty'  -> the coded default applies.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed

from mstrio.connection import get_connection
from mstrio.users_and_groups import User, list_users

# Default email device ("Generic Email - Altus") used when adding a missing address.
GENERIC_EMAIL_DEVICE_ID = "4F549AE14DD30180A9C7A8AF57E6D08D"
AGDATA_MARKERS = ("@agdata.com", "@agdata.net")  # substring match also covers .com1/.net1

_EMPTY_SENTINELS = {"", "-", "null", "blank", "empty"}


def _blank(value, default=None):
    if value is None:
        return default
    if isinstance(value, str) and value.strip().lower() in _EMPTY_SENTINELS:
        return default
    return value


def _as_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in ("true", "1", "yes", "y")


# --- Substituted by Workstation (text -> 'quoted') --------------------------
USER_TYPE = _blank($user_type, "All")                        # noqa: F821
SET_TRUSTED_AUTH = _as_bool($set_trusted_auth, True)         # noqa: F821
DISABLE_PWD_EXPIRATION = _as_bool($disable_password_expiration, True)   # noqa: F821
DISABLE_MUST_CHANGE = _as_bool($disable_must_change_password, True)     # noqa: F821
ENSURE_EMAIL_ADDRESS = _as_bool($ensure_email_address, True) # noqa: F821
APPLY = _as_bool($apply, False)                              # noqa: F821
CONCURRENCY = _blank($concurrency, 10)                       # noqa: F821
# ----------------------------------------------------------------------------


def _is_email(login: str) -> bool:
    return "@" in (login or "")


def _is_internal(login: str) -> bool:
    low = (login or "").strip().lower()
    return any(marker in low for marker in AGDATA_MARKERS)


def _in_scope(login: str, user_type: str) -> bool:
    ut = (user_type or "All").strip().lower()
    if ut == "all":
        return True
    if ut == "internal":
        return _is_internal(login)
    if ut == "external":
        return _is_email(login) and not _is_internal(login)
    raise ValueError(f"Invalid user_type '{user_type}' — use Internal, External, or All.")


def _process_user(conn, uid, login, apply):
    """Evaluate (and, if apply, perform) the enabled fixes for one user."""
    result = {"login": login, "actions": [], "status": "", "details": ""}
    try:
        user = User(conn, id=uid)
        is_email = _is_email(login)

        alter_kwargs = {}
        if SET_TRUSTED_AUTH and is_email and not (getattr(user, "trust_id", None) or ""):
            alter_kwargs["trust_id"] = login
            result["actions"].append("trust_id=login")
        if DISABLE_PWD_EXPIRATION and getattr(user, "password_auto_expire", None) is not False:
            alter_kwargs["password_auto_expire"] = False
            result["actions"].append("password_auto_expire=False")
        if DISABLE_MUST_CHANGE and getattr(user, "require_new_password", None) is not False:
            alter_kwargs["require_new_password"] = False
            result["actions"].append("require_new_password=False")

        needs_email = (ENSURE_EMAIL_ADDRESS and is_email
                       and not getattr(user, "default_email_address", None))
        if needs_email:
            result["actions"].append("+default_email_address")

        if not result["actions"]:
            result["status"] = "ok (no change)"
            return result

        if apply:
            if alter_kwargs:
                user.alter(**alter_kwargs)
            if needs_email:
                user.add_address(name="Default Email", address=login, default=True,
                                 delivery_type="email", device_id=GENERIC_EMAIL_DEVICE_ID)
            result["status"] = "updated"
        else:
            result["status"] = "dry-run"
    except Exception as exc:  # noqa: BLE001
        result["status"] = "error"
        result["details"] = str(exc)
    return result


def main():
    conn = get_connection(connectionData)  # noqa: F821 - injected by Workstation

    try:
        concurrency = max(1, int(CONCURRENCY))
    except (TypeError, ValueError):
        concurrency = 10

    mode = "APPLY" if APPLY else "DRY-RUN"
    enabled = [name for name, on in (
        ("trusted_auth", SET_TRUSTED_AUTH),
        ("disable_password_expiration", DISABLE_PWD_EXPIRATION),
        ("disable_must_change_password", DISABLE_MUST_CHANGE),
        ("ensure_email_address", ENSURE_EMAIL_ADDRESS),
    ) if on]

    users = list_users(conn)
    in_scope = [u for u in users if _in_scope(u.username or "", USER_TYPE)]
    print(f"=== UserDefaultConfiguration [{mode}] — user_type={USER_TYPE} ===")
    print(f"Processes: {', '.join(enabled) or '(none)'}")
    print(f"{len(in_scope)} of {len(users)} user(s) in scope.")
    if not enabled:
        print("No processes enabled — nothing to do.")
        return

    updated = unchanged = errors = 0
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(_process_user, conn, u.id, u.username or "", APPLY): u
                   for u in in_scope}
        for fut in as_completed(futures):
            res = fut.result()
            if res["status"] == "error":
                errors += 1
                print(f"ERROR {res['login']}: {res['details']}")
            elif res["actions"]:
                updated += 1
                print(f"{res['status']:8} {res['login']}: {', '.join(res['actions'])}")
            else:
                unchanged += 1

    print(f"=== Done [{mode}]: {updated} with changes, {unchanged} already ok, "
          f"{errors} error(s) ===")
    if not APPLY:
        print("Set the apply flag to true to commit these changes.")


# The host exec()s this script as a string, so the __main__ guard is unreliable —
# call main() unconditionally.
main()
