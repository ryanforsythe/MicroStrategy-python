"""
UserUpdate.py — Update MicroStrategy users from a CSV or Excel file.

Updatable fields (a blank cell means "leave this field alone"):
    login                  → User.username
    name                   → User.full_name (what UsersExport writes as `name`)
    status                 → User.enabled      (Enabled/Disabled, true/false, 1/0)
    email                  → the user's DEFAULT email address
    standard_auth_enabled  → User.standard_auth (true/false)
    trusted_auth           → User.trust_id
    description            → User.description

Matching:
    * `guid` column present and filled → match on GUID. A `login` column in that
      row is then a TARGET value, so this is how a login is renamed.
    * otherwise → match on the existing `login` (case-insensitive, resolved once
      from list_users). The login is the key, so it isn't changed — unless the
      row also has `new_login`, which always wins as the target login.
    A login matching more than one user, or matching none, is reported, not guessed.

Accepted column names (case-insensitive, spaces/underscores ignored):
    guid                  guid, user_guid, user_id, object_id
    login                 login, username, user_login, id
    new login             new_login, new_username
    name                  name, full_name, display_name
    status                status, enabled
    email                 email, email_address, default_email_address
    standard auth         standard_auth_enabled, standard_auth
    trusted auth          trusted_auth, trust_id
    description           description

Usage:
    python UserUpdate.py <env> --input PATH [--apply] [--concurrency N]
                               [--output-dir PATH]

    python UserUpdate.py dev  --input c:/tmp/users.xlsx          # dry run
    python UserUpdate.py prod --input c:/tmp/users.csv --apply

Dry run (the default) reports the changes it would make and touches nothing.
With --apply each user is altered, then re-read and every intended field is
verified, so a row only says "updated" when the server agrees.

Output: user_update_{env}_{YYYYMMDD_HHMM}.csv — one row per field change, plus
one row per user that needed none, with before/after values and a status.

Email note:
    `alter(default_email_address=...)` only edits an address that is ALREADY the
    user's default. A user with no default email address needs `add_address(...,
    default=True, delivery_type='email', device_id=GENERIC_EMAIL_DEVICE_ID)`
    instead — the same rule UserSync.py follows.
"""

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

from loguru import logger
from mstrio.users_and_groups import User, list_users

from mstrio_core import MstrConfig, get_mstrio_connection, read_excel, write_csv
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]
DEFAULT_CONCURRENCY = 10
# "Generic Email - Altus" — the device a new default email address is attached to.
GENERIC_EMAIL_DEVICE_ID = "4F549AE14DD30180A9C7A8AF57E6D08D"

COLUMNS = ["row", "matched_by", "user_guid", "login", "field", "old_value",
           "new_value", "status", "details"]

# Input column aliases → canonical field name.
ALIASES = {
    "guid": "guid", "userguid": "guid", "userid": "guid", "objectid": "guid",
    "login": "login", "username": "login", "userlogin": "login", "id": "login",
    "newlogin": "new_login", "newusername": "new_login",
    "name": "name", "fullname": "name", "displayname": "name",
    "status": "status", "enabled": "status",
    "email": "email", "emailaddress": "email", "defaultemailaddress": "email",
    "standardauthenabled": "standard_auth", "standardauth": "standard_auth",
    "trustedauth": "trusted_auth", "trustid": "trusted_auth",
    "description": "description",
}
# Fields that go straight to User.alter(), in the order they're reported.
ALTER_FIELDS = {
    "login": "username",
    "name": "full_name",
    "status": "enabled",
    "standard_auth": "standard_auth",
    "trusted_auth": "trust_id",
    "description": "description",
}

# ── Input ─────────────────────────────────────────────────────────────────────


def _key(name: str) -> str:
    """Normalize a header: 'Standard Auth Enabled' → 'standardauthenabled'."""
    return "".join(ch for ch in str(name).lower() if ch.isalnum())


def read_input(path: Path) -> list[dict]:
    """Rows from a CSV or Excel file, keyed by canonical field name.

    Unknown columns are ignored; blank cells are dropped so they read as
    "leave this field alone" rather than "set it to empty".
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    if path.suffix.lower() in (".xlsx", ".xlsm", ".xls"):
        frame = read_excel(path)
        raw_rows = frame.to_dict(orient="records")
    else:
        text = path.read_text(encoding="utf-8-sig")
        first = text.splitlines()[0] if text.splitlines() else ""
        delimiter = ";" if first.count(";") > first.count(",") else ","
        raw_rows = list(csv.DictReader(text.splitlines(), delimiter=delimiter))

    rows = []
    for number, raw in enumerate(raw_rows, start=2):      # row 1 is the header
        row = {"row": number}
        for header, value in (raw or {}).items():
            field = ALIASES.get(_key(header))
            if not field:
                continue
            text_value = "" if value is None else str(value).strip()
            if text_value and text_value.lower() != "nan":
                row[field] = text_value
        if len(row) > 1:
            rows.append(row)
    return rows


def _as_bool(value, field: str) -> bool:
    text = str(value).strip().lower()
    if text in ("true", "yes", "y", "1", "enabled", "enable"):
        return True
    if text in ("false", "no", "n", "0", "disabled", "disable"):
        return False
    raise ValueError(f"{field} must be true/false (or Enabled/Disabled), not {value!r}")


# ── Users ─────────────────────────────────────────────────────────────────────


def login_index(conn) -> dict[str, list[str]]:
    """{lowercased login: [user guid, ...]} for the whole environment.

    A list, not a single id, so an ambiguous login is reported instead of
    silently updating whichever user came back first.
    """
    index: dict[str, list[str]] = {}
    for user in list_users(conn):
        login = (getattr(user, "username", "") or "").strip().lower()
        if login:
            index.setdefault(login, []).append(user.id)
    return index


def _addresses(user) -> list[dict]:
    """The user's addresses as plain dicts (mstrio returns objects or dicts)."""
    result = []
    for address in getattr(user, "addresses", None) or []:
        if isinstance(address, dict):
            result.append(address)
            continue
        delivery = getattr(address, "delivery_type", "")
        result.append({
            "id": getattr(address, "id", None),
            "name": getattr(address, "name", None),
            "physicalAddress": getattr(address, "physical_address", None),
            "deliveryType": str(getattr(delivery, "value", delivery) or ""),
            "isDefault": bool(getattr(address, "is_default", False)),
        })
    return result


def _email_addresses(user) -> list[dict]:
    return [a for a in _addresses(user)
            if "email" in str(a.get("deliveryType", "")).lower()]


def current_email(user) -> str:
    """The user's default email address, or "" when there isn't one."""
    emails = _email_addresses(user)
    default = next((a for a in emails if a.get("isDefault")), None)
    return (default or {}).get("physicalAddress") or ""


def current_value(user, field: str):
    """The user's current value for a canonical field name."""
    if field == "email":
        return current_email(user)
    if field == "status":
        return user.enabled
    if field == "login":
        return user.username
    if field == "name":
        return user.full_name
    if field == "standard_auth":
        return user.standard_auth
    if field == "trusted_auth":
        return user.trust_id
    return user.description


def wanted_value(row: dict, field: str):
    """The row's target value for a field, or None when it isn't being set."""
    if field == "login":
        # With a GUID match, `login` is a target value; otherwise it's the key
        # and only `new_login` renames.
        source = row.get("new_login") or (row.get("login") if row.get("guid") else None)
        return source
    raw = row.get(field)
    if raw is None:
        return None
    if field in ("status", "standard_auth"):
        return _as_bool(raw, field)
    return raw


def _same(old, new) -> bool:
    if isinstance(new, bool) or isinstance(old, bool):
        return bool(old) == bool(new)
    return str(old or "").strip() == str(new or "").strip()


def plan(user, row: dict) -> list[dict]:
    """[{field, old, new}] for every field this row actually changes."""
    changes = []
    for field in list(ALTER_FIELDS) + ["email"]:
        new = wanted_value(row, field)
        if new is None:
            continue
        old = current_value(user, field)
        if not _same(old, new):
            changes.append({"field": field, "old": old, "new": new})
    return changes


# ── Applying ──────────────────────────────────────────────────────────────────


def set_email(user, email: str) -> str:
    """Point the user's default email address at `email`; return what was done.

    alter(default_email_address=) edits an existing default address only, so a
    user without one needs add_address().
    """
    emails = _email_addresses(user)
    if any(a.get("isDefault") for a in emails):
        user.alter(default_email_address=email)
        return "altered the existing default address"
    user.add_address(name="Email", address=email, default=True,
                     delivery_type="email", device_id=GENERIC_EMAIL_DEVICE_ID)
    return "added a default email address (no default existed)"


def apply_changes(user, changes: list[dict]) -> str:
    """Apply the planned changes; return a note about how."""
    kwargs = {ALTER_FIELDS[c["field"]]: c["new"] for c in changes
              if c["field"] in ALTER_FIELDS}
    notes = []
    if kwargs:
        user.alter(**kwargs)
        notes.append("altered " + ", ".join(sorted(kwargs)))
    email = next((c["new"] for c in changes if c["field"] == "email"), None)
    if email is not None:
        notes.append(set_email(user, email))
    return "; ".join(notes)


def verify(conn, user_id: str, changes: list[dict]) -> dict[str, bool]:
    """Re-read the user and check each intended field actually changed."""
    fresh = User(conn, id=user_id)
    return {c["field"]: _same(current_value(fresh, c["field"]), c["new"])
            for c in changes}


# ── One row ───────────────────────────────────────────────────────────────────


def process(conn, row: dict, index: dict[str, list[str]], do_apply: bool) -> list[list]:
    """Result rows for one input row."""
    def result(field="", old="", new="", status="", details="",
               guid="", login="", matched_by=""):
        return [row["row"], matched_by, guid, login, field, old, new, status, details]

    guid, login = row.get("guid"), row.get("login")
    try:
        if guid:
            matched_by, user_id = "guid", guid
        elif login:
            matches = index.get(login.strip().lower(), [])
            if not matches:
                return [result(status="error", details=f"login {login!r} not found",
                               login=login)]
            if len(matches) > 1:
                return [result(status="error", login=login,
                               details=f"login {login!r} matches {len(matches)} users: "
                                       f"{', '.join(matches)} — use the guid column")]
            matched_by, user_id = "login", matches[0]
        else:
            return [result(status="error", details="row has neither guid nor login")]

        user = User(conn, id=user_id)
        current_login = user.username
    except Exception as exc:  # noqa: BLE001
        return [result(status="error", guid=guid or "", login=login or "",
                       matched_by=locals().get("matched_by", ""),
                       details=f"could not read the user: {exc}")]

    # A rename must not collide with another user's login.
    new_login = wanted_value(row, "login")
    if new_login and not _same(current_login, new_login):
        clash = [u for u in index.get(str(new_login).strip().lower(), []) if u != user_id]
        if clash:
            return [result(status="error", guid=user_id, login=current_login,
                           matched_by=matched_by, field="login", old=current_login,
                           new=new_login,
                           details=f"login {new_login!r} already belongs to {clash[0]}")]

    try:
        changes = plan(user, row)
    except ValueError as exc:                       # bad status / standard_auth value
        return [result(status="error", guid=user_id, login=current_login,
                       matched_by=matched_by, details=str(exc))]

    if not changes:
        return [result(status="no change", guid=user_id, login=current_login,
                       matched_by=matched_by)]

    if not do_apply:
        return [result(field=c["field"], old=c["old"], new=c["new"], status="dry run",
                       guid=user_id, login=current_login, matched_by=matched_by)
                for c in changes]

    try:
        note = apply_changes(user, changes)
        verified = verify(conn, user_id, changes)
    except Exception as exc:  # noqa: BLE001
        return [result(field=c["field"], old=c["old"], new=c["new"], status="error",
                       guid=user_id, login=current_login, matched_by=matched_by,
                       details=str(exc)) for c in changes]

    return [result(field=c["field"], old=c["old"], new=c["new"],
                   status="updated" if verified.get(c["field"]) else "not applied",
                   guid=user_id, login=current_login, matched_by=matched_by,
                   details=note if verified.get(c["field"])
                   else "the server did not report the new value")
            for c in changes]


# ── Main ──────────────────────────────────────────────────────────────────────


def main(env: str, input_path: Path, do_apply: bool = False,
         output_dir: Path | None = None,
         concurrency: int = DEFAULT_CONCURRENCY) -> list[list]:
    config = MstrConfig(environment=MstrEnvironment(env))
    rows = read_input(input_path)
    if not rows:
        raise ValueError(f"No usable rows in {input_path} — check the column names.")
    logger.info("{n} row(s) from {path}; {mode}", n=len(rows), path=input_path,
                mode="APPLY" if do_apply else "DRY RUN — nothing will be changed")

    conn = get_mstrio_connection(config=config)
    try:
        needs_index = any(not r.get("guid") or wanted_value(r, "login") for r in rows)
        index = login_index(conn) if needs_index else {}
        if index:
            logger.info("Indexed {n} login(s) in the environment.", n=len(index))

        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
            results = [r for group in pool.map(
                lambda row: process(conn, row, index, do_apply), rows) for r in group]
    finally:
        conn.close()

    counts: dict[str, int] = {}
    for result in results:
        counts[result[7]] = counts.get(result[7], 0) + 1
    logger.info("Result: " + ", ".join(f"{status}={n}" for status, n in sorted(counts.items())))
    for result in [r for r in results if r[7] in ("error", "not applied")]:
        logger.error("  row {row}: {login} {field} — {details}", row=result[0],
                     login=result[3] or result[2], field=result[4], details=result[8])

    out_dir = output_dir or config.output_dir
    out_path = Path(out_dir) / f"user_update_{env}_{datetime.now():%Y%m%d_%H%M}.csv"
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    write_csv(results, columns=COLUMNS, path=out_path)
    logger.success("Wrote {n} result row(s) → {path}", n=len(results), path=out_path)
    return results


# ── CLI ───────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update MicroStrategy users from a CSV or Excel file.")
    parser.add_argument("env", choices=ENVS, help="Environment to update.")
    parser.add_argument("--input", type=Path, required=True, metavar="PATH",
                        help="CSV or Excel file of users and target values.")
    parser.add_argument("--apply", action="store_true",
                        help="Make the changes (default: dry run).")
    parser.add_argument("--output-dir", type=Path, default=None, metavar="PATH",
                        help="Output directory (default: MSTR_OUTPUT_DIR or c:/tmp).")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                        metavar="N", help=f"Parallel users (default: {DEFAULT_CONCURRENCY}).")

    args = parser.parse_args()
    main(env=args.env, input_path=args.input, do_apply=args.apply,
         output_dir=args.output_dir, concurrency=args.concurrency)
