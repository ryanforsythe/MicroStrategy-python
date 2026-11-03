"""
SubscriptionsCreate.py — Create missing subscriptions from an export.

Three sources:
    Sheet          SubscriptionsExport.py --format excel (or csv), one row per
                   recipient. Filter and edit it; see "Working sheet".
    .json          SubscriptionsExport.py --format json, one object per subscription.
                   Edit that file to change what gets created; see "JSON source".
    .csv / .xlsx   Command Manager subscription output, one row per recipient.
                   Told apart from a sheet by its columns.

Subscription GUIDs differ between environments, so a subscription is identified
by project + name. One that already exists is left alone.

Supported types, Command Manager:  Email, History List. File is reported
                                   `unsupported`, nothing is created.
Supported types, sheet and JSON:   Email, History List, File, FTP, Cache. Shared
                                   Link and Personal View are `unsupported`.

Usage:
    python SubscriptionsCreate.py <env> --input PATH [--sheet NAME] [--apply]
                                  [--concurrency N] [--output-dir PATH]

    python SubscriptionsCreate.py dev --input c:/tmp/subs.csv            # dry run
    python SubscriptionsCreate.py dev --input c:/tmp/subs.xlsx --sheet "Email"
    python SubscriptionsCreate.py prod --input c:/tmp/subs.xlsx --sheet "History List" --apply
    python SubscriptionsCreate.py qa --input c:/tmp/subscriptions_export_dev.xlsx
    python SubscriptionsCreate.py qa --input c:/tmp/subscriptions_export_dev.json
    python SubscriptionsCreate.py qa --input c:/tmp/subscriptions_export_dev.json --subscription-id GUID

    Without --sheet, the first sheet of an Excel file is read.

How each Command Manager column is resolved (all names are case-insensitive):
    Owner, Recipient   full user name, via one list_users() call. User(name=) is
                       not used: it raises when two users share the name. A shared
                       name is reported (`duplicate user name`) with each user's
                       GUID and login, and listed in a separate file so it can be
                       renamed; the user is never guessed.
    Recipient Address  ContactAddress name on the Recipient user (`Recipient Address`
                       is blank for History List). No match, or two matches, means
                       the subscription is not created: a partial recipient list
                       would silently drop someone.
    Content + Folder   the report/document with that name in that folder.
    Schedule           schedule name, including hidden schedules.

Prompts: the input has no answers, so the `prompted` column marks each subscription
as no / optional / required. `required` is not created (status `prompted`);
`optional` is created and runs without answers.

Working sheet: rows with the same subscription_id are one subscription, built from
definition_json with the sheet's edits on top, then handled as the JSON source below.
    target_action      `Create` creates it; anything else, or deleting its rows, skips
                       it. Excel's filter does not: hidden rows are still read.
    Whole subscription subscription_name, project_id, owner_login, schedule_names
                       (`;` between several), delivery_filename, email_subject,
                       expiration, compressed, zip_filename, prompts_json.
                       Rows that disagree on one of these mean it is not created.
    Each row           recipient_login (recipient_name for a group), address_name,
                       recipient_include_type. Copy a row to add a recipient, delete
                       it to remove one.
    Anything else      edit definition_json, on every row of the subscription.
The results file lists the edits found in `changes`, so a dry run confirms them.

JSON source: the entry's own settings are posted as they are (delivery, format,
compression, bursting, refresh conditions, allow* flags); GUIDs are looked up again
on the target. `project_id` in the entry says which project to create in.
    Owner, users       recipients[].login and resolved.owner_login, then the full
                       name if no login is present. The exported GUID is ignored.
    Groups, lists      user group or dynamic recipient list with the recipient's name.
    Recipient address  addressName on that user, any delivery type; physical_address
                       decides between two with the same name.
    Content            resolved.content_locations folder + content name.
    Schedule           schedule name, including hidden schedules.
    Notification       personalNotification addressName among the owner's and the
                       recipients' addresses. Not found: created without notification,
                       and details says so.
    Prompts            prompts[].answers are answered on a content instance, and the
                       instance goes in the POST as personalization.prompt.instanceId.
                       Subscription.answer_prompts() is not used: it rewrites contents
                       with only format_type, which loses bursting and compression.
                       The answers are read back after creating. A prompt with no
                       saved answer uses its default; a required one is not created
                       (status `prompted`).
    Not created        a zip password is required (add delivery.<mode>.zip.password to
                       the JSON to supply it), or the expiration date has passed.

Dry run (the default) resolves everything and creates nothing. With --apply each
subscription is created with sendNow=false: `Send Preview Now` is ignored, so
nothing is delivered until its schedule fires.

Output:
    subscriptions_create_{env}_{YYYYMMDD_HHMM}.csv — one row per subscription.
    subscriptions_create_duplicate_names_{env}_{YYYYMMDD_HHMM}.csv — only when a
    name is shared: full_name, user_guid, login.

REST note:
    Created with a raw POST /api/subscriptions, not EmailSubscription.create.
    mstrio replaces each recipient with the server's entry for that user id, which
    drops addressId, so one user could not be delivered to several addresses.
    Email delivery has no run-fresh or update-cache setting, so `Run Fresh` and
    `Create Update Cache` only apply to History List.
"""

import argparse
import copy
import csv
import io
import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path

from loguru import logger
from mstrio.api import documents, reports, subscriptions
from mstrio.distribution_services import list_subscriptions
from mstrio.distribution_services.subscription.dynamic_recipient_list import (
    list_dynamic_recipient_lists,
)
from mstrio.modeling.prompt import Prompt
from mstrio.object_management import full_search
from mstrio.project_objects import Dashboard, Document, Report
from mstrio.users_and_groups import User, list_user_groups, list_users
from mstrio.utils.helper import is_dashboard

from mstrio_core import (
    MstrConfig,
    MstrRestSession,
    folder_contents,
    folder_path_to_guid,
    read_excel,
    write_csv,
)
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]
DEFAULT_CONCURRENCY = 10

COLUMNS = ["project_id", "subscription_name", "source_guid", "subscription_type",
           "owner", "content", "content_id", "schedule", "recipients", "prompted",
           "new_subscription_id", "status", "details", "changes"]
DUPLICATE_COLUMNS = ["full_name", "user_guid", "login"]

REQUIRED_COLUMNS = ["projectguid", "name", "content", "folder", "schedule",
                    "subscriptiontype", "owner", "recipienttype", "recipient"]
# Rows of one subscription must agree on these; the first row is used if not.
SHARED_COLUMNS = ["content", "folder", "schedule", "subscriptiontype", "owner"]

FORMAT_TYPES = {
    "excel": "EXCEL",
    "pdf": "PDF",
    "html": "HTML",
    "csv": "CSV",
    "plaintext": "PLAIN_TEXT",
}
# (Include Data, Send To History List, Include Link) → delivery.email.sendContentAs
SEND_CONTENT_AS = {
    (True, False, False): "data",
    (True, True, False): "data_and_history_list",
    (True, True, True): "data_and_link_and_history_list",
    (False, True, True): "link_and_history_list",
}
EXPIRATION_FORMATS = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y", "%B %d, %Y"]
CUBE_SUBTYPES = {776, 779}

# delivery.mode → the delivery block of that mode, for the JSON source
DELIVERY_BLOCKS = {"EMAIL": "email", "FILE": "file", "FTP": "ftp",
                   "HISTORY_LIST": "historyList", "CACHE": "cache"}
# Settings copied from an exported subscription; the rest is resolved or read-only
EXPORT_SETTINGS = ["allowDeliveryChanges", "allowPersonalizationChanges", "allowUnsubscribe"]

# ── Input ─────────────────────────────────────────────────────────────────────


def _key(name: str) -> str:
    """Normalize a header: 'Recipient Address' → 'recipientaddress'."""
    return "".join(ch for ch in str(name).lower() if ch.isalnum())


def _read_text(path: Path) -> str:
    raw = path.read_bytes()
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return raw.decode("utf-16")
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("cp1252")


def read_input(path: Path, sheet: str | None = None) -> list[dict]:
    """Rows from a CSV or Excel file, keyed by normalized header, values stripped."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    if path.suffix.lower() in (".xlsx", ".xlsm", ".xls"):
        frame = read_excel(path, sheet=sheet if sheet else 0, dtype=str,
                           keep_default_na=False)
        raw_rows = frame.to_dict(orient="records")
    else:
        text = _read_text(path)
        first = text.splitlines()[0] if text.splitlines() else ""
        delimiter = max("\t,;", key=first.count)
        raw_rows = list(csv.DictReader(io.StringIO(text), delimiter=delimiter))

    rows = [{_key(header): str(value or "").strip() for header, value in raw.items() if header}
            for raw in raw_rows]
    if rows:
        if "definitionjson" in rows[0]:
            return rows
        if "subscriptionid" in rows[0]:
            raise ValueError("This SubscriptionsExport.py output has no definition_json "
                             "column. Export again with --format excel.")
        missing = [c for c in REQUIRED_COLUMNS if c not in rows[0]]
        if missing:
            raise ValueError(f"Input is missing column(s): {', '.join(missing)}")
    return [row for row in rows if row.get("name")]


def group_rows(rows: list[dict]) -> list[list[dict]]:
    """One list of rows per subscription (ProjectGUID + Name), in input order."""
    groups: dict[tuple, list[dict]] = {}
    for row in rows:
        groups.setdefault((row["projectguid"].upper(), row["name"].lower()), []).append(row)
    return list(groups.values())


def _as_bool(row: dict, column: str) -> bool:
    return row.get(column, "").lower() in ("true", "yes", "y", "1")


def _expiration(value: str) -> str | None:
    """A yyyy-MM-dd date from the export's Expiration Date cell, or None when blank."""
    if not value:
        return None
    for fmt in EXPIRATION_FORMATS:
        try:
            parsed = datetime.strptime(value, fmt).date()
        except ValueError:
            continue
        if parsed < date.today():
            raise ValueError(f"expiration date {value!r} has passed")
        return parsed.isoformat()
    raise ValueError(f"unrecognized expiration date {value!r}")


# ── Lookups ───────────────────────────────────────────────────────────────────


def user_index(conn) -> dict[str, list[dict]]:
    """{lowercased full name: [{id, login, name}, ...]} with each user once."""
    index: dict[str, dict[str, dict]] = {}
    for user in list_users(conn, to_dictionary=True):
        name = (user.get("name") or "").strip()
        index.setdefault(name.lower(), {})[user["id"]] = {
            "id": user["id"], "login": user.get("username") or "", "name": name}
    return {name: list(users.values()) for name, users in index.items()}


def schedule_index(conn) -> dict[str, list[str]]:
    """{lowercased schedule name: [id, ...]}, including hidden schedules."""
    index: dict[str, list[str]] = {}
    for schedule in full_search(conn, project=None, domain=4, object_types=51,
                                to_dictionary=True):
        index.setdefault(schedule["name"].strip().lower(), []).append(schedule["id"])
    return index


def fetch_addresses(conn, user_id: str, email_only: bool = True) -> tuple[str, list[dict] | str]:
    """(user id, [{id, name, physical_address}] of its addresses), or an error message."""
    try:
        return user_id, [{"id": a.id, "name": a.name, "physical_address": a.physical_address}
                         for a in User(conn, id=user_id).addresses or []
                         if not email_only or getattr(a.delivery_type, "value", "") == "email"]
    except Exception as exc:  # noqa: BLE001
        return user_id, f"could not read addresses: {exc}"


def resolve_user(name: str, users: dict) -> tuple[dict | None, tuple | None]:
    """(user, None), or (None, problem) when the name matches 0 or 2+ users."""
    matches = users.get(name.lower(), [])
    if len(matches) == 1:
        return matches[0], None
    if not matches:
        return None, ("user not found", f"no user named {name!r}")
    listed = ", ".join(f"{m['id']} ({m['login']})" for m in matches)
    return None, ("duplicate user name", f"{name!r} matches {len(matches)} users: {listed}")


def find_content(session, row: dict, folders: dict) -> tuple[dict | None, tuple | None]:
    """The content object for a row. `folders` caches each folder's listing."""
    path = row["folder"]
    if path not in folders:
        try:
            folders[path] = folder_contents(session, folder_id=folder_path_to_guid(session, path))
        except Exception as exc:  # noqa: BLE001
            folders[path] = str(exc)
    entries = folders[path]
    if isinstance(entries, str):
        return None, ("content not found", f"folder {path!r}: {entries}")

    matches = [e for e in entries if e.get("type") in (3, 55)
               and (e.get("name") or "").strip().lower() == row["content"].lower()]
    if len(matches) == 1:
        return matches[0], None
    if not matches:
        return None, ("content not found", f"{row['content']!r} not in {path!r}")
    return None, ("duplicate content", f"{len(matches)} objects named {row['content']!r} "
                                       f"in {path!r}")


def content_type(entry: dict) -> str:
    """The REST content type of a folder entry."""
    if entry["type"] == 3:
        return "cube" if entry.get("subtype") in CUBE_SUBTYPES else "report"
    return "dossier" if is_dashboard(entry.get("viewMedia", 0)) else "document"


def prompt_state(conn, entry: dict, kind: str, project_id: str) -> str:
    """no / optional / required for a content object."""
    if kind == "cube":
        return "no"
    if kind == "report":
        prompts = reports.get_report_prompts(conn, entry["id"], closed=False).json()
    else:
        prompts = documents.get_prompts(conn, entry["id"], project_id=project_id,
                                        closed=False).json()
    if not prompts:
        return "no"
    return "required" if any(p.get("required") for p in prompts) else "optional"


# ── Body ──────────────────────────────────────────────────────────────────────


def recipients_for(rows: list[dict], users: dict, addresses: dict) -> tuple[list[dict], list[tuple]]:
    """(recipient dicts, problems). A repeated user + address is kept once."""
    recipients, seen, problems = [], set(), []
    for row in rows:
        if row.get("recipienttype", "").upper() != "USER":
            problems.append(("unsupported", f"recipient type {row.get('recipienttype')!r}"))
            continue
        user, problem = resolve_user(row["recipient"], users)
        if problem:
            problems.append(problem)
            continue

        wanted = row.get("recipientaddress", "")
        recipient = {"id": user["id"], "type": "user", "includeType": "TO"}
        if wanted:
            held = addresses.get(user["id"], [])
            if isinstance(held, str):
                problems.append(("error", held))
                continue
            matches = [a for a in held if (a["name"] or "").strip().lower() == wanted.lower()]
            if len(matches) != 1:
                status = "address not found" if not matches else "duplicate address name"
                problems.append((status, f"{len(matches)} email address(es) named {wanted!r} "
                                         f"on {row['recipient']}"))
                continue
            recipient["addressId"] = matches[0]["id"]

        key = (recipient["id"], recipient.get("addressId"))
        if key not in seen:
            seen.add(key)
            recipients.append(recipient)
    return recipients, problems


def email_delivery(row: dict) -> dict:
    send_as = (_as_bool(row, "includedata"), _as_bool(row, "sendtohistorylist"),
               _as_bool(row, "includelink"))
    if send_as not in SEND_CONTENT_AS:
        raise ValueError("no send-as option for Include Data/Send To History List/"
                         f"Include Link = {send_as}")
    email = {"subject": row.get("subject") or row["name"],
             "filename": row.get("filename") or row["name"],
             "sendContentAs": SEND_CONTENT_AS[send_as]}
    if row.get("message"):
        email["message"] = row["message"]
    if _as_bool(row, "compresscontent"):
        email["zip"] = {"filename": row.get("zipfilename") or email["filename"]}
    return {"mode": "EMAIL", "email": email}


def history_list_delivery(row: dict) -> dict:
    return {"mode": "HISTORY_LIST", "historyList": {
        "overwriteOlderVersion": _as_bool(row, "overwriteolderversion"),
        "doNotCreateUpdateCaches": not _as_bool(row, "createupdatecache"),
        "reRunHl": _as_bool(row, "runfresh"),
    }}


def build_body(row: dict, content: dict, kind: str, schedule_id: str, owner_id: str,
               recipients: list[dict], is_email: bool) -> dict:
    """The POST /api/subscriptions body. Raises ValueError on an unusable value."""
    delivery = email_delivery(row) if is_email else history_list_delivery(row)
    delivery["contactSecurity"] = True
    expiration = _expiration(row.get("expirationdate", ""))
    if expiration:
        delivery["expiration"] = expiration

    body_content = {"id": content["id"], "type": kind}
    if is_email:
        format_name = row.get("deliveryformat", "")
        format_type = FORMAT_TYPES.get(_key(format_name))
        if not format_type:
            raise ValueError(f"unrecognized Delivery Format {format_name!r}")
        body_content["personalization"] = {"formatMode": "DEFAULT", "formatType": format_type}

    can_modify = _as_bool(row, "modificationbyrecipients")
    return {
        "multipleContents": False,
        "name": row["name"],
        "allowDeliveryChanges": can_modify,
        "allowPersonalizationChanges": can_modify,
        "sendNow": False,
        "owner": {"id": owner_id},
        "schedules": [{"id": schedule_id}],
        "contents": [body_content],
        "recipients": recipients,
        "delivery": delivery,
    }


# ── One subscription ──────────────────────────────────────────────────────────


def process(session, group: list[dict], existing: set[str], users: dict, schedules: dict,
            addresses: dict, folders: dict, do_apply: bool) -> list:
    """The result row for one subscription."""
    row = group[0]
    project_id = row["projectguid"]
    conn = session.mstrio_conn
    sub_type = _key(row["subscriptiontype"])
    found = {"content_id": "", "recipients": len(group), "prompted": "", "new_id": ""}

    def result(status: str, details: str = "") -> list:
        return [project_id, row["name"], row.get("guid", ""), row["subscriptiontype"],
                row["owner"], row["content"], found["content_id"], row["schedule"],
                found["recipients"], found["prompted"], found["new_id"], status, details]

    if row["name"].lower() in existing:
        return result("exists", "already on target; not changed")
    if sub_type not in ("email", "historylist"):
        return result("unsupported", f"subscription type {row['subscriptiontype']!r}")

    guids = {r.get("guid", "") for r in group}
    if len(guids) > 1:
        return result("error", f"{len(guids)} source subscriptions share this name in the "
                               "project; ProjectGUID + Name cannot tell them apart")
    if _as_bool(row, "segmentedsubscription"):
        return result("unsupported", "segmented subscription")

    # Keep going after a problem, so one dry run shows everything to fix.
    problems: list[tuple] = []

    owner, problem = resolve_user(row["owner"], users)
    if problem:
        problems.append(problem)

    schedule_ids = schedules.get(row["schedule"].lower(), [])
    if len(schedule_ids) != 1:
        problems.append(("schedule not found" if not schedule_ids else "duplicate schedule",
                         f"{len(schedule_ids)} schedule(s) named {row['schedule']!r}"))

    recipients, recipient_problems = recipients_for(group, users, addresses)
    problems += recipient_problems
    found["recipients"] = len(recipients)

    content, problem = find_content(session, row, folders)
    if problem:
        problems.append(problem)
    kind = ""
    if content:
        found["content_id"] = content["id"]
        kind = content_type(content)
        try:
            found["prompted"] = prompt_state(conn, content, kind, project_id)
        except Exception as exc:  # noqa: BLE001
            found["prompted"] = "unknown"
            problems.append(("error", f"could not read prompts: {exc}"))
        if found["prompted"] == "required":
            problems.append(("prompted", "required prompts, and the input has no answers"))

    notes = [f"first row used; rows differ on {c}" for c in SHARED_COLUMNS
             if len({r.get(c, "") for r in group}) > 1]

    if not problems:
        try:
            body = build_body(row, content, kind, schedule_ids[0], owner["id"], recipients,
                              sub_type == "email")
        except ValueError as exc:
            problems.append(("error", str(exc)))

    if problems:
        return result(problems[0][0], "; ".join(p[1] for p in problems + [("", n) for n in notes]))
    if not do_apply:
        return result("dry run", "; ".join(notes))

    response = session.post("/subscriptions", json=body)
    logger.debug("POST subscriptions {name}: {status}", name=row["name"],
                 status=response.status_code)
    if not response.ok:
        return result("error", f"HTTP {response.status_code}: {response.text[:300]}")

    created = response.json()
    found["new_id"] = created.get("id", "")
    existing.add(row["name"].lower())
    saved = {(r.get("id"), r.get("addressId")) for r in created.get("recipients") or []}
    missing = [r for r in recipients if "addressId" in r and (r["id"], r["addressId"]) not in saved]
    if missing:
        notes.append(f"{len(missing)} address(es) not in the server's response")
        return result("created, check recipients", "; ".join(notes))
    logger.info("Created {name} ({id})", name=row["name"], id=found["new_id"])
    return result("created", "; ".join(notes))


# ── Working sheet ─────────────────────────────────────────────────────────────


def _one_value(rows: list[dict], column: str) -> str:
    """The value all rows of a subscription share. Raises ValueError if they differ."""
    values = {row.get(column, "") for row in rows}
    if len(values) > 1:
        raise ValueError(f"rows disagree on {column}")
    return values.pop()


def apply_compression(sub: dict, block: dict, rows: list[dict], changes: list[str]) -> None:
    """Apply the compressed + zip_filename columns."""
    was = bool(block.get("zip")) or any((c.get("personalization") or {}).get("compressed")
                                        for c in sub["contents"])
    old_name = (block.get("zip") or {}).get("filename", "")
    value = _one_value(rows, "compressed").lower()
    now = value in ("true", "yes", "y", "1") if value else was
    name = _one_value(rows, "zipfilename")
    if now == was and (not now or name == old_name):
        return
    changes.append(f"compressed: {was} → {now}" if now != was
                   else f"zip filename: {old_name} → {name}")
    for content in sub["contents"]:
        content.setdefault("personalization", {})["compressed"] = now
    if now:
        block.setdefault("zip", {})["filename"] = (name or block.get("filename") or sub["name"])
    else:
        block.pop("zip", None)


def apply_recipients(sub: dict, rows: list[dict], changes: list[str]) -> None:
    """Rebuild sub["recipients"] from the rows: each row keeps its exported recipient,
    found by recipient_id + address_id, with the row's edits on top."""
    originals = {(r.get("id", ""), r.get("addressId", "")): r
                 for r in sub.get("recipients") or []}
    recipients, seen, used = [], set(), set()
    for row in rows:
        key = (row.get("recipientid", ""), row.get("addressid", ""))
        login, name = row.get("recipientlogin", ""), row.get("recipientname", "")
        if not (key[0] or login or name):
            continue  # a subscription with no recipients
        base = originals.get(key)
        used.add(key)
        recipient = dict(base or {"type": row.get("recipienttype") or "user"})
        recipient["includeType"] = (row.get("recipientincludetype")
                                    or recipient.get("includeType", "TO"))
        if recipient["type"] == "user":
            if base is None:
                changes.append(f"added recipient {login or name}")
            elif login != base.get("login", ""):
                changes.append(f"recipient: {base.get('login', '')} → {login}")
                recipient.pop("physical_address", None)
            recipient["login"] = login
        elif base is None or name != base.get("name", ""):
            changes.append(f"recipient: {(base or {}).get('name', '')} → {name}")
        recipient["name"] = name or recipient.get("name", "")

        address = row.get("addressname", "")
        if address != recipient.get("addressName", ""):
            changes.append(f"address of {login or name}: "
                           f"{recipient.get('addressName', '')} → {address}")
            recipient["addressName"] = address
            recipient.pop("physical_address", None)
            if address:
                recipient["addressId"] = "from sheet"  # resolved by name on the target
            else:
                recipient.pop("addressId", None)

        identity = (recipient.get("login") or recipient["name"].lower(),
                    recipient.get("addressName", ""))
        if identity not in seen:
            seen.add(identity)
            recipients.append(recipient)

    changes += [f"removed recipient {r.get('login') or r.get('name')}"
                for key, r in originals.items() if key not in used]
    sub["recipients"] = recipients


def apply_sheet_edits(sub: dict, rows: list[dict]) -> list[str]:
    """Apply a subscription's rows to its definition; returns the changes found.
    Raises ValueError on an unusable edit."""
    changes: list[str] = []

    def edit(column: str, label: str, old: str, assign) -> None:
        new = _one_value(rows, column)
        if new != (old or ""):
            changes.append(f"{label}: {old} → {new}")
            assign(new)

    resolved = sub.setdefault("resolved", {})
    delivery = sub["delivery"]
    block = delivery.get(DELIVERY_BLOCKS.get(delivery.get("mode"), "")) or {}

    edit("subscriptionname", "name", sub["name"], lambda v: sub.update(name=v))
    edit("projectid", "project", sub["project_id"], lambda v: sub.update(project_id=v))
    edit("ownerlogin", "owner", resolved.get("owner_login", ""),
         lambda v: resolved.update(owner_login=v))
    edit("deliveryfilename", "filename", block.get("filename", ""),
         lambda v: block.update(filename=v))
    edit("emailsubject", "subject", block.get("subject", ""), lambda v: block.update(subject=v))

    def set_expiration(value: str) -> None:
        delivery.pop("expiration", None)
        if value:
            delivery["expiration"] = _expiration(value)

    edit("expiration", "expiration", delivery.get("expiration", ""), set_expiration)

    old_schedules = [s.get("name", "") for s in sub.get("schedules") or []]
    new_schedules = [n.strip() for n in _one_value(rows, "schedulenames").split(";") if n.strip()]
    if new_schedules != old_schedules:
        changes.append(f"schedule: {'; '.join(old_schedules)} → {'; '.join(new_schedules)}")
        sub["schedules"] = [{"name": name} for name in new_schedules]

    apply_compression(sub, block, rows, changes)
    apply_recipients(sub, rows, changes)

    prompts = _one_value(rows, "promptsjson")
    if prompts.startswith("ERROR"):
        raise ValueError(prompts)
    if prompts:
        sub["prompts"] = json.loads(prompts)
    return changes


def subs_from_sheet(rows: list[dict]) -> list[dict]:
    """One definition per subscription_id, edits applied. `sheet_status` holds a
    (status, details) when the subscription is skipped or its rows are unusable."""
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        if row.get("subscriptionid"):
            grouped.setdefault(row["subscriptionid"], []).append(row)

    subs = []
    for sub_id, group in grouped.items():
        actions = {r.get("targetaction", "").lower() for r in group}
        sub = {"id": sub_id, "project_id": group[0].get("projectid", ""),
               "name": group[0].get("subscriptionname", ""), "contents": [], "delivery": {}}
        try:
            sub = json.loads(_one_value(group, "definitionjson"))
            if actions == {"create"}:
                sub["sheet_changes"] = "; ".join(apply_sheet_edits(sub, group))
            elif "create" in actions:
                sub["sheet_status"] = ("error", "rows disagree on target_action")
            else:
                sub["sheet_status"] = ("skipped", f"target_action {group[0].get('targetaction')!r}")
        except ValueError as exc:
            sub["sheet_status"] = ("error", str(exc))
        subs.append(sub)
    return subs


# ── JSON source ───────────────────────────────────────────────────────────────


def read_export(path: Path) -> list[dict]:
    """The subscriptions of a SubscriptionsExport.py --format json file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    subs = [data] if isinstance(data, dict) else data
    for sub in subs:
        missing = [k for k in ("project_id", "name", "contents", "delivery") if k not in sub]
        if missing:
            raise ValueError(f"{sub.get('name', '?')!r} is missing: {', '.join(missing)}")
    return subs


def _has_answer(prompt: dict) -> bool:
    return prompt.get("answers") not in (None, "", [], {})


def _is_prompted(sub: dict) -> bool:
    return any(((c.get("personalization") or {}).get("prompt") or {}).get("enabled")
               for c in sub["contents"])


def export_user(login: str, name: str, lookups: dict) -> tuple[dict | None, tuple | None]:
    """Resolve by login; by full name only when the export has no login."""
    if not login:
        return resolve_user(name, lookups["users"])
    user = lookups["logins"].get(login.lower())
    return (user, None) if user else (None, ("user not found", f"no user with login {login!r}"))


def export_address(user_id: str, rec: dict, addresses: dict) -> tuple[str, tuple | None]:
    """(target address id, problem) for a recipient's addressName."""
    held = addresses.get(user_id, [])
    if isinstance(held, str):
        return "", ("error", held)
    wanted = rec.get("addressName", "")
    matches = [a for a in held if (a["name"] or "").strip().lower() == wanted.strip().lower()]
    if len(matches) > 1:
        matches = [a for a in matches
                   if a["physical_address"] == rec.get("physical_address")] or matches
    if len(matches) == 1:
        return matches[0]["id"], None
    status = "address not found" if not matches else "duplicate address name"
    return "", (status, f"{len(matches)} address(es) named {wanted!r} on {rec.get('name')}")


def export_recipients(sub: dict, lookups: dict) -> tuple[list[dict], list[tuple]]:
    """(recipient dicts with target GUIDs, problems)."""
    recipients, problems = [], []
    for rec in sub.get("recipients") or []:
        kind, name = rec.get("type", ""), rec.get("name", "")
        recipient = {"type": kind, "includeType": rec.get("includeType", "TO")}
        if kind == "user":
            user, problem = export_user(rec.get("login", ""), name, lookups)
            if problem:
                problems.append(problem)
                continue
            recipient["id"] = user["id"]
            if rec.get("addressId"):
                recipient["addressId"], problem = export_address(user["id"], rec,
                                                                 lookups["addresses"])
                if problem:
                    problems.append(problem)
                    continue
        elif kind in ("user_group", "dynamic_recipient_list"):
            index = lookups["groups" if kind == "user_group" else "lists"]
            ids = index.get(name.strip().lower(), [])
            if len(ids) != 1:
                problems.append(("recipient not found" if not ids else "duplicate recipient",
                                 f"{len(ids)} {kind} named {name!r}"))
                continue
            recipient["id"] = ids[0]
        else:
            problems.append(("unsupported", f"recipient type {kind!r}"))
            continue
        recipients.append(recipient)
    return recipients, problems


def export_delivery(sub: dict, recipients: list[dict], owner: dict | None,
                    addresses: dict) -> tuple[dict, str]:
    """(delivery for the POST, note). Raises ValueError on an unusable value."""
    delivery = copy.deepcopy(sub["delivery"])
    block = delivery.get(DELIVERY_BLOCKS[delivery["mode"]]) or {}
    zip_settings = block.get("zip") or {}
    if zip_settings.get("passwordProtect") and not zip_settings.get("password"):
        raise ValueError(f"no password in delivery.{DELIVERY_BLOCKS[delivery['mode']]}.zip")
    _expiration(delivery.get("expiration") or "")

    notification = delivery.get("personalNotification")
    if not notification:
        return delivery, ""
    wanted = (notification.get("addressName") or "").strip().lower()
    user_ids = {r["id"] for r in recipients if r["type"] == "user"} | ({owner["id"]} if owner else set())
    found = {a["id"] for uid in user_ids if isinstance(addresses.get(uid), list)
             for a in addresses[uid] if (a["name"] or "").strip().lower() == wanted}
    if len(found) == 1:
        delivery["personalNotification"] = {"addressId": found.pop()}
        return delivery, ""
    delivery.pop("personalNotification")
    delivery.pop("notificationEnabled", None)
    return delivery, f"{len(found)} notification address(es) named {wanted!r}; notification left off"


def export_content(sub: dict, session, folders: dict) -> tuple[list[dict], list[tuple]]:
    """([{id, type, source}] on the target, problems), matching on folder + name."""
    locations = (sub.get("resolved") or {}).get("content_locations") or {}
    found, problems = [], []
    for source in sub["contents"]:
        path = locations.get(source["id"], "")
        if not path:
            problems.append(("content not found", f"no folder for {source.get('name')!r}"))
            continue
        entry, problem = find_content(session, {"folder": path, "content": source.get("name", "")},
                                      folders)
        if problem:
            problems.append(problem)
        else:
            found.append({"id": entry["id"], "type": content_type(entry), "source": source})
    return found, problems


def answered_instance(conn, content: dict, prompts: list[dict]) -> str:
    """Answer the prompts on an instance of the content; its id goes in the POST."""
    # By keyword: the second positional is id on Report, but name on Document
    source_class = {"report": Report, "document": Document, "dossier": Dashboard}[content["type"]]
    source = source_class(conn, id=content["id"])
    answers = [Prompt(connection=conn, id=p.get("id"), key=p.get("key"), type=p.get("type"),
                      answers=p.get("answers") if _has_answer(p) else None,
                      use_default=not _has_answer(p)) for p in prompts]
    if not source.answer_prompts(answers, force=True):
        raise ValueError("the content did not ask for prompts on the target")
    return source.instance_id


def export_body(sub: dict, contents: list[dict], schedule_ids: list[str], owner_id: str,
                recipients: list[dict], delivery: dict, instance_id: str | None) -> dict:
    """The POST /api/subscriptions body."""
    body_contents = []
    for content in contents:
        entry = {"id": content["id"], "type": content["type"]}
        for key in ("personalization", "refreshCondition"):
            if key in content["source"]:
                entry[key] = copy.deepcopy(content["source"][key])
        if instance_id:
            entry["personalization"]["prompt"] = {"enabled": True, "instanceId": instance_id}
        body_contents.append(entry)
    return {
        "multipleContents": len(body_contents) > 1,
        "name": sub["name"],
        **{key: sub[key] for key in EXPORT_SETTINGS if key in sub},
        "sendNow": False,
        "owner": {"id": owner_id},
        "schedules": [{"id": schedule_id} for schedule_id in schedule_ids],
        "contents": body_contents,
        "recipients": recipients,
        "delivery": delivery,
    }


def changed_prompts(conn, sub_id: str, project_id: str, prompts: list[dict]) -> list[str]:
    """Titles of the prompts whose saved answers differ from the export."""
    saved = subscriptions.get_subscription_prompts(
        conn, subscription_id=sub_id, project_id=project_id).json().get("prompts") or []
    answers = {p.get("id"): p.get("answers") for p in saved}
    return [p.get("title", p.get("id")) for p in prompts
            if _has_answer(p) and answers.get(p.get("id")) != p.get("answers")]


def process_export(session, sub: dict, existing: set[str], lookups: dict, folders: dict,
                   shared_name: bool, do_apply: bool) -> list:
    """The result row for one exported subscription."""
    project_id = sub["project_id"]
    mode = (sub["delivery"].get("mode") or "").upper()
    resolved = sub.get("resolved") or {}
    owner_name = (sub.get("owner") or {}).get("name", "")
    prompts = sub.get("prompts") or []
    found = {"content_id": "", "recipients": len(sub.get("recipients") or []), "new_id": "",
             "prompted": (f"{sum(_has_answer(p) for p in prompts)}/{len(prompts)} answered"
                          if prompts else "no")}

    def result(status: str, details: str = "") -> list:
        return [project_id, sub["name"], sub.get("id", ""), mode, owner_name,
                "; ".join(c.get("name", "") for c in sub["contents"]), found["content_id"],
                "; ".join(s.get("name", "") for s in sub.get("schedules") or []),
                found["recipients"], found["prompted"], found["new_id"], status, details,
                sub.get("sheet_changes", "")]

    if sub.get("sheet_status"):
        return result(*sub["sheet_status"])
    conn = session.mstrio_conn
    if sub["name"].strip().lower() in existing:
        return result("exists", "already on target; not changed")
    if mode not in DELIVERY_BLOCKS:
        return result("unsupported", f"delivery mode {mode!r}")
    if shared_name:
        return result("error", "several subscriptions share this name in the project; "
                               "project + name cannot tell them apart")

    # Keep going after a problem, so one dry run shows everything to fix.
    problems: list[tuple] = []

    owner, problem = export_user(resolved.get("owner_login", ""), owner_name, lookups)
    if problem:
        problems.append(problem)

    schedule_ids = []
    for schedule in sub.get("schedules") or []:
        ids = lookups["schedules"].get(schedule.get("name", "").strip().lower(), [])
        if len(ids) == 1:
            schedule_ids.append(ids[0])
        else:
            problems.append(("schedule not found" if not ids else "duplicate schedule",
                             f"{len(ids)} schedule(s) named {schedule.get('name')!r}"))

    recipients, recipient_problems = export_recipients(sub, lookups)
    problems += recipient_problems
    found["recipients"] = len(recipients)

    contents, content_problems = export_content(sub, session, folders)
    problems += content_problems
    found["content_id"] = "; ".join(c["id"] for c in contents)

    if _is_prompted(sub):
        unanswered = [p for p in prompts if p.get("required") and not _has_answer(p)]
        if not prompts:
            problems.append(("prompted", "prompted, and the export has no prompts"))
        elif unanswered:
            problems.append(("prompted", f"{len(unanswered)} required prompt(s) have no "
                                         "saved answer"))
        if len(sub["contents"]) > 1 or any(c["type"] not in ("report", "document", "dossier")
                                           for c in contents):
            problems.append(("unsupported", "prompts on multiple contents or a cube"))

    notes = []
    try:
        delivery, note = export_delivery(sub, recipients, owner, lookups["addresses"])
        notes += [note] if note else []
    except ValueError as exc:
        problems.append(("not created", str(exc)))

    if problems:
        return result(problems[0][0], "; ".join(p[1] for p in problems + [("", n) for n in notes]))
    if not do_apply:
        return result("dry run", "; ".join(notes))

    instance_id = None
    if prompts:
        try:
            instance_id = answered_instance(conn, contents[0], prompts)
        except Exception as exc:  # noqa: BLE001
            return result("prompted", f"could not answer prompts, not created: {exc}")

    body = export_body(sub, contents, schedule_ids, owner["id"], recipients, delivery, instance_id)
    response = session.post("/subscriptions", json=body)
    logger.debug("POST subscriptions {name}: {status}", name=sub["name"],
                 status=response.status_code)
    if not response.ok:
        return result("error", f"HTTP {response.status_code}: {response.text[:300]}")

    created = response.json()
    found["new_id"] = created.get("id", "")
    existing.add(sub["name"].strip().lower())
    saved = {(r.get("id"), r.get("addressId")) for r in created.get("recipients") or []}
    missing = [r for r in recipients if "addressId" in r and (r["id"], r["addressId"]) not in saved]
    if missing:
        notes.append(f"{len(missing)} address(es) not in the server's response")
        return result("created, check recipients", "; ".join(notes))
    if prompts:
        try:
            changed = changed_prompts(conn, found["new_id"], project_id, prompts)
        except Exception as exc:  # noqa: BLE001
            changed = [f"could not read them back: {exc}"]
        if changed:
            notes.append(f"answers differ: {', '.join(changed)}")
            return result("created, check prompts", "; ".join(notes))
    logger.info("Created {name} ({id})", name=sub["name"], id=found["new_id"])
    return result("created", "; ".join(notes))


def run_export(session, subs: list[dict], users: dict, schedules: dict, concurrency: int,
               do_apply: bool) -> list[list]:
    """Process a sheet or JSON source, one project at a time."""
    conn = session.mstrio_conn
    results = [process_export(session, s, set(), {}, {}, False, False)
               for s in subs if s.get("sheet_status")]
    subs = [s for s in subs if not s.get("sheet_status")]
    groups: dict[str, list[str]] = {}
    for group in list_user_groups(conn, to_dictionary=True):
        groups.setdefault((group.get("name") or "").strip().lower(), []).append(group["id"])
    lookups = {"users": users, "schedules": schedules, "groups": groups, "lists": {},
               "logins": {u["login"].lower(): u for matches in users.values()
                          for u in matches if u["login"]}}

    address_users = set()
    for sub in subs:
        people = [((sub.get("resolved") or {}).get("owner_login", ""),
                   (sub.get("owner") or {}).get("name", ""))]
        people += [(r.get("login", ""), r.get("name", "")) for r in sub.get("recipients") or []
                   if r.get("type") == "user"]
        for login, name in people:
            user, _ = export_user(login, name, lookups)
            if user:
                address_users.add(user["id"])
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        lookups["addresses"] = dict(pool.map(
            lambda uid: fetch_addresses(conn, uid, email_only=False), address_users))

    name_counts = Counter((s["project_id"], s["name"].strip().lower()) for s in subs)
    for project_id in dict.fromkeys(s["project_id"] for s in subs):
        project_subs = [s for s in subs if s["project_id"] == project_id]
        try:
            session.set_project(project_id=project_id)
            existing = {(s.get("name") or "").strip().lower() for s in
                        list_subscriptions(conn, project_id=project_id, to_dictionary=True)}
            lookups["lists"] = {}
            if any(r.get("type") == "dynamic_recipient_list" for s in project_subs
                   for r in s.get("recipients") or []):
                for entry in list_dynamic_recipient_lists(conn, project_id=project_id,
                                                          to_dictionary=True):
                    lookups["lists"].setdefault((entry.get("name") or "").strip().lower(),
                                                []).append(entry["id"])
        except Exception as exc:  # noqa: BLE001
            logger.error("Project {pid}: {exc}", pid=project_id, exc=exc)
            results += [[project_id, s["name"], s.get("id", ""), s["delivery"].get("mode", ""),
                         (s.get("owner") or {}).get("name", ""), "", "", "", "", "", "", "error",
                         f"project unavailable: {exc}"] for s in project_subs]
            continue

        folders: dict = {}
        for sub in project_subs:
            shared = name_counts[(project_id, sub["name"].strip().lower())] > 1
            try:
                results.append(process_export(session, sub, existing, lookups, folders, shared,
                                              do_apply))
            except Exception as exc:  # noqa: BLE001
                logger.error("{name}: {exc}", name=sub["name"], exc=exc)
                results.append([project_id, sub["name"], sub.get("id", ""),
                                sub["delivery"].get("mode", ""),
                                (sub.get("owner") or {}).get("name", ""), "", "", "", "", "", "",
                                "error", str(exc)])
    return results


# ── Main ──────────────────────────────────────────────────────────────────────


def run_rows(session, groups: list[list[dict]], users: dict, schedules: dict, concurrency: int,
             do_apply: bool) -> list[list]:
    """Process a Command Manager source, one project at a time."""
    conn = session.mstrio_conn
    results: list[list] = []
    address_users = {users[r["recipient"].lower()][0]["id"]
                     for group in groups for r in group
                     if r.get("recipientaddress")
                     and len(users.get(r["recipient"].lower(), [])) == 1}
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        addresses = dict(pool.map(lambda uid: fetch_addresses(conn, uid), address_users))

    for project_id in dict.fromkeys(g[0]["projectguid"] for g in groups):
        project_groups = [g for g in groups if g[0]["projectguid"] == project_id]
        try:
            session.set_project(project_id=project_id)
            existing = {(s.get("name") or "").strip().lower() for s in
                        list_subscriptions(conn, project_id=project_id, to_dictionary=True)}
        except Exception as exc:  # noqa: BLE001
            logger.error("Project {pid}: {exc}", pid=project_id, exc=exc)
            results += [[project_id, g[0]["name"], g[0].get("guid", ""),
                         g[0]["subscriptiontype"], g[0]["owner"], g[0]["content"], "",
                         g[0]["schedule"], len(g), "", "", "error",
                         f"project unavailable: {exc}"] for g in project_groups]
            continue

        folders: dict = {}
        for group in project_groups:
            try:
                results.append(process(session, group, existing, users, schedules,
                                       addresses, folders, do_apply))
            except Exception as exc:  # noqa: BLE001
                logger.error("{name}: {exc}", name=group[0]["name"], exc=exc)
                results.append([project_id, group[0]["name"], group[0].get("guid", ""),
                                group[0]["subscriptiontype"], group[0]["owner"],
                                group[0]["content"], "", group[0]["schedule"], len(group),
                                "", "", "error", str(exc)])
    return results


def main(env: str, input_path: Path, sheet: str | None = None, do_apply: bool = False,
         output_dir: Path | None = None, concurrency: int = DEFAULT_CONCURRENCY,
         subscription_ids: list[str] | None = None) -> list[list]:
    config = MstrConfig(environment=MstrEnvironment(env))
    is_json = Path(input_path).suffix.lower() == ".json"
    rows = [] if is_json else read_input(input_path, sheet)
    is_sheet = bool(rows) and "definitionjson" in rows[0]
    if subscription_ids and not (is_json or is_sheet):
        raise ValueError("--subscription-id does not apply to Command Manager input.")
    groups = group_rows(rows) if rows and not is_sheet else []
    subs = read_export(input_path) if is_json else subs_from_sheet(rows) if is_sheet else []
    if subscription_ids:
        wanted = {guid.upper() for guid in subscription_ids}
        subs = [s for s in subs if str(s.get("id", "")).upper() in wanted]
    if not groups and not subs:
        raise ValueError(f"No usable rows in {input_path} — check the column names.")
    logger.info("{n} subscription(s) from {path}; {mode}", n=len(subs or groups), path=input_path,
                mode="APPLY" if do_apply else "DRY RUN — nothing will be changed")

    with MstrRestSession(config) as session:
        conn = session.mstrio_conn
        users = user_index(conn)
        schedules = schedule_index(conn)
        logger.info("Indexed {u} user name(s), {s} schedule name(s).", u=len(users),
                    s=len(schedules))
        if subs:
            results = run_export(session, subs, users, schedules, concurrency, do_apply)
        else:
            results = run_rows(session, groups, users, schedules, concurrency, do_apply)

    counts: dict[str, int] = {}
    for result in results:
        counts[result[11]] = counts.get(result[11], 0) + 1
    logger.info("Result: " + ", ".join(f"{status}={n}" for status, n in sorted(counts.items())))

    stamp = f"{datetime.now():%Y%m%d_%H%M}"
    out_dir = Path(output_dir or config.output_dir)
    out_path = out_dir / f"subscriptions_create_{env}_{stamp}.csv"
    results = [result + [""] * (len(COLUMNS) - len(result)) for result in results]
    write_csv(results, columns=COLUMNS, path=out_path)

    # Every shared name the input uses, so they can all be renamed in one pass.
    used = {name.lower() for group in groups for r in group
            for name in (r["owner"], r["recipient"])}
    duplicates = [[u["name"], u["id"], u["login"]] for name in sorted(used)
                  for u in users.get(name, []) if len(users.get(name, [])) > 1]
    if duplicates:
        dup_path = out_dir / f"subscriptions_create_duplicate_names_{env}_{stamp}.csv"
        write_csv(duplicates, columns=DUPLICATE_COLUMNS, path=dup_path)
        logger.warning("{n} user(s) share a name → {path}", n=len(duplicates), path=dup_path)

    logger.success("Wrote {n} result row(s) → {path}", n=len(results), path=out_path)
    return results


# ── CLI ───────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create missing subscriptions from an edited SubscriptionsExport.py sheet "
                    "or JSON, or a Command Manager export.")
    parser.add_argument("env", choices=ENVS, help="Environment to create them on.")
    parser.add_argument("--input", type=Path, required=True, metavar="PATH",
                        help="SubscriptionsExport.py sheet (--format excel/csv) or --format "
                             "json output, or CSV/Excel Command Manager output.")
    parser.add_argument("--sheet", default=None, metavar="NAME",
                        help="Excel sheet name (default: the first sheet).")
    parser.add_argument("--subscription-id", nargs="+", default=None, metavar="GUID",
                        help="Sheet or JSON input: create just these source subscriptions.")
    parser.add_argument("--apply", action="store_true",
                        help="Create the subscriptions (default: dry run).")
    parser.add_argument("--output-dir", type=Path, default=None, metavar="PATH",
                        help="Output directory (default: MSTR_OUTPUT_DIR or c:/tmp).")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                        metavar="N", help=f"Parallel user address reads "
                                          f"(default: {DEFAULT_CONCURRENCY}).")

    args = parser.parse_args()
    main(env=args.env, input_path=args.input, sheet=args.sheet, do_apply=args.apply,
         output_dir=args.output_dir, concurrency=args.concurrency,
         subscription_ids=args.subscription_id)
