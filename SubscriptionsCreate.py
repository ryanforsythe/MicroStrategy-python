"""
SubscriptionsCreate.py — Create missing subscriptions from a Command Manager export.

Input is the Command Manager subscription output, as CSV or Excel, with one row
per recipient. Subscription GUIDs differ between environments, so a subscription
is identified by ProjectGUID + Name. One that already exists is left alone.

Supported types:  Email, History List.
Not built yet:    File — reported as `unsupported`, nothing is created.

Usage:
    python SubscriptionsCreate.py <env> --input PATH [--sheet NAME] [--apply]
                                  [--concurrency N] [--output-dir PATH]

    python SubscriptionsCreate.py dev --input c:/tmp/subs.csv            # dry run
    python SubscriptionsCreate.py dev --input c:/tmp/subs.xlsx --sheet "Email"
    python SubscriptionsCreate.py prod --input c:/tmp/subs.xlsx --sheet "History List" --apply

    Without --sheet, the first sheet of an Excel file is read.

How each column is resolved (all names are case-insensitive):
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
import csv
import io
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path

from loguru import logger
from mstrio.api import documents, reports
from mstrio.distribution_services import list_subscriptions
from mstrio.object_management import full_search
from mstrio.users_and_groups import User, list_users
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
           "new_subscription_id", "status", "details"]
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
    rows = [row for row in rows if row.get("name")]
    if rows:
        missing = [c for c in REQUIRED_COLUMNS if c not in rows[0]]
        if missing:
            raise ValueError(f"Input is missing column(s): {', '.join(missing)}")
    return rows


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


def fetch_addresses(conn, user_id: str) -> tuple[str, list[dict] | str]:
    """(user id, [{id, name}] of email addresses), or an error message."""
    try:
        return user_id, [{"id": a.id, "name": a.name} for a in User(conn, id=user_id).addresses or []
                         if getattr(a.delivery_type, "value", "") == "email"]
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


# ── Main ──────────────────────────────────────────────────────────────────────


def main(env: str, input_path: Path, sheet: str | None = None, do_apply: bool = False,
         output_dir: Path | None = None, concurrency: int = DEFAULT_CONCURRENCY) -> list[list]:
    config = MstrConfig(environment=MstrEnvironment(env))
    groups = group_rows(read_input(input_path, sheet))
    if not groups:
        raise ValueError(f"No usable rows in {input_path} — check the column names.")
    logger.info("{n} subscription(s) from {path}; {mode}", n=len(groups), path=input_path,
                mode="APPLY" if do_apply else "DRY RUN — nothing will be changed")

    results: list[list] = []
    with MstrRestSession(config) as session:
        conn = session.mstrio_conn
        users = user_index(conn)
        schedules = schedule_index(conn)
        logger.info("Indexed {u} user name(s), {s} schedule name(s).", u=len(users),
                    s=len(schedules))

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

    counts: dict[str, int] = {}
    for result in results:
        counts[result[11]] = counts.get(result[11], 0) + 1
    logger.info("Result: " + ", ".join(f"{status}={n}" for status, n in sorted(counts.items())))

    stamp = f"{datetime.now():%Y%m%d_%H%M}"
    out_dir = Path(output_dir or config.output_dir)
    out_path = out_dir / f"subscriptions_create_{env}_{stamp}.csv"
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
        description="Create missing subscriptions from a Command Manager export.")
    parser.add_argument("env", choices=ENVS, help="Environment to create them on.")
    parser.add_argument("--input", type=Path, required=True, metavar="PATH",
                        help="CSV or Excel Command Manager output.")
    parser.add_argument("--sheet", default=None, metavar="NAME",
                        help="Excel sheet name (default: the first sheet).")
    parser.add_argument("--apply", action="store_true",
                        help="Create the subscriptions (default: dry run).")
    parser.add_argument("--output-dir", type=Path, default=None, metavar="PATH",
                        help="Output directory (default: MSTR_OUTPUT_DIR or c:/tmp).")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                        metavar="N", help=f"Parallel user address reads "
                                          f"(default: {DEFAULT_CONCURRENCY}).")

    args = parser.parse_args()
    main(env=args.env, input_path=args.input, sheet=args.sheet, do_apply=args.apply,
         output_dir=args.output_dir, concurrency=args.concurrency)
