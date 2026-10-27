# List the bookmarks saved on Library items in a project (read-only).
"""LibraryBookmarks_Workstation — report Library bookmarks in a project.

MicroStrategy Workstation script. Uses the injected `connectionData` session.
Nothing is changed.

Where bookmarks live
--------------------
A bookmark is saved on a Library shortcut: the per-user object created when a
dashboard or document is added to a user's Library. Every user who has the item
in their Library has their own shortcut with their own bookmarks, so the report
is one row per bookmark per shortcut.

How bookmarks are read
----------------------
Each shortcut's bookmarks come from GET /api/shortcuts/{id}/bookmarks, the call
behind mstrio.api.bookmarks.get_bookmarks_from_shortcut. It is made through the
mstrio connection directly, so a failed shortcut is counted instead of printing
mstrio's error block. Observed on this environment (2026-09-15):
  * A shortcut in the running user's Library returns 200 with its bookmarks
    ([] when there are none).
  * Shortcuts outside the running user's Library, including ones they own,
    returned ERR002 "Catastrophic failure".
  * get_shortcuts(..., shortcut_info_flag=Bookmark) never included bookmarks,
    even on a shortcut that had one, so it is only used for target names.
A bookmark carries name, id, version, creationTime, lastUpdateTime and
lastViewTime. It has no owner field; the Library user is the shortcut's owner.

Scope
-----
  Current user  Library(conn).list_library_shortcuts(), limited to the project
                (it returns the user's items from every project).
  All users     full_search for every Library shortcut in the project (hidden
                included), then the same call for each. Other users' bookmarks
                may not be readable: if the first OTHER_USER_PROBE shortcuts
                owned by other users all fail, the rest of theirs are skipped
                and the summary says so.

Inputs
------
    project_name   text  Project to report on (required)
    scope          text  'Current user' (default) or 'All users'
    target_name    text  Only items whose name contains this text (optional)

Output: one line per bookmark, a summary, then the rows as JSON (also available
from get_results()).

Workstation "unset" tokens (case-insensitive): '', '-', 'null', 'blank', 'empty'.
"""

import json
from collections import Counter

from mstrio.connection import get_connection
from mstrio.object_management import full_search
from mstrio.object_management.library_shortcut import get_shortcuts
from mstrio.project_objects.library import Library
from mstrio.types import ObjectSubTypes

BATCH_SIZE = 50          # shortcuts per get_shortcuts request (target names)
OTHER_USER_PROBE = 25    # stop trying other users' shortcuts if this many all fail
PROGRESS_EVERY = 200     # print a progress line after this many shortcuts

_EMPTY_SENTINELS = {"", "-", "null", "blank", "empty"}


def _blank(value, default=None):
    if value is None:
        return default
    if isinstance(value, str) and value.strip().lower() in _EMPTY_SENTINELS:
        return default
    return value


# --- Substituted by Workstation (text -> 'quoted') --------------------------
PROJECT_NAME = _blank($project_name)             # noqa: F821
SCOPE = _blank($scope, "Current user")           # noqa: F821
TARGET_NAME = _blank($target_name)               # noqa: F821
# ----------------------------------------------------------------------------


def _all_users_scope(scope):
    """True for 'All users', False for 'Current user'; anything else stops."""
    key = "".join(ch for ch in str(scope).lower() if ch.isalpha())
    if key in ("currentuser", "current", "me"):
        return False
    if key in ("allusers", "all"):
        return True
    raise ValueError(f"scope must be 'Current user' or 'All users', not {scope!r}.")


def _name(value):
    """A name from a {'id', 'name'} reference, or the value itself."""
    return value.get("name") if isinstance(value, dict) else value


def _id(value):
    return value.get("id") if isinstance(value, dict) else None


# --- Finding shortcuts ----------------------------------------------------------


def current_user_shortcuts(conn, project_id):
    """Shortcut records for the running user's Library items in the project."""
    user = {"id": conn.user_id, "name": conn.user_full_name}
    shortcuts = Library(conn, project_id=project_id).list_library_shortcuts()
    in_project = [s for s in shortcuts if getattr(s, "project_id", None) in (None, project_id)]
    if len(in_project) < len(shortcuts):
        other_ids = sorted({s.project_id for s in shortcuts
                            if getattr(s, "project_id", None) not in (None, project_id)})
        print(f"  {len(shortcuts) - len(in_project)} of {len(shortcuts)} Library item(s) are in "
              f"other project(s) {other_ids} and are skipped.")
    records = []
    for s in in_project:
        if not s.id:
            continue
        target = getattr(s, "target", None) or {}
        records.append({"id": s.id, "name": getattr(s, "name", None), "owner": user,
                        "target_id": target.get("id"),
                        "target_name": target.get("name") or getattr(s, "name", None)})
    return records


def _read_details(conn, project_id, shortcut_ids):
    return get_shortcuts(conn, shortcut_ids=shortcut_ids, project_id=project_id,
                         to_dictionary=True)


def fetch_details(conn, project_id, shortcut_ids):
    """{shortcut_id: shortcut dict} for target names, plus a count of failures.

    Read in batches; a failed batch is retried one shortcut at a time.
    """
    ids = list(shortcut_ids)
    details, failed = {}, 0
    for start in range(0, len(ids), BATCH_SIZE):
        batch = ids[start:start + BATCH_SIZE]
        try:
            items = _read_details(conn, project_id, batch)
        except Exception:  # noqa: BLE001
            items = []
            for shortcut_id in batch:
                try:
                    items.extend(_read_details(conn, project_id, [shortcut_id]))
                except Exception:  # noqa: BLE001
                    failed += 1
        for item in items:
            if item.get("id"):
                details[item["id"]] = item
    return details, failed


def all_user_shortcuts(conn, project_id):
    """Shortcut records for every Library shortcut the search returns."""
    found = full_search(conn, project=project_id,
                        object_types=[ObjectSubTypes.LIBRARY_SHORTCUT],
                        include_hidden=True, to_dictionary=True)
    records = {r["id"]: {"id": r["id"], "name": r.get("name"), "owner": r.get("owner") or {}}
               for r in found if r.get("id")}
    details, failed = fetch_details(conn, project_id, records)
    if failed:
        print(f"  Target names unavailable for {failed} shortcut(s); their shortcut name is used.")
    for shortcut_id, record in records.items():
        detail = details.get(shortcut_id) or {}
        target = detail.get("target") or {}
        record["target_id"] = target.get("id")
        record["target_name"] = target.get("name") or detail.get("name") or record["name"]
    return list(records.values())


# --- Reading bookmarks ----------------------------------------------------------


def _error_text(response):
    """'HTTP 500 ERR002: message' from a failed response."""
    try:
        body = response.json()
    except ValueError:
        return f"HTTP {response.status_code}"
    if isinstance(body, dict):
        errors = body.get("errors")
        if isinstance(errors, list) and errors and isinstance(errors[0], dict):
            body = errors[0]
        code, message = body.get("code"), body.get("message")
        if code or message:
            return f"HTTP {response.status_code} {code or ''}: {message or ''}".replace(" :", ":")
    return f"HTTP {response.status_code}"


def read_bookmarks(conn, project_id, shortcut_id):
    """(bookmark list, None) on success, or (None, error text)."""
    try:
        response = conn.get(endpoint=f"/api/shortcuts/{shortcut_id}/bookmarks",
                            headers={"X-MSTR-ProjectID": project_id})
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)
    if not response.ok:
        return None, _error_text(response)
    try:
        body = response.json()
    except ValueError:
        return None, f"HTTP {response.status_code}: response was not JSON"
    if isinstance(body, dict):
        body = body.get("bookmarks", [])
    return (body if isinstance(body, list) else []), None


def bookmark_rows(record, bookmarks, project_id):
    """One row per bookmark on a shortcut."""
    owner = record.get("owner")
    return [{
        "ProjectID": project_id,
        "LibraryUserID": _id(owner),
        "LibraryUser": _name(owner),
        "ShortcutID": record["id"],
        "TargetID": record.get("target_id"),
        "TargetName": record.get("target_name") or record.get("name"),
        "BookmarkID": b.get("id"),
        "BookmarkName": b.get("name"),
        "Created": b.get("creationTime"),
        "Modified": b.get("lastUpdateTime"),
        "LastViewed": b.get("lastViewTime"),
        "Version": b.get("version"),
    } for b in bookmarks if isinstance(b, dict)]


# --- Main -------------------------------------------------------------------------


def main():
    if not PROJECT_NAME:
        raise ValueError("project_name is required.")
    all_users = _all_users_scope(SCOPE)

    conn = get_connection(connectionData, project_name=PROJECT_NAME)  # noqa: F821
    project_id = conn.project_id
    scope_label = "All users" if all_users else f"Current user ({conn.user_full_name})"
    print(f"Project {PROJECT_NAME} ({project_id}); scope: {scope_label}.")

    records = (all_user_shortcuts(conn, project_id) if all_users
               else current_user_shortcuts(conn, project_id))
    print(f"Found {len(records)} Library shortcut(s).")
    if TARGET_NAME:
        wanted = TARGET_NAME.lower()
        records = [r for r in records
                   if wanted in str(r.get("target_name") or r.get("name") or "").lower()]
        print(f"{len(records)} match target_name '{TARGET_NAME}'.")

    rows, failures, skipped = [], {}, []
    checked = with_bookmarks = other_tried = other_ok = 0
    for record in records:
        other_user = all_users and _id(record.get("owner")) != conn.user_id
        if other_user and other_tried >= OTHER_USER_PROBE and other_ok == 0:
            skipped.append(record["id"])
            continue
        bookmarks, error = read_bookmarks(conn, project_id, record["id"])
        checked += 1
        if other_user:
            other_tried += 1
            other_ok += error is None
        if error:
            failures[record["id"]] = error
        else:
            shortcut_rows = bookmark_rows(record, bookmarks, project_id)
            if shortcut_rows:
                with_bookmarks += 1
                rows.extend(shortcut_rows)
        if checked % PROGRESS_EVERY == 0:
            print(f"  checked {checked} of {len(records)} shortcut(s); {len(rows)} bookmark(s) so far")

    print("=== Bookmarks ===")
    for r in sorted(rows, key=lambda r: (str(r["LibraryUser"]), str(r["TargetName"]),
                                         str(r["BookmarkName"]))):
        print(f"  {r['LibraryUser']} | {r['TargetName']} ({r['TargetID']}) | "
              f"{r['BookmarkName']} ({r['BookmarkID']}) | modified {r['Modified']}")

    print("=== Summary ===")
    print(f"  Library shortcuts:           {len(records)}")
    print(f"  Checked:                     {checked}")
    print(f"  Shortcuts with bookmarks:    {with_bookmarks}")
    print(f"  Bookmarks:                   {len(rows)}")
    if failures:
        print(f"  Could not read bookmarks:    {len(failures)}")
        for error, count in Counter(failures.values()).most_common(5):
            example = next(s for s, e in failures.items() if e == error)
            print(f"    {count} x {error} (e.g. shortcut {example})")
    if skipped:
        print(f"  Skipped {len(skipped)} shortcut(s) owned by other users: the first "
              f"{OTHER_USER_PROBE} of theirs all failed, so this user can't read other "
              f"users' bookmarks.")
    return rows


# The host exec()s this script as a string, so the __main__ guard is unreliable.
result = main()
print(json.dumps(result, default=str))


def get_results():
    return result
