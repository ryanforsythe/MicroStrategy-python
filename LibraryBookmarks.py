"""LibraryBookmarks — export Library bookmarks for a project with the REST API only.

No mstrio-py: plain `requests` calls, so every endpoint, parameter and error is
visible in this file and in the log.

What the API allows
-------------------
A bookmark belongs to a Library shortcut, the per-user object created when a
dashboard or document is added to a user's Library. Bookmarks are returned only
for shortcuts in the Library of the user whose session makes the call:
GET /api/shortcuts/{id}/bookmarks answers 200 for those, and ERR002
"Catastrophic failure" for another user's shortcut, even for an Administrator
(tested 2026-09-17 on dev). There is no administrator-wide bookmark endpoint.

So one run exports:
  * every Library shortcut in the project, with its owner and target (an
    Administrator can list them all), and
  * the bookmarks of the user this run logs in as.
Each shortcut gets a status, so the export doubles as an inventory showing which
shortcuts could not be read. To cover a whole project, run it once per user with
`--username`, then concatenate the CSVs.

Endpoints used
--------------
    POST /api/auth/login                          log in (token in X-MSTR-AuthToken)
    GET  /api/sessions/userInfo                   who this session is
    GET  /api/projects                            resolve the project name or GUID
    POST /api/v2/metadataSearches/results         search Library shortcuts
         ?type=4609&domain=2&pattern=4&visibility=all
    GET  /api/metadataSearches/results?searchId=  paged search results
    POST /api/searches/library/shortcuts          target object of each shortcut
    GET  /api/shortcuts/{id}/bookmarks            the bookmarks (per-user)
    POST /api/auth/logout                         log out
Older servers without the v2 search fall back to POST /api/metadataSearches/results,
which cannot filter hidden objects.

Credentials
-----------
From the environment, following the repo's prefix chain
MSTR_{ENV}_{VAR} -> MSTR_{VAR}: MSTR_BASE_URL, MSTR_USERNAME, MSTR_PASSWORD,
MSTR_LOGIN_MODE (default 1), MSTR_SSL_VERIFY. `--username` overrides the login and
prompts for the password unless MSTR_OTHER_PASSWORD is set. Nothing is written to
disk except the export.

Usage
-----
    python LibraryBookmarks.py dev --project "Market Intelligence - US"
    python LibraryBookmarks.py dev --project 5043963F4F486B6CECACB6929A9958BD \
        --username ryan.forsythe@agdata.com --format json
    python LibraryBookmarks.py prod --project "Market Intelligence - US" --bookmarks-only
"""

import argparse
import csv
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from getpass import getpass
from pathlib import Path

import requests
import urllib3
from loguru import logger

LIBRARY_SHORTCUT_SUBTYPE = 4609   # ObjectSubTypes.LIBRARY_SHORTCUT
SEARCH_DOMAIN_PROJECT = 2         # SearchDomain.PROJECT
SEARCH_PATTERN_CONTAINS = 4       # SearchPattern.CONTAINS
SEARCH_PAGE = 500                 # search results per request
SHORTCUT_BATCH = 50               # shortcuts per target-name request
REQUEST_TIMEOUT = 120

COLUMNS = ["project_id", "project_name", "owner_id", "owner_name", "shortcut_id",
           "shortcut_name", "target_id", "target_name", "bookmark_id", "bookmark_name",
           "version", "creation_time", "last_update_time", "last_view_time",
           "status", "status_details"]


def setup_logging(level="INFO"):
    logger.remove()
    logger.add(lambda message: sys.stdout.write(message), colorize=False, level=level,
               format="{time:YYYY-MM-DD HH:mm:ss.SSS ZZ} | {level: <8} | {message}")


# --- Environment -------------------------------------------------------------------


def env_value(env, name, default=None):
    """MSTR_{ENV}_{NAME} -> MSTR_{NAME} -> default (the repo's prefix chain)."""
    for key in (f"MSTR_{env.upper()}_{name}", f"MSTR_{name}"):
        value = os.environ.get(key)
        if value not in (None, ""):
            return value
    return default


def ssl_verify(env):
    return str(env_value(env, "SSL_VERIFY", "true")).strip().lower() not in ("false", "0", "no")


# --- Session -----------------------------------------------------------------------


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
        parts = [str(body[key]) for key in ("code", "message") if body.get(key)]
        if parts:
            return f"HTTP {response.status_code} " + ": ".join(parts)
    return f"HTTP {response.status_code}"


class LibrarySession:
    """A logged-in REST session. Raises RuntimeError on a failed call, except
    `get_bookmarks`, which returns the error so one shortcut can't stop a run."""

    def __init__(self, base_url, verify=True):
        self.base_url = base_url.rstrip("/")
        self.verify = verify
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.user = {}

    # -- plumbing
    def request(self, method, endpoint, **kwargs):
        kwargs.setdefault("timeout", REQUEST_TIMEOUT)
        return self.session.request(method, self.base_url + endpoint, verify=self.verify,
                                    **kwargs)

    def call(self, method, endpoint, what, **kwargs):
        response = self.request(method, endpoint, **kwargs)
        if not response.ok:
            raise RuntimeError(f"{what} failed: {_error_text(response)}")
        return response.json() if response.content else {}

    # -- authentication
    def login(self, username, password, login_mode=1):
        response = self.request("POST", "/api/auth/login", data={
            "username": username, "password": password, "loginMode": int(login_mode)})
        if not response.ok:
            raise RuntimeError(f"Login as '{username}' failed: {_error_text(response)}")
        token = response.headers.get("X-MSTR-AuthToken")
        if not token:
            raise RuntimeError("Login returned no X-MSTR-AuthToken header.")
        self.session.headers["X-MSTR-AuthToken"] = token
        self.user = self.call("GET", "/api/sessions/userInfo", "Reading the session user")
        logger.info("Logged in as {name} ({id}) at {url}",
                    name=self.user.get("fullName") or username,
                    id=self.user.get("id"), url=self.base_url)
        return self

    def logout(self):
        try:
            self.request("POST", "/api/auth/logout")
        except requests.RequestException as exc:
            logger.warning("Logout failed: {error}", error=exc)

    # -- data
    def project(self, wanted):
        """Resolve a project name or GUID against the projects this user can see."""
        projects = self.call("GET", "/api/projects", "Listing projects")
        for project in projects:
            if wanted.lower() in (str(project.get("id", "")).lower(),
                                  str(project.get("name", "")).lower()):
                return {"id": project["id"], "name": project.get("name")}
        raise RuntimeError(f"Project {wanted!r} not found. Available: "
                           + ", ".join(sorted(str(p.get('name')) for p in projects)))

    def library_shortcuts(self, project_id):
        """Every Library shortcut in the project, hidden ones included."""
        headers = {"X-MSTR-ProjectID": project_id}
        params = {"type": LIBRARY_SHORTCUT_SUBTYPE, "domain": SEARCH_DOMAIN_PROJECT,
                  "pattern": SEARCH_PATTERN_CONTAINS, "visibility": "all"}
        response = self.request("POST", "/api/v2/metadataSearches/results",
                                headers=headers, params=params, json={})
        if response.status_code in (400, 404, 405):   # server without the v2 search
            logger.warning("v2 search unavailable ({error}); using the v1 search, which "
                           "cannot include hidden objects.", error=_error_text(response))
            params.pop("visibility")
            response = self.request("POST", "/api/metadataSearches/results",
                                    headers=headers, params=params)
        if not response.ok:
            raise RuntimeError(f"Searching Library shortcuts failed: {_error_text(response)}")
        search_id = response.json().get("id")
        if not search_id:
            raise RuntimeError("The shortcut search returned no search id.")

        found, offset = [], 0
        while True:
            page = self.call("GET", "/api/metadataSearches/results", "Reading search results",
                             headers=headers,
                             params={"searchId": search_id, "offset": offset,
                                     "limit": SEARCH_PAGE})
            items = page.get("result", page) if isinstance(page, dict) else page
            items = [i for i in (items or []) if i.get("id")]
            found.extend(items)
            if len(items) < SEARCH_PAGE:
                return found
            offset += SEARCH_PAGE

    def shortcut_targets(self, project_id, shortcut_ids):
        """{shortcut_id: target dict}. Works for other users' shortcuts."""
        targets = {}
        ids = list(shortcut_ids)
        for start in range(0, len(ids), SHORTCUT_BATCH):
            batch = ids[start:start + SHORTCUT_BATCH]
            try:
                items = self.call("POST", "/api/searches/library/shortcuts",
                                  "Reading shortcut targets",
                                  headers={"X-MSTR-ProjectID": None},
                                  params={"shortcutInfoFlag": 0},
                                  json=[{"projectId": project_id, "shortcutIds": batch}])
            except RuntimeError as exc:
                logger.warning("  target names unavailable for {count} shortcut(s): {error}",
                               count=len(batch), error=exc)
                continue
            for item in items if isinstance(items, list) else items.get("shortcuts", []):
                if item.get("id"):
                    targets[item["id"]] = item.get("target") or {}
        return targets

    def get_bookmarks(self, project_id, shortcut_id):
        """(bookmark list, None) on success, or (None, error text)."""
        try:
            response = self.request("GET", f"/api/shortcuts/{shortcut_id}/bookmarks",
                                    headers={"X-MSTR-ProjectID": project_id})
        except requests.RequestException as exc:
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


# --- Rows --------------------------------------------------------------------------


def _base_row(project, shortcut, target):
    owner = shortcut.get("owner") or {}
    return {
        "project_id": project["id"],
        "project_name": project.get("name"),
        "owner_id": owner.get("id"),
        "owner_name": owner.get("name"),
        "shortcut_id": shortcut.get("id"),
        "shortcut_name": shortcut.get("name"),
        "target_id": (target or {}).get("id"),
        "target_name": (target or {}).get("name") or shortcut.get("name"),
        "bookmark_id": None, "bookmark_name": None, "version": None,
        "creation_time": None, "last_update_time": None, "last_view_time": None,
        "status": None, "status_details": None,
    }


def shortcut_rows(project, shortcut, target, bookmarks, error):
    """Rows for one shortcut: one per bookmark, or a single status row."""
    base = _base_row(project, shortcut, target)
    if error:
        return [{**base, "status": "error", "status_details": error}]
    if not bookmarks:
        return [{**base, "status": "no bookmarks"}]
    return [{**base,
             "bookmark_id": b.get("id"),
             "bookmark_name": b.get("name"),
             "version": b.get("version"),
             "creation_time": b.get("creationTime"),
             "last_update_time": b.get("lastUpdateTime"),
             "last_view_time": b.get("lastViewTime"),
             "status": "ok"} for b in bookmarks if isinstance(b, dict)]


def collect(session, project, shortcuts, concurrency=10):
    """Read every shortcut's bookmarks and return the rows."""
    targets = session.shortcut_targets(project["id"], [s["id"] for s in shortcuts])

    def read(shortcut):
        bookmarks, error = session.get_bookmarks(project["id"], shortcut["id"])
        return shortcut_rows(project, shortcut, targets.get(shortcut["id"]),
                             bookmarks, error)

    rows = []
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        for done, shortcut_rows_ in enumerate(pool.map(read, shortcuts), start=1):
            rows.extend(shortcut_rows_)
            if done % 200 == 0:
                logger.info("  read {done} of {total} shortcut(s)",
                            done=done, total=len(shortcuts))
    return rows


def summarize(rows):
    """Log what the run found; return the counts."""
    bookmarks = [r for r in rows if r["status"] == "ok"]
    errors = [r for r in rows if r["status"] == "error"]
    counts = {"shortcuts": len({r["shortcut_id"] for r in rows}),
              "bookmarks": len(bookmarks),
              "shortcuts_with_bookmarks": len({r["shortcut_id"] for r in bookmarks}),
              "unreadable": len(errors)}
    for row in sorted(bookmarks, key=lambda r: (str(r["owner_name"]), str(r["target_name"]),
                                                str(r["bookmark_name"]))):
        logger.info("  {owner} | {target} | {bookmark} | updated {updated}",
                    owner=row["owner_name"], target=row["target_name"],
                    bookmark=row["bookmark_name"], updated=row["last_update_time"])
    logger.info("Shortcuts: {shortcuts}; bookmarks: {bookmarks} on "
                "{shortcuts_with_bookmarks} shortcut(s); unreadable: {unreadable}", **counts)
    grouped = {}
    for row in errors:
        grouped.setdefault(row["status_details"], []).append(row["shortcut_id"])
    for error, ids in sorted(grouped.items(), key=lambda kv: -len(kv[1]))[:5]:
        logger.warning("  {count} x {error} (e.g. shortcut {example})",
                       count=len(ids), error=error, example=ids[0])
    return counts


def write_rows(rows, path, fmt="csv"):
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "json":
        path.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")
    else:
        with open(path, "w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=COLUMNS, delimiter=";")
            writer.writeheader()
            writer.writerows(rows)
    logger.success("Wrote {count} row(s) to {path}", count=len(rows), path=path)
    return path


# --- CLI ---------------------------------------------------------------------------


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0],
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("env", help="environment for the MSTR_{ENV}_* variables, "
                                    "e.g. dev, qa, prod")
    parser.add_argument("--project", required=True, help="project name or GUID")
    parser.add_argument("--username", help="log in as this user instead of MSTR_USERNAME, "
                                           "to collect that user's bookmarks")
    parser.add_argument("--bookmarks-only", action="store_true",
                        help="write only bookmark rows, not a row per shortcut without any")
    parser.add_argument("--format", choices=("csv", "json"), default="csv")
    parser.add_argument("--output-dir", help="default: MSTR_OUTPUT_DIR or c:/tmp")
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def credentials(args):
    """(base_url, username, password, login_mode) for this run."""
    base_url = env_value(args.env, "BASE_URL")
    if not base_url:
        raise RuntimeError(f"Set MSTR_{args.env.upper()}_BASE_URL or MSTR_BASE_URL.")
    login_mode = env_value(args.env, "LOGIN_MODE", 1)
    if args.username:
        password = os.environ.get("MSTR_OTHER_PASSWORD") or getpass(
            f"Password for {args.username}: ")
        return base_url, args.username, password, login_mode
    username = env_value(args.env, "USERNAME")
    password = env_value(args.env, "PASSWORD")
    if not (username and password):
        raise RuntimeError(f"Set MSTR_{args.env.upper()}_USERNAME and _PASSWORD "
                           f"(or MSTR_USERNAME / MSTR_PASSWORD), or pass --username.")
    return base_url, username, password, login_mode


def main(argv=None):
    args = parse_args(argv)
    setup_logging(args.log_level)
    base_url, username, password, login_mode = credentials(args)
    verify = ssl_verify(args.env)
    if not verify:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    session = LibrarySession(base_url, verify=verify)
    session.login(username, password, login_mode)
    try:
        project = session.project(args.project)
        logger.info("Project {name} ({id})", name=project["name"], id=project["id"])
        shortcuts = session.library_shortcuts(project["id"])
        logger.info("Found {count} Library shortcut(s).", count=len(shortcuts))
        rows = collect(session, project, shortcuts, args.concurrency)
    finally:
        session.logout()

    summarize(rows)
    if args.bookmarks_only:
        rows = [r for r in rows if r["status"] == "ok"]
    out_dir = Path(args.output_dir or env_value(args.env, "OUTPUT_DIR", "c:/tmp"))
    safe_user = "".join(c if c.isalnum() or c in "-._" else "_" for c in username)
    path = out_dir / f"library_bookmarks_{args.env}_{safe_user}.{args.format}"
    write_rows(rows, path, args.format)
    return 0


if __name__ == "__main__":
    sys.exit(main())
