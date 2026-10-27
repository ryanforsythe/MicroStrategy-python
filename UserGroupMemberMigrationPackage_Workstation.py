# Build an administration migration package from the users in a group tree (Workstation).
"""UserGroupMemberMigrationPackage_Workstation — package a user group's members.

MicroStrategy Workstation script. Uses the injected `connectionData` session.

What it does
------------
  1. Resolves the user group by name or GUID.
  2. Walks it and every sub-group (cycle-guarded) and collects the users.
  3. Drops disabled users (exclude_disabled, on by default) and any login
     containing exclude_login_contains, e.g. "@email.com".
  4. Lists the users it would package (preview) and, when create_package is
     true, creates an ADMINISTRATION migration package holding them, with
     default_action (default "use existing").

Group members come back with only id/name/subtype, so login and enabled state
are read from one list_users() call and matched by id; anyone missing from that
list is fetched individually.

Notes
-----
Migration needs I-Server 11.3.10+ and a configured shared file store; the
package is created on the server and appears in Workstation under Migrations.
Nothing else is changed — creating a package neither exports nor imports.
Migration.create_admin_migration() takes no project_id parameter; the connection
carries the project.

Creation is asynchronous ("prefer: respond-async"): the call returns while the
I-Server is still building the package, whose status runs creating -> created /
empty / create_failed. So the script waits for the status to settle and then
reads the package back, reporting how many objects it really holds — an empty
package is reported as an error instead of a success.

Each content entry carries id, type, action and include_dependents=False: the
package holds exactly the users listed, with no dependent objects pulled in.
(mstrio-py's code_snippets/migration.py uses True; we don't want dependents.)

Inputs
------
    project_name            text  Project for the connection (required)
    user_group              text  Group name or GUID to package (required)
    exclude_login_contains  text  Skip logins containing this text, e.g.
                                  "@email.com" (case-insensitive). Blank = keep all.
    exclude_disabled        text  true (default) / false
    create_package          text  false (default) = preview only, list the users
                                  and create nothing; true = create the package
    default_action          text  use existing (default), replace, keep both,
                                  use newer, use older, force replace
    package_name            text  Package name. Blank = UserMigration_<group>_<UTC stamp>

Workstation "unset" tokens (case-insensitive): '', '-', 'null', 'blank', 'empty'.
"""

import json
import re
import sys
import time
from datetime import datetime, timezone

from mstrio.connection import get_connection
from mstrio.object_management.migration.migration import Migration
from mstrio.object_management.migration.package import (
    Action,
    PackageConfig,
    PackageContentInfo,
    PackageSettings,
)
from mstrio.types import ObjectTypes
from mstrio.users_and_groups.user import User, list_users
from mstrio.users_and_groups.user_group import UserGroup, list_user_groups

_EMPTY_SENTINELS = {"", "-", "null", "blank", "empty"}
_GUID = re.compile(r"[0-9A-Fa-f]{32}")

PACKAGE_TIMEOUT_SECONDS = 300   # wait for the server to finish building the package
PACKAGE_POLL_SECONDS = 5


def _blank(value, default=None):
    if value is None:
        return default
    if isinstance(value, str) and value.strip().lower() in _EMPTY_SENTINELS:
        return default
    return value


def _as_bool(value, name):
    text = str(value).strip().lower()
    if text in ("true", "yes", "y", "1"):
        return True
    if text in ("false", "no", "n", "0"):
        return False
    raise ValueError(f"{name} must be true or false, not {value!r}.")


def _as_action(value):
    """'use existing' / 'USE_EXISTING' / 'use-existing' -> Action.USE_EXISTING."""
    key = re.sub(r"[\s\-]+", "_", str(value).strip().lower())
    try:
        return Action[key.upper()]
    except KeyError:
        raise ValueError(f"default_action {value!r} is not valid. Use one of: "
                         + ", ".join(a.name.lower().replace('_', ' ') for a in Action)) from None


# --- Substituted by Workstation (text -> 'quoted') --------------------------
PROJECT_NAME = _blank($project_name)                       # noqa: F821
USER_GROUP = _blank($user_group)                           # noqa: F821
EXCLUDE_LOGIN_CONTAINS = _blank($exclude_login_contains)   # noqa: F821
EXCLUDE_DISABLED = _as_bool(_blank($exclude_disabled, "true"), "exclude_disabled")  # noqa: F821
CREATE_PACKAGE = _as_bool(_blank($create_package, "false"), "create_package")  # noqa: F821
DEFAULT_ACTION = _as_action(_blank($default_action, "use existing"))  # noqa: F821
PACKAGE_NAME = _blank($package_name)                       # noqa: F821
# ----------------------------------------------------------------------------


# --- Logging (loguru, with a same-format fallback) ----------------------------------
LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS ZZ} | {level: <8} | {message}"

try:
    from loguru import logger
    logger.remove()
    logger.add(lambda message: sys.stdout.write(message), format=LOG_FORMAT,
               level="INFO", colorize=False)
except ImportError:  # loguru isn't installed in this runtime
    class _TimestampLogger:
        def __getattr__(self, level):
            def log(message, *args, **kwargs):
                if args or kwargs:
                    message = message.format(*args, **kwargs)
                now = datetime.now().astimezone()
                print(f"{now:%Y-%m-%d %H:%M:%S}.{now.microsecond // 1000:03d} {now:%z} | "
                      f"{level.upper():<8} | {message}", flush=True)
            return log

    logger = _TimestampLogger()


# --- Members ---------------------------------------------------------------------------


def resolve_group(conn, name_or_id):
    """The user group, by GUID when the value looks like one, else by exact name.

    A name is resolved through list_user_groups so an ambiguous name stops the
    run: UserGroup(name=...) takes the first match silently, which can package
    the wrong group's members.
    """
    value = str(name_or_id).strip()
    if _GUID.fullmatch(value):
        return UserGroup(conn, id=value)
    candidates = list_user_groups(conn, name_begins=value)
    matches = [g for g in candidates if str(g.name).strip().lower() == value.lower()]
    if not matches:
        near = ", ".join(sorted(str(g.name) for g in candidates)[:10]) or "(none)"
        raise ValueError(f"No user group is named {value!r}. Groups starting with that "
                         f"text: {near}")
    if len(matches) > 1:
        listed = ", ".join(f"{g.name} ({g.id})" for g in matches[:10])
        raise ValueError(f"{len(matches)} user groups are named {value!r}: {listed}. "
                         f"Use the GUID in user_group instead.")
    logger.info("Resolved '{value}' to group {id}.", value=value, id=matches[0].id)
    return UserGroup(conn, id=matches[0].id)


def collect_users(group, seen_groups=None, users=None, depth=0):
    """{user_id: name} for the group and every sub-group, cycle-guarded."""
    seen_groups = set() if seen_groups is None else seen_groups
    users = {} if users is None else users
    if group.id in seen_groups:
        logger.warning("  group '{name}' already visited — skipped (cycle)", name=group.name)
        return users
    seen_groups.add(group.id)
    members = group.members or []
    sub_groups = [m for m in members if isinstance(m, UserGroup)]
    member_users = [m for m in members if isinstance(m, User)]
    logger.info("  {indent}group '{name}' ({id}): {u} direct user(s), {g} sub-group(s)",
                indent="  " * depth, name=group.name, id=group.id,
                u=len(member_users), g=len(sub_groups))
    if len(members) != len(sub_groups) + len(member_users):
        logger.warning("  {indent}{n} member(s) are neither users nor groups and were skipped",
                       indent="  " * depth,
                       n=len(members) - len(sub_groups) - len(member_users))
    for member in member_users:
        users.setdefault(member.id, member.name)
    for member in sub_groups:
        collect_users(member, seen_groups, users, depth + 1)
    return users


def user_details(conn, user_ids):
    """{id: (login, enabled)} — one list_users() call, with a per-user fetch for
    anyone it doesn't return."""
    details = {}
    for user in list_users(conn):
        if user.id in user_ids:
            details[user.id] = (getattr(user, "username", None), getattr(user, "enabled", None))
    for user_id in user_ids - set(details):
        try:
            user = User(conn, id=user_id)
            details[user_id] = (user.username, user.enabled)
        except Exception as exc:  # noqa: BLE001
            logger.warning("  could not read user {id}: {error}", id=user_id, error=exc)
            details[user_id] = (None, None)
    return details


def filter_users(users, details, exclude_login_contains, exclude_disabled):
    """(included, excluded) lists of {id, name, login, enabled, reason}."""
    needle = (exclude_login_contains or "").lower()
    included, excluded = [], []
    for user_id, name in sorted(users.items(), key=lambda kv: str(kv[1]).lower()):
        login, enabled = details.get(user_id, (None, None))
        row = {"id": user_id, "name": name, "login": login, "enabled": enabled}
        if exclude_disabled and enabled is False:
            excluded.append({**row, "reason": "disabled"})
        elif needle and needle in str(login or "").lower():
            excluded.append({**row, "reason": f"login contains {exclude_login_contains!r}"})
        else:
            included.append(row)
    return included, excluded


# --- Package ---------------------------------------------------------------------------


def build_package(included, default_action):
    """PackageConfig for the included users: one entry each, no dependents."""
    content = [PackageContentInfo(id=user["id"], type=ObjectTypes.USER,
                                  action=default_action, include_dependents=False)
               for user in included]
    return PackageConfig(PackageSettings(default_action=default_action), content)


def create_package(conn, package_config, name):
    migration = Migration.create_admin_migration(conn, toc_view=package_config, name=name)
    return migration


def wait_for_package(migration):
    """Wait while the server builds the package; return its status as a string
    ('created', 'empty', 'create_failed', ...) or 'timed out while creating'."""
    deadline = time.time() + PACKAGE_TIMEOUT_SECONDS
    while True:
        try:
            migration.fetch()
        except Exception as exc:  # noqa: BLE001
            logger.warning("  could not refresh the migration: {error}", error=exc)
        status = getattr(getattr(migration, "package_info", None), "status", None)
        status = str(getattr(status, "value", status) or "unknown").lower()
        if status != "creating":
            return status
        if time.time() >= deadline:
            return "timed out while creating"
        logger.info("  package status 'creating'; waiting...")
        time.sleep(PACKAGE_POLL_SECONDS)


def package_contents(migration):
    """(count, names, stored body) of what the server actually put in the package.

    The server echoes the package under packageInfo.tocView.content, one entry
    per object with name/version/explicitIncluded filled in.
    """
    try:
        body = migration.get_migration_content() or {}
    except Exception as exc:  # noqa: BLE001
        logger.warning("  could not read the package back: {error}", error=exc)
        return None, [], {}
    info = body.get("packageInfo") or body.get("package_info") or {}
    toc = info.get("tocView") or info.get("toc_view") or {}
    content = toc.get("content") or []
    names = [str(item.get("name") or item.get("id")) for item in content
             if isinstance(item, dict)]
    return len(content), names, body


def report_package_gap(sent_ids, body):
    """Log why a package doesn't hold what was sent: the server's own status and
    message, which ids came back, and which of ours are missing."""
    info = (body or {}).get("packageInfo") or {}
    toc = info.get("tocView") or {}
    stored = [item.get("id") for item in (toc.get("content") or []) if isinstance(item, dict)]
    logger.warning("  server says: status={status!r} message={message!r} progress={progress} "
                   "storage={storage}", status=info.get("status"),
                   message=info.get("message"), progress=info.get("progress"),
                   storage=(info.get("storage") or {}).get("size"))
    missing = [i for i in sent_ids if i not in stored]
    logger.warning("  sent {sent} id(s), stored {stored}; missing {n}: {ids}",
                   sent=len(sent_ids), stored=len(stored), n=len(missing), ids=missing[:10])
    logger.warning("  stored package (first 2000 chars): {body}",
                   body=json.dumps(body, default=str)[:2000])


# --- Main ------------------------------------------------------------------------------


def main():
    if not PROJECT_NAME:
        raise ValueError("project_name is required.")
    if not USER_GROUP:
        raise ValueError("user_group is required: a group name or GUID.")

    conn = get_connection(connectionData, project_name=PROJECT_NAME)  # noqa: F821
    group = resolve_group(conn, USER_GROUP)
    logger.info("Group '{name}' ({id}); {mode}; exclude_disabled={d}; "
                "exclude_login_contains={f}; default_action={a}",
                name=group.name, id=group.id,
                mode="CREATE PACKAGE" if CREATE_PACKAGE else "PREVIEW — nothing is created",
                d=EXCLUDE_DISABLED, f=EXCLUDE_LOGIN_CONTAINS or "(none)",
                a=DEFAULT_ACTION.name.lower())

    logger.info("Walking the group tree:")
    users = collect_users(group)
    logger.info("{n} distinct user(s) in the tree.", n=len(users))
    if not users:
        raise ValueError(f"Group '{group.name}' contains no users.")

    details = user_details(conn, set(users))
    included, excluded = filter_users(users, details, EXCLUDE_LOGIN_CONTAINS, EXCLUDE_DISABLED)
    for user in excluded:
        logger.info("  excluded {login} ({name}) — {reason}",
                    login=user["login"] or user["id"], name=user["name"], reason=user["reason"])
    logger.info("{inc} user(s) to package, {exc} excluded.", inc=len(included), exc=len(excluded))
    if not included:
        raise ValueError("No users are left after the filters; nothing to package.")

    logger.info("Users to package:")
    for user in included:
        logger.info("  {login} ({name})", login=user["login"] or user["id"], name=user["name"])

    name = PACKAGE_NAME or (f"UserMigration_{re.sub(r'[^A-Za-z0-9]+', '', group.name)}_"
                            f"{datetime.now(timezone.utc):%Y%m%d_%H%M%S}")
    migration_id, status, object_count = None, None, None
    if CREATE_PACKAGE:
        migration = create_package(conn, build_package(included, DEFAULT_ACTION), name)
        migration_id = getattr(migration, "id", None)
        logger.info("Submitted package '{name}' (migration {id}); waiting for the server "
                    "to build it.", name=name, id=migration_id or "?")
        status = wait_for_package(migration)
        object_count, names, stored = package_contents(migration)
        if status == "created" and object_count == len(included):
            logger.success("Package '{name}' (migration {id}) holds {n} object(s).",
                           name=name, id=migration_id or "?", n=object_count)
        else:
            logger.error("Package '{name}' (migration {id}) ended as '{status}' with "
                         "{n} object(s), expected {want}. Package contents: {names}",
                         name=name, id=migration_id or "?", status=status,
                         n="unknown" if object_count is None else object_count,
                         want=len(included), names=names[:10] or "(none)")
            report_package_gap([u["id"] for u in included], stored)
    else:
        logger.info("PREVIEW: nothing created. Set create_package to true to build "
                    "'{name}' with these {n} user(s).", name=name, n=len(included))

    return {"group": {"id": group.id, "name": group.name},
            "package": {"name": name, "migration_id": migration_id, "created": CREATE_PACKAGE,
                        "status": status, "object_count": object_count,
                        "default_action": DEFAULT_ACTION.name.lower()},
            "included": included, "excluded": excluded}


# The host exec()s this script as a string, so the __main__ guard is unreliable.
result = main()
print(json.dumps(result, default=str))


def get_results():
    return result


_package = result["package"]
if _package["created"] and (_package["status"] != "created"
                            or _package["object_count"] != len(result["included"])):
    sys.exit(1)     # the package is missing objects — see the error line above
