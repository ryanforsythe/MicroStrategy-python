"""UserSync.py — Synchronize users across environments by direct create/update.

Two-phase, audit-driven user synchronization between two MicroStrategy
environments (dev / qa / prod). Unlike the earlier package-based approach, this
version applies changes directly through the mstrio-py SDK (no .mmp, no REST):

    1. audit  — Compare the members of a base user group in the SOURCE
                environment (recursing every sub user group) against EVERY user
                in the TARGET environment, matching on LOGIN only (GUID is not
                considered). Writes an .xlsx audit file, one row per in-scope
                source user, with a `Target Action` column defaulting to the
                diff reason (Create / Update).

    2. apply  — Re-reads the source live, keeps only rows whose `Target Action`
                is Create or Update in the reviewed audit file, and creates or
                updates each user directly on the target.

What apply enforces
-------------------
For every in-scope user (created or updated):
    * Status (enabled/disabled) is set to match the source.
    * User-group membership is synced to match the source — memberships are
      added and removed so the target set equals the source set. Only the
      'Everyone' group is ignored (it is automatic). Source groups whose GUID
      does not exist in the target are skipped with a warning.
    * standard_auth is disabled (the user cannot log on with standard auth).
    * If the login contains '@', trust_id (Trusted Authentication user id) is
      set to the login, and a default email address is ensured (device
      'Generic Email - Altus') when none exists.
    * If the login is NOT an '@agdata.net' / '@agdata.com' address,
      password_modifiable is disabled (the user cannot change their password).
    * Logins without '@' get no address (they cannot form a valid email).

Membership matching uses the group GUID; the audit reports group NAMES.

Usage
-----
    python UserSync.py audit <source_env> <target_env> --source-group "Group" \
        [--output-dir PATH] [--concurrency N]

    python UserSync.py apply <source_env> <target_env> --audit-file PATH \
        [--apply] [--output-dir PATH] [--concurrency N]

`apply` is dry-run by default (reports intended actions); pass --apply to
commit.

mstrio-py used
--------------
    UserGroup(conn, id=|name=).members            -> recurse source group tree
    list_users(conn)                               -> whole target env, by login
    list_user_groups(conn)                         -> target group GUIDs that exist
    User(conn, id=).enabled/.username/.memberships/.default_email_address
    User.create(...) / User.alter(...)             -> direct create/update
    UserGroup(conn, id=).add_users/remove_users    -> membership sync
"""

from __future__ import annotations

import argparse
import re
import secrets
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger
from mstrio.users_and_groups import User, UserGroup, list_user_groups, list_users

from mstrio_core import MstrConfig, get_mstrio_connection, read_excel, write_csv, write_excel
from mstrio_core.config import MstrEnvironment

# ── Constants ───────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]

# Default email device used when a user has no default address ("Generic Email - Altus")
GENERIC_EMAIL_DEVICE_ID = "4F549AE14DD30180A9C7A8AF57E6D08D"

# Logins on these domains keep password_modifiable; everything else gets it disabled.
AGDATA_DOMAINS = ("@agdata.net", "@agdata.com")

# Group names ignored during membership sync (case-insensitive). 'Everyone' is automatic.
IGNORED_GROUP_NAMES = {"everyone"}

GROUP_NAME_DELIM = "; "

AUDIT_COLUMNS = [
    "GUID",                # source user GUID (informational only; matching is by login)
    "Name",
    "Login",
    "Last Modified Time",  # source
    "Created Time",        # source
    "Status",              # source account: Enabled / Disabled
    "Diff Reason",         # CREATE / UPDATE
    "Differences",         # what drives an update (status; membership)
    "Target Action",       # editable gate: Create/Update = include, else exclude
    "Target Status",       # target account Enabled/Disabled (blank when CREATE)
    "SourceGroups",        # group names (Everyone excluded); shown only when differ
    "TargetGroups",        # group names (Everyone excluded); shown only when differ
]

DEFAULT_TARGET_ACTION = "Update"
INCLUDE_ACTIONS = {"create", "update"}   # recognized Target Action values (case-insensitive)

STATUS_CREATE = "CREATE"
STATUS_UPDATE = "UPDATE"

_GUID_LEN = 32


# ── Small helpers ────────────────────────────────────────────────────────────


def _is_guid(identifier: str) -> bool:
    s = str(identifier).strip()
    return len(s) == _GUID_LEN and all(c in "0123456789abcdefABCDEF" for c in s)


def _resolve_group(conn, identifier: str) -> UserGroup:
    identifier = str(identifier).strip()
    if _is_guid(identifier):
        return UserGroup(conn, id=identifier)
    return UserGroup(conn, name=identifier)


def _enabled_label(enabled) -> str:
    if enabled is True:
        return "Enabled"
    if enabled is False:
        return "Disabled"
    return ""


def _fmt_dt(value) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _is_email(login: str) -> bool:
    return "@" in (login or "")


def _is_agdata(login: str) -> bool:
    low = (login or "").strip().lower()
    return low.endswith(AGDATA_DOMAINS)


def _normalize_memberships(raw) -> list[dict]:
    """Return [{'id','name'}] from a memberships value that may be UserGroup
    objects or plain dicts."""
    out: list[dict] = []
    for m in raw or []:
        if isinstance(m, dict):
            mid, name = m.get("id"), m.get("name", "")
        else:
            mid, name = getattr(m, "id", None), getattr(m, "name", "") or ""
        if mid:
            out.append({"id": mid, "name": name})
    return out


def _membership_index(memberships: list[dict]) -> dict:
    """{group_id: group_name} excluding ignored groups (Everyone)."""
    return {
        m["id"]: m["name"]
        for m in memberships
        if (m["name"] or "").strip().lower() not in IGNORED_GROUP_NAMES
    }


def _group_names_str(id_to_name: dict) -> str:
    return GROUP_NAME_DELIM.join(sorted(n for n in id_to_name.values() if n))


# ── Group-tree expansion (source) ────────────────────────────────────────────


def _expand_member_user_ids(group: UserGroup, seen_groups: Optional[set] = None) -> set:
    """Set of member User IDs in a group and all its sub-groups (cycle-guarded)."""
    seen_groups = seen_groups if seen_groups is not None else set()
    user_ids: set = set()
    try:
        members = group.members
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not read members of group {gid}: {err}",
                       gid=getattr(group, "id", "?"), err=exc)
        return user_ids

    for member in members:
        if isinstance(member, UserGroup):
            if member.id in seen_groups:
                continue
            seen_groups.add(member.id)
            user_ids |= _expand_member_user_ids(member, seen_groups)
        elif isinstance(member, User):
            user_ids.add(member.id)
        else:
            sub = getattr(member, "subtype", None)
            mid = getattr(member, "id", None)
            if mid is None:
                continue
            if sub == 8705:  # USER_GROUP subtype
                if mid in seen_groups:
                    continue
                seen_groups.add(mid)
                user_ids |= _expand_member_user_ids(
                    UserGroup(group.connection, id=mid), seen_groups)
            else:
                user_ids.add(mid)
    return user_ids


def _fetch_user_meta(conn, user_id: str) -> dict:
    """Authoritative identity + status + direct memberships for one user."""
    u = User(conn, id=user_id)
    return {
        "id": u.id,
        "name": u.name or "",
        "username": (getattr(u, "username", "") or ""),
        "full_name": (getattr(u, "full_name", "") or ""),
        "enabled": getattr(u, "enabled", None),
        "date_modified": getattr(u, "date_modified", None),
        "date_created": getattr(u, "date_created", None),
        "memberships": _normalize_memberships(getattr(u, "memberships", []) or []),
    }


def _collect_source_users(conn, group_identifier: str, concurrency: int) -> dict:
    """Expand the source group tree → {user_id: meta} (with memberships)."""
    group = _resolve_group(conn, group_identifier)
    logger.info("[SOURCE] Group '{grp}' → {gid} ({name})",
                grp=group_identifier, gid=group.id, name=group.name)
    user_ids = _expand_member_user_ids(group)
    logger.info("[SOURCE] {n} distinct member user(s) across the group tree.", n=len(user_ids))

    by_id: dict = {}
    if not user_ids:
        return by_id
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        futures = {pool.submit(_fetch_user_meta, conn, uid): uid for uid in user_ids}
        for fut in as_completed(futures):
            uid = futures[fut]
            try:
                meta = fut.result()
            except Exception as exc:  # noqa: BLE001
                logger.warning("[SOURCE] Could not fetch user {uid}: {err}", uid=uid, err=exc)
                continue
            by_id[meta["id"]] = meta
    logger.success("[SOURCE] Loaded metadata for {n} user(s).", n=len(by_id))
    return by_id


def _collect_target_login_index(conn) -> dict:
    """{login_lower: target_user_id} for every user in the target environment."""
    index: dict = {}
    for u in list_users(conn):
        login = (getattr(u, "username", "") or "").strip().lower()
        uid = getattr(u, "id", None)
        if login and uid:
            index.setdefault(login, uid)
    logger.success("[TARGET] Indexed {n} target login(s).", n=len(index))
    return index


# ── Phase 1: audit ───────────────────────────────────────────────────────────


def cmd_audit(source_env: str, target_env: str, source_group: str,
              output_dir: Optional[Path], concurrency: int) -> Path:
    src_config = MstrConfig(environment=MstrEnvironment(source_env))
    tgt_config = MstrConfig(environment=MstrEnvironment(target_env))
    out_dir = output_dir or src_config.output_dir
    out_path = Path(out_dir) / f"usersync_audit_{source_env}_to_{target_env}.xlsx"

    src_conn = get_mstrio_connection(config=src_config)
    tgt_conn = get_mstrio_connection(config=tgt_config)
    try:
        source_users = _collect_source_users(src_conn, source_group, concurrency)
        target_index = _collect_target_login_index(tgt_conn)

        # Fetch target status + memberships only for logins that match (updates).
        matched_ids = {login: target_index[login]
                       for login in (
                           (m["username"] or "").strip().lower() for m in source_users.values())
                       if login and login in target_index}
        target_meta: dict = {}
        if matched_ids:
            logger.info("Fetching status/memberships for {n} matched target user(s).",
                        n=len(matched_ids))
            with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
                futures = {pool.submit(_fetch_user_meta, tgt_conn, uid): login
                           for login, uid in matched_ids.items()}
                for fut in as_completed(futures):
                    login = futures[fut]
                    try:
                        target_meta[login] = fut.result()
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Could not fetch target user for '{login}': {err}",
                                       login=login, err=exc)
    finally:
        for c in (src_conn, tgt_conn):
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass

    rows: list[list] = []
    n_create = n_update = 0
    for src in source_users.values():
        login = (src["username"] or "").strip().lower()
        src_groups = _membership_index(src["memberships"])
        tgt = target_meta.get(login) if login else None

        if tgt is None:
            reason = STATUS_CREATE
            n_create += 1
            differences = ""
            target_status = ""
            # For a create the target has nothing → groups differ by definition.
            source_groups_str = _group_names_str(src_groups)
            target_groups_str = ""
        else:
            reason = STATUS_UPDATE
            n_update += 1
            tgt_groups = _membership_index(tgt["memberships"])
            diffs = []
            if src["enabled"] != tgt["enabled"]:
                diffs.append("status")
            groups_differ = set(src_groups) != set(tgt_groups)
            if groups_differ:
                diffs.append("membership")
            differences = "; ".join(diffs)
            target_status = _enabled_label(tgt["enabled"])
            # Group columns are populated only when they differ (per spec).
            source_groups_str = _group_names_str(src_groups) if groups_differ else ""
            target_groups_str = _group_names_str(tgt_groups) if groups_differ else ""

        rows.append([
            src["id"], src["name"], src["username"],
            _fmt_dt(src["date_modified"]), _fmt_dt(src["date_created"]),
            _enabled_label(src["enabled"]),
            reason, differences,
            "Create" if reason == STATUS_CREATE else DEFAULT_TARGET_ACTION,
            target_status, source_groups_str, target_groups_str,
        ])

    # Creates first, then by login for a stable, reviewable order.
    rows.sort(key=lambda r: (r[6] != STATUS_CREATE, (r[2] or "").lower()))

    write_excel(rows, path=out_path, columns=AUDIT_COLUMNS, sheet_name="UserSync")
    logger.success("Audit complete: {c} to create, {u} to update → {path}",
                   c=n_create, u=n_update, path=out_path)
    return out_path


# ── Phase 2: apply ───────────────────────────────────────────────────────────


def _read_included_logins(audit_file: Path) -> set:
    """Logins whose `Target Action` is Create/Update (case-insensitive)."""
    df = read_excel(audit_file)
    cols = {str(c).strip().lower(): c for c in df.columns}
    login_col = cols.get("login")
    action_col = cols.get("target action")
    if login_col is None or action_col is None:
        raise ValueError(
            f"Audit file must contain 'Login' and 'Target Action' columns; found {list(df.columns)}")

    included, skipped = set(), 0
    for _, row in df.iterrows():
        login = str(row[login_col]).strip()
        action = str(row[action_col]).strip().lower()
        if not login or login.lower() == "nan":
            continue
        if action in INCLUDE_ACTIONS:
            included.add(login.lower())
        else:
            skipped += 1
    logger.info("Audit file: {inc} user(s) marked create/update, {skip} excluded.",
                inc=len(included), skip=skipped)
    return included


def _desired_target_group_ids(src_groups: dict, target_group_ids: set, login: str) -> set:
    """Source group ids (Everyone already excluded) that exist in the target.
    Missing groups are skipped with a warning."""
    desired = set()
    for gid, name in src_groups.items():
        if gid in target_group_ids:
            desired.add(gid)
        else:
            logger.warning("User '{login}': source group '{name}' ({gid}) "
                           "does not exist in target — skipping.",
                           login=login, name=name, gid=gid)
    return desired


_COLLISION_ID_RE = re.compile(r"with ID ([0-9A-Fa-f]{32})")


def _update_user(tgt_conn, user, src, target_group_ids, apply: bool) -> str:
    """Reconcile an existing target `user` to match `src`. Returns a details str.

    Note: `default_email_address` is NOT passed to alter() — mstrio's alter only
    EDITS an existing default address and raises when none exists, so a missing
    default email is created via add_address() instead.
    """
    login = (src["username"] or "").strip()
    is_email = _is_email(login)
    is_agdata = _is_agdata(login)
    src_groups = _membership_index(src["memberships"])
    desired_gids = _desired_target_group_ids(src_groups, target_group_ids, login)

    alter_kwargs = {"standard_auth": False}
    if user.enabled != bool(src["enabled"]):
        alter_kwargs["enabled"] = bool(src["enabled"])
    # Sync the display name from source (carries any "- Disabled <date>" suffix).
    src_display = src["full_name"] or src["name"]
    tgt_display = (getattr(user, "full_name", "") or getattr(user, "name", "") or "")
    if src_display and src_display != tgt_display:
        alter_kwargs["full_name"] = src_display
    if is_email and (getattr(user, "trust_id", None) or "") != login:
        alter_kwargs["trust_id"] = login
    if not is_agdata and (
            getattr(user, "password_modifiable", None) is not False
            or getattr(user, "password_auto_expire", None) is not False
            or getattr(user, "require_new_password", None) is not False):
        # password_modifiable=False conflicts with auto-expire / require-new-password;
        # the server rejects the combination, so disable all three together.
        alter_kwargs["password_modifiable"] = False
        alter_kwargs["password_auto_expire"] = False
        alter_kwargs["require_new_password"] = False

    # Ensure a default email address exists (add, never alter) for '@' logins.
    needs_default_email = is_email and not getattr(user, "default_email_address", None)

    current_gids = set(_membership_index(
        _normalize_memberships(getattr(user, "memberships", []) or [])))
    to_add = desired_gids - current_gids
    to_remove = current_gids - desired_gids

    details = (f"alter={sorted(alter_kwargs)}"
               + ("; +default_email" if needs_default_email else "")
               + f"; groups +{len(to_add)}/-{len(to_remove)}")

    if apply:
        user.alter(**alter_kwargs)  # always at least enforces standard_auth=False
        if needs_default_email:
            user.add_address(name="Default Email", address=login, default=True,
                             delivery_type="email", device_id=GENERIC_EMAIL_DEVICE_ID)
        for gid in to_add:
            UserGroup(tgt_conn, id=gid).add_users([user])
        for gid in to_remove:
            UserGroup(tgt_conn, id=gid).remove_users([user])
    return details


def _apply_one(tgt_conn, src, target_index, target_group_ids, apply: bool) -> dict:
    """Create or update one target user to match the source. Returns a result dict."""
    login = (src["username"] or "").strip()
    login_l = login.lower()
    full_name = src["full_name"] or src["name"] or login
    is_email = _is_email(login)
    is_agdata = _is_agdata(login)
    src_groups = _membership_index(src["memberships"])
    desired_gids = _desired_target_group_ids(src_groups, target_group_ids, login)

    result = {"login": login, "action": "", "status": "", "details": ""}
    exists_id = target_index.get(login_l)

    try:
        if exists_id is not None:
            # ---- UPDATE (matched by login) ----
            result["action"] = "update"
            user = User(tgt_conn, id=exists_id)
            result["details"] = _update_user(tgt_conn, user, src, target_group_ids, apply)
            result["status"] = "updated" if apply else "dry-run"
            return result

        # ---- CREATE ----
        result["action"] = "create"
        details = [f"enabled={src['enabled']}", "standard_auth=False",
                   f"groups=+{len(desired_gids)}"]
        if is_email:
            details += ["trust_id=login", "default_email=login"]
        if not is_agdata:
            details.append("password_modifiable=False")
        result["details"] = "; ".join(details)

        if not apply:
            result["status"] = "dry-run"
            return result

        try:
            User.create(
                connection=tgt_conn,
                username=login,
                full_name=full_name,
                password=secrets.token_urlsafe(16),  # unusable: standard_auth off
                enabled=bool(src["enabled"]),
                standard_auth=False,
                require_new_password=False,
                password_modifiable=(False if not is_agdata else True),
                # non-agdata users get password_modifiable=False, which requires
                # auto-expire off (server rejects the combination otherwise).
                password_auto_expire=(False if not is_agdata else None),
                trust_id=(login if is_email else None),
                memberships=sorted(desired_gids) or None,
                default_email_address=(login if is_email else None),
                email_device=(GENERIC_EMAIL_DEVICE_ID if is_email else None),
            )
            result["status"] = "created"
        except Exception as create_exc:  # noqa: BLE001
            # The identity (trust_id/username) already exists under a DIFFERENT
            # username, so the login match missed it. Recover the colliding user
            # id from the error and update that account instead of creating a dup.
            m = _COLLISION_ID_RE.search(str(create_exc))
            if not m:
                raise
            other_id = m.group(1)
            logger.warning("Create '{login}' collided with existing user {oid} "
                           "(same identity, different username) — updating instead.",
                           login=login, oid=other_id)
            user = User(tgt_conn, id=other_id)
            result["action"] = "update"
            result["details"] = ("converted-from-create; "
                                 + _update_user(tgt_conn, user, src, target_group_ids, apply))
            result["status"] = "updated"
        return result
    except Exception as exc:  # noqa: BLE001
        result["status"] = "error"
        result["details"] = f"{result.get('details','')} | ERROR: {exc}"
        logger.error("User '{login}' {act} failed: {err}",
                     login=login, act=result["action"], err=exc)
    return result


def cmd_apply(source_env: str, target_env: str, audit_file: Path, apply: bool,
              output_dir: Optional[Path], concurrency: int) -> Optional[Path]:
    included = _read_included_logins(Path(audit_file))
    if not included:
        logger.warning("No users marked create/update in the audit file — nothing to do.")
        return None

    src_config = MstrConfig(environment=MstrEnvironment(source_env))
    tgt_config = MstrConfig(environment=MstrEnvironment(target_env))
    out_dir = output_dir or src_config.output_dir
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = Path(out_dir) / f"usersync_apply_{source_env}_to_{target_env}_{ts}.csv"

    src_conn = get_mstrio_connection(config=src_config)
    tgt_conn = get_mstrio_connection(config=tgt_config)
    results: list[dict] = []
    try:
        # Re-read the source live so we have full metadata (full_name, memberships).
        # The audit file carries only logins, so resolve each included login directly
        # against the source environment rather than re-expanding the group tree.
        source_index = _collect_target_login_index(src_conn)  # {login: src_user_id}
        wanted = {login: source_index[login] for login in included if login in source_index}
        missing = included - set(wanted)
        if missing:
            logger.warning("{n} audit login(s) not found in source — skipping: {sample}",
                           n=len(missing), sample=sorted(missing)[:10])

        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
            futs = {pool.submit(_fetch_user_meta, src_conn, uid): login
                    for login, uid in wanted.items()}
            source_users = {}
            for fut in as_completed(futs):
                login = futs[fut]
                try:
                    source_users[login] = fut.result()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Could not fetch source user '{login}': {err}",
                                   login=login, err=exc)

        target_index = _collect_target_login_index(tgt_conn)
        target_group_ids = {g.id for g in list_user_groups(tgt_conn)}

        mode = "APPLY" if apply else "DRY-RUN"
        logger.info("=== UserSync apply [{mode}] — {n} user(s) ===", mode=mode, n=len(source_users))
        for src in source_users.values():
            res = _apply_one(tgt_conn, src, target_index, target_group_ids, apply)
            results.append(res)
            logger.info("  {action:6} {login}: {status} — {details}",
                        action=res["action"], login=res["login"],
                        status=res["status"], details=res["details"])
    finally:
        for c in (src_conn, tgt_conn):
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass

    rows = [[r["login"], r["action"], r["status"], r["details"]] for r in results]
    write_csv(rows, columns=["login", "action", "status", "details"], path=out_path)
    ok = sum(1 for r in results if r["status"] in ("created", "updated", "dry-run"))
    logger.success("apply {mode}: {ok}/{tot} ok → {path}",
                   mode=("APPLY" if apply else "DRY-RUN"), ok=ok, tot=len(results), path=out_path)
    return out_path


# ── CLI ──────────────────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Synchronize users across environments by direct create/update.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_audit = sub.add_parser("audit", help="Diff a source group tree vs the whole target env → xlsx.")
    p_audit.add_argument("source_env", choices=ENVS)
    p_audit.add_argument("target_env", choices=ENVS)
    p_audit.add_argument("--source-group", required=True,
                         help="Base user group in the source env (name or GUID).")
    p_audit.add_argument("--concurrency", type=int, default=10)
    p_audit.add_argument("--output-dir", type=Path, default=None, metavar="PATH")

    p_apply = sub.add_parser("apply", help="Create/update users from a reviewed audit file.")
    p_apply.add_argument("source_env", choices=ENVS)
    p_apply.add_argument("target_env", choices=ENVS)
    p_apply.add_argument("--audit-file", type=Path, required=True)
    p_apply.add_argument("--apply", action="store_true", help="Commit changes (otherwise dry-run).")
    p_apply.add_argument("--concurrency", type=int, default=10)
    p_apply.add_argument("--output-dir", type=Path, default=None, metavar="PATH")
    return parser


def main(argv: Optional[list] = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "audit":
        cmd_audit(args.source_env, args.target_env, args.source_group,
                  args.output_dir, args.concurrency)
    elif args.command == "apply":
        cmd_apply(args.source_env, args.target_env, args.audit_file,
                  args.apply, args.output_dir, args.concurrency)
    return 0


if __name__ == "__main__":
    sys.exit(main())
