"""UserSync.py — Synchronize users across environments via a review-then-package flow.

Two-phase, audit-driven user migration between two MicroStrategy environments
(dev / qa / prod):

    1. audit   — Compare the members of a base user group in the SOURCE
                 environment against a group in the TARGET environment,
                 recursing through every sub user group. A user is flagged as a
                 diff when it is missing in the target, or when its source
                 Last-Modified time is newer than the target's. The diff is
                 written to an .xlsx audit file, one row per user, with a
                 `Target Action` column defaulting to "Update".

    2. package — Read that audit file, keep only rows whose `Target Action` is
                 still "Update" (any other value excludes the row), and build a
                 MicroStrategy object-migration package (.mmp) containing those
                 users, ready to import into the target environment.

Why the audit file drives the package
-------------------------------------
The reviewer edits the .xlsx between the two phases: change a row's
`Target Action` to anything other than "Update" (e.g. "Skip", "Exclude", blank)
and that user is dropped from the package. This gives a human gate before any
object is packaged for migration.

Package creation — version note
-------------------------------
The package is built on the SOURCE environment (that is where the user objects
live). Because the source is often a LOWER I-Server version, this script uses
the long-supported "package holder" REST API rather than the newer
`/api/migrations` storage-service flow (which requires 11.3.10+ and a shared
file store):

    POST   /api/packages                 -> create in-memory package holder
    PUT    /api/packages/{id}            -> fill holder from PackageConfig (async)
    GET    /api/packages/{id}            -> poll until status == created
    GET    /api/packages/{id}/binary     -> download the .mmp bytes
    DELETE /api/packages/{id}            -> release the holder

Users are configuration objects, so every call is server-scoped
(X-MSTR-ProjectID = None) and the package is a configuration/administration
package.

Usage
-----
    # Phase 1 — produce the audit file (read-only; always writes the xlsx)
    python UserSync.py audit <source_env> <target_env> \
        --source-group "Finance Users" [--target-group "Finance Users"] \
        [--output-dir PATH] [--concurrency N]

    # Phase 2 — build the .mmp from the (reviewed) audit file
    python UserSync.py package <source_env> --audit-file PATH [--apply] \
        [--output-dir PATH] [--action replace|use_newer|force_replace]

`package` is dry-run by default (lists what would be included); pass --apply to
create and download the .mmp. `--target-group` defaults to `--source-group`
when the group carries the same name/GUID in both environments.

Audit columns
-------------
    GUID, Name, Login, Last Modified Time, Created Time, Status, Target Action,
    Target Last Modified Time, Target GUID
    (GUID/Name/Login/times describe the SOURCE user; the package matches on GUID)

mstrio-py used
--------------
    UserGroup(conn, id=|name=).members        -> recurse group tree
    User(conn, id=).date_modified/.date_created/.username/.enabled
    PackageConfig / PackageSettings / PackageContentInfo / Action  (body shape)
    ObjectTypes.USER (34)                      -> content object type
"""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger
from mstrio.object_management.migration import (
    Action,
    PackageConfig,
    PackageContentInfo,
    PackageSettings,
)
from mstrio.types import ObjectTypes
from mstrio.users_and_groups import User, UserGroup

from mstrio_core import MstrConfig, get_mstrio_connection, read_excel, write_excel
from mstrio_core.config import MstrEnvironment

# ── Constants ───────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]

AUDIT_COLUMNS = [
    "GUID",                       # source user GUID (the object the package pulls)
    "Name",
    "Login",
    "Last Modified Time",         # source
    "Created Time",               # source
    "Status",
    "Target Action",              # editable gate: "Update" = include, else exclude
    "Target Last Modified Time",  # context for the reviewer
    "Target GUID",                # context (usually equals GUID on cloned envs)
]

DEFAULT_TARGET_ACTION = "Update"
INCLUDE_ACTION = "update"         # compared case-insensitively against Target Action

# Status classifications (only diffs are written to the audit file)
STATUS_MISSING = "MISSING_IN_TARGET"
STATUS_NEWER = "SOURCE_NEWER"

# Per-object action in the package. The audit already narrowed the set to users
# that need updating, so REPLACE is the sensible default; the package-level
# default_action stays USE_NEWER as a safety net.
CONTENT_ACTION = Action.REPLACE
PACKAGE_DEFAULT_ACTION = Action.USE_NEWER

# Package-holder polling
POLL_INTERVAL_SECONDS = 3
POLL_TIMEOUT_SECONDS = 180

_GUID_LEN = 32


# ── Small helpers ───────────────────────────────────────────────────────────


def _is_guid(identifier: str) -> bool:
    s = str(identifier).strip()
    return len(s) == _GUID_LEN and all(c in "0123456789abcdefABCDEF" for c in s)


def _resolve_group(conn, identifier: str) -> UserGroup:
    """Resolve a user group by 32-hex GUID or by name (auto-detected)."""
    identifier = str(identifier).strip()
    if _is_guid(identifier):
        return UserGroup(conn, id=identifier)
    return UserGroup(conn, name=identifier)


def _fmt_dt(value) -> str:
    """Format a datetime (or datetime-like string) as 'YYYY-MM-DD HH:MM:SS'."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _as_datetime(value) -> Optional[datetime]:
    """Best-effort coercion of a mstrio date value to a naive datetime for
    comparison. Returns None when unavailable/unparseable."""
    if value is None:
        return None
    if isinstance(value, datetime):
        # Drop tzinfo so source/target compare on the same (wall-clock UTC) basis.
        return value.replace(tzinfo=None)
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(value), fmt).replace(tzinfo=None)
        except (ValueError, TypeError):
            continue
    return None


# ── Group-tree expansion ─────────────────────────────────────────────────────


def _expand_member_user_ids(group: UserGroup, seen_groups: Optional[set] = None) -> set:
    """Return the set of member User IDs in a group and all its sub-groups.

    Sub user groups are recursed; a `seen_groups` guard prevents infinite loops
    on cyclic memberships. Only `User` members are collected — nested
    `UserGroup` members are traversal nodes, not results.
    """
    seen_groups = seen_groups if seen_groups is not None else set()
    user_ids: set = set()

    try:
        members = group.members
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Could not read members of group {gid}: {err}",
            gid=getattr(group, "id", "?"),
            err=exc,
        )
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
            # Fallback: distinguish by subtype when the SDK hands back a base object.
            sub = getattr(member, "subtype", None)
            mid = getattr(member, "id", None)
            if mid is None:
                continue
            if sub == 8705:  # USER_GROUP subtype
                if mid in seen_groups:
                    continue
                seen_groups.add(mid)
                user_ids |= _expand_member_user_ids(UserGroup(group.connection, id=mid),
                                                    seen_groups)
            else:
                user_ids.add(mid)

    return user_ids


def _fetch_user_meta(conn, user_id: str) -> dict:
    """Fetch authoritative identity + timestamps for one user."""
    u = User(conn, id=user_id)
    return {
        "id": u.id,
        "name": u.name or "",
        "username": (getattr(u, "username", "") or ""),
        "date_modified": getattr(u, "date_modified", None),
        "date_created": getattr(u, "date_created", None),
        "enabled": getattr(u, "enabled", None),
    }


def _collect_users(conn, group_identifier: str, concurrency: int, label: str) -> dict:
    """Expand a group tree and return {match_key: meta} for all member users.

    match_key is the lowercased login (username); users without a login fall
    back to a GUID-based key so they can still surface in the audit.
    """
    group = _resolve_group(conn, group_identifier)
    logger.info("[{label}] Resolving group '{grp}' → {gid} ({name})",
                label=label, grp=group_identifier, gid=group.id, name=group.name)

    user_ids = _expand_member_user_ids(group)
    logger.info("[{label}] {n} distinct member user(s) across the group tree.",
                label=label, n=len(user_ids))

    by_key: dict = {}
    if not user_ids:
        return by_key

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        futures = {pool.submit(_fetch_user_meta, conn, uid): uid for uid in user_ids}
        for fut in as_completed(futures):
            uid = futures[fut]
            try:
                meta = fut.result()
            except Exception as exc:  # noqa: BLE001
                logger.warning("[{label}] Could not fetch user {uid}: {err}",
                               label=label, uid=uid, err=exc)
                continue
            login = meta["username"].strip().lower()
            key = login if login else f"guid:{meta['id']}"
            by_key[key] = meta

    logger.success("[{label}] Loaded metadata for {n} user(s).", label=label, n=len(by_key))
    return by_key


# ── Phase 1: audit ───────────────────────────────────────────────────────────


def cmd_audit(
    source_env: str,
    target_env: str,
    source_group: str,
    target_group: Optional[str],
    output_dir: Optional[Path],
    concurrency: int,
) -> Path:
    """Compare source vs target group trees and write the diff audit .xlsx."""
    target_group = target_group or source_group

    src_config = MstrConfig(environment=MstrEnvironment(source_env))
    tgt_config = MstrConfig(environment=MstrEnvironment(target_env))
    out_dir = output_dir or src_config.output_dir
    out_path = Path(out_dir) / f"usersync_audit_{source_env}_to_{target_env}.xlsx"

    src_conn = get_mstrio_connection(config=src_config)
    tgt_conn = get_mstrio_connection(config=tgt_config)
    try:
        source_users = _collect_users(src_conn, source_group, concurrency, "SOURCE")
        target_users = _collect_users(tgt_conn, target_group, concurrency, "TARGET")
    finally:
        for c in (src_conn, tgt_conn):
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass

    rows: list[list] = []
    n_missing = n_newer = 0
    for key, src in source_users.items():
        tgt = target_users.get(key)
        src_mod = _as_datetime(src["date_modified"])

        if tgt is None:
            status = STATUS_MISSING
            n_missing += 1
            tgt_mod_display, tgt_guid = "", ""
        else:
            tgt_mod = _as_datetime(tgt["date_modified"])
            # Diff only when the source copy is strictly newer than the target.
            if src_mod is not None and (tgt_mod is None or src_mod > tgt_mod):
                status = STATUS_NEWER
                n_newer += 1
                tgt_mod_display = _fmt_dt(tgt["date_modified"])
                tgt_guid = tgt["id"]
            else:
                continue  # in sync (target same or newer) — not a diff

        rows.append([
            src["id"],
            src["name"],
            src["username"],
            _fmt_dt(src["date_modified"]),
            _fmt_dt(src["date_created"]),
            status,
            DEFAULT_TARGET_ACTION,
            tgt_mod_display,
            tgt_guid,
        ])

    # Stable, reviewer-friendly ordering: missing first, then newest source edits.
    rows.sort(key=lambda r: (r[5] != STATUS_MISSING, r[3]), reverse=False)

    write_excel(rows, path=out_path, columns=AUDIT_COLUMNS, sheet_name="UserSync")
    logger.success(
        "Audit complete: {total} diff user(s) — {miss} missing in target, "
        "{newer} newer in source → {path}",
        total=len(rows), miss=n_missing, newer=n_newer, path=out_path,
    )
    if not rows:
        logger.info("No differences found — source group tree is in sync with target.")
    return out_path


# ── Phase 2: package ─────────────────────────────────────────────────────────


def _read_included_guids(audit_file: Path) -> list[tuple[str, str]]:
    """Read the audit file and return [(guid, name)] for rows whose
    `Target Action` is still 'Update' (case-insensitive). Any other value
    (including blank) excludes the row."""
    df = read_excel(audit_file)

    # Case-insensitive header resolution so hand-edited files still work.
    cols = {str(c).strip().lower(): c for c in df.columns}
    guid_col = cols.get("guid")
    action_col = cols.get("target action")
    name_col = cols.get("name")
    if guid_col is None or action_col is None:
        raise ValueError(
            "Audit file must contain 'GUID' and 'Target Action' columns; found: "
            f"{list(df.columns)}"
        )

    included: list[tuple[str, str]] = []
    skipped = 0
    for _, row in df.iterrows():
        guid = str(row[guid_col]).strip()
        action = str(row[action_col]).strip().lower()
        if not guid or guid.lower() == "nan":
            continue
        if action == INCLUDE_ACTION:
            name = str(row[name_col]).strip() if name_col is not None else ""
            included.append((guid, name))
        else:
            skipped += 1

    logger.info("Audit file: {inc} user(s) marked '{act}', {skip} excluded.",
                inc=len(included), act=DEFAULT_TARGET_ACTION, skip=skipped)
    return included


def _build_package_config(guids: list[str], action: Action) -> PackageConfig:
    settings = PackageSettings(
        default_action=PACKAGE_DEFAULT_ACTION,
        acl_on_replacing_objects=PackageSettings.AclOnReplacingObjects.USE_EXISTING,
        acl_on_new_objects=PackageSettings.AclOnNewObjects.KEEP_ACL_AS_SOURCE_OBJECT,
    )
    content = [
        PackageContentInfo(id=guid, action=action, type=ObjectTypes.USER)
        for guid in guids
    ]
    return PackageConfig(settings=settings, content=content)


def _create_mmp(conn, config: PackageConfig, out_path: Path) -> Path:
    """Run the package-holder REST flow on the source env and write the .mmp."""
    hdr = {"X-MSTR-ProjectID": None}          # configuration package → server scope
    hdr_async = {**hdr, "Prefer": "respond-async"}

    # 1) create holder
    r = conn.post(endpoint="/api/packages", headers=hdr)
    if not r.ok:
        raise RuntimeError(f"Create package holder failed: HTTP {r.status_code} {r.text}")
    pkg_id = r.json()["id"]
    logger.info("Package holder created: {id}", id=pkg_id)

    try:
        # 2) fill holder (async)
        body = config.to_dict()
        r = conn.put(endpoint=f"/api/packages/{pkg_id}", headers=hdr_async, json=body)
        if not r.ok:
            raise RuntimeError(f"Fill package failed: HTTP {r.status_code} {r.text}")

        # 3) poll until created
        deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
        status = None
        while time.monotonic() < deadline:
            r = conn.get(endpoint=f"/api/packages/{pkg_id}",
                         headers=hdr, params={"showContent": False})
            status = (r.json().get("status") or "").lower() if r.ok else None
            logger.debug("Package {id} status: {st}", id=pkg_id, st=status)
            if status == "created":
                break
            if status in ("create_failed", "empty"):
                raise RuntimeError(f"Package creation ended with status '{status}'.")
            time.sleep(POLL_INTERVAL_SECONDS)
        else:
            raise TimeoutError(
                f"Package {pkg_id} not created within {POLL_TIMEOUT_SECONDS}s "
                f"(last status: {status})."
            )

        # 4) download binary
        r = conn.get(endpoint=f"/api/packages/{pkg_id}/binary", headers=hdr)
        if not r.ok:
            raise RuntimeError(f"Download package failed: HTTP {r.status_code} {r.text}")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(r.content)
        logger.success("Wrote migration package: {path} ({size} bytes)",
                       path=out_path, size=len(r.content))
        return out_path

    finally:
        # 5) release holder (best effort)
        try:
            conn.delete(endpoint=f"/api/packages/{pkg_id}", headers=hdr_async)
            logger.debug("Released package holder {id}", id=pkg_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not release package holder {id}: {err}",
                           id=pkg_id, err=exc)


def cmd_package(
    source_env: str,
    audit_file: Path,
    apply: bool,
    output_dir: Optional[Path],
    action: Action,
) -> Optional[Path]:
    """Read the audit file and build a .mmp from the included users."""
    included = _read_included_guids(Path(audit_file))
    if not included:
        logger.warning("No users marked '{act}' in the audit file — nothing to package.",
                       act=DEFAULT_TARGET_ACTION)
        return None

    guids = [g for g, _ in included]
    mode = "APPLY" if apply else "DRY-RUN"
    logger.info("=== Build user migration package [{mode}] — {n} user(s) ===",
                mode=mode, n=len(guids))
    for guid, name in included:
        logger.info("  include {name} ({guid})", name=name or "?", guid=guid)

    if not apply:
        logger.info("Dry-run: no package created. Re-run with --apply to build the .mmp.")
        return None

    config = MstrConfig(environment=MstrEnvironment(source_env))
    out_dir = output_dir or config.output_dir
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = Path(out_dir) / f"usersync_{source_env}_{ts}.mmp"

    conn = get_mstrio_connection(config=config)
    try:
        pkg_config = _build_package_config(guids, action)
        return _create_mmp(conn, pkg_config, out_path)
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


# ── CLI ──────────────────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Synchronize users across environments: audit group-tree "
                    "diffs, then package the reviewed users into an .mmp.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # audit
    p_audit = sub.add_parser(
        "audit",
        help="Compare a source group tree vs a target group tree → diff .xlsx.",
    )
    p_audit.add_argument("source_env", choices=ENVS, help="Source environment.")
    p_audit.add_argument("target_env", choices=ENVS, help="Target environment.")
    p_audit.add_argument("--source-group", required=True,
                         help="Base user group in the source env (name or GUID).")
    p_audit.add_argument("--target-group", default=None,
                         help="Group in the target env (name or GUID). "
                              "Defaults to --source-group.")
    p_audit.add_argument("--concurrency", type=int, default=10,
                         help="Parallel user fetches per environment (default 10).")
    p_audit.add_argument("--output-dir", type=Path, default=None, metavar="PATH",
                         help="Output directory (default: MSTR_OUTPUT_DIR or c:/tmp).")

    # package
    p_pkg = sub.add_parser(
        "package",
        help="Build an .mmp from users marked 'Update' in the audit file.",
    )
    p_pkg.add_argument("source_env", choices=ENVS,
                       help="Environment the package is built on (where users live).")
    p_pkg.add_argument("--audit-file", type=Path, required=True,
                       help="Path to the reviewed audit .xlsx from the 'audit' phase.")
    p_pkg.add_argument("--action", choices=["replace", "use_newer", "force_replace"],
                       default="replace",
                       help="Per-user conflict action in the package (default replace).")
    p_pkg.add_argument("--apply", action="store_true",
                       help="Create and download the .mmp (otherwise dry-run).")
    p_pkg.add_argument("--output-dir", type=Path, default=None, metavar="PATH",
                       help="Output directory (default: MSTR_OUTPUT_DIR or c:/tmp).")

    return parser


def main(argv: Optional[list] = None) -> int:
    args = _build_parser().parse_args(argv)

    if args.command == "audit":
        cmd_audit(
            source_env=args.source_env,
            target_env=args.target_env,
            source_group=args.source_group,
            target_group=args.target_group,
            output_dir=args.output_dir,
            concurrency=args.concurrency,
        )
    elif args.command == "package":
        cmd_package(
            source_env=args.source_env,
            audit_file=args.audit_file,
            apply=args.apply,
            output_dir=args.output_dir,
            action=Action(args.action),
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
