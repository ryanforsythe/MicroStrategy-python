"""
StandardAuthManage.py — Manage standard authentication based on user group
membership.

Disables standard authentication for users who do NOT belong to the specified
user group (default: "Function Access: Standard Authentication").  Users who
are members of the group (directly or through nested groups) are skipped —
unless --enable-excepted is passed, in which case their standard_auth is
set to True if it is not already.

Group membership is resolved via the REST API flatMembers endpoint, which
returns the fully-expanded (recursive) member list — so nested user group
structures are correctly handled.

Configuration
─────────────
The excepted user group GUID is read from a script-specific env file
(StandardAuthManage.env in the script directory) and can be overridden at
runtime via --group-id:

    # StandardAuthManage.env
    STANDARD_AUTH_GROUP_ID=ABCDEF01234567890ABCDEF012345678

Concurrency
───────────
The ``list_users()`` API returns lightweight user objects that do not include
the ``standard_auth`` attribute.  Each user must be fetched individually via
``User(conn, id=...)`` to read and update that flag.  To keep run times
reasonable on large environments, the fetch and apply phases use a thread
pool.  The thread count is configurable:

    # StandardAuthManage.env
    CONCURRENCY=10

Override at runtime with ``--concurrency``.

Last-Run Tracking
─────────────────
When --apply is used, the script records the execution timestamp in the
same StandardAuthManage.env file as a per-environment variable:

    LAST_RUN_DEV=2026-04-22T14:30:00
    LAST_RUN_QA=2026-04-21T10:00:00

On subsequent runs, pass --since-last-run to process only users whose
date_modified is after the recorded timestamp.

The default is to scan ALL users (no date filter).  This is intentional:
users migrated from another environment may retain their original
date_modified from the source server, which could predate the last run
on the target environment and cause them to be incorrectly skipped.  Use
--since-last-run only when you are confident that the full user base has
already been reviewed at least once on this environment.

Standard Auth Logic
───────────────────
  User.standard_auth values:
      True  — standard authentication is explicitly allowed
      False — standard authentication is explicitly disabled
      None  — not set (inherits server default, which may allow it)

  For users NOT in the excepted group:
      True or None → set to False (disable)
      False        → skip (already disabled)

  For users IN the excepted group (with --enable-excepted):
      False or None → set to True (enable)
      True          → skip (already enabled)

Output columns:
    user_id              – User GUID
    user_name            – Login / username
    full_name            – Display name
    enabled              – Whether the user account is enabled
    in_excepted_group    – Whether the user is in the excepted group (flat)
    standard_auth_before – standard_auth value before this run
    standard_auth_after  – standard_auth value after (or planned) change
    action               – "disable" / "enable" / "skip"
    status               – "pending" (dry run) / "success" / "skip" / "error: ..."

Usage:
    python StandardAuthManage.py <env>  [--apply]
                                        [--group-id GUID]
                                        [--enabled-only]
                                        [--enable-excepted]
                                        [--since-last-run]
                                        [--concurrency N]
                                        [--output-dir PATH]

    # Preview — no changes (scans all users)
    python StandardAuthManage.py dev

    # Apply changes
    python StandardAuthManage.py prod --apply

    # Only scan enabled user accounts
    python StandardAuthManage.py dev --enabled-only

    # Also enable standard_auth for users IN the excepted group
    python StandardAuthManage.py dev --apply --enable-excepted

    # Only process users modified since the last --apply run
    python StandardAuthManage.py dev --apply --since-last-run

    # Override group ID and concurrency at runtime
    python StandardAuthManage.py dev --group-id ABC123 --concurrency 20

Run without --apply first to review the CSV output, then re-run with
--apply to commit the changes.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from dotenv import dotenv_values, set_key
from loguru import logger
from mstrio.users_and_groups import list_users, User

from mstrio_core import MstrConfig, MstrRestSession, write_csv
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]
DEFAULT_GROUP_NAME = "Function Access: Standard Authentication"
DEFAULT_CONCURRENCY = 10
OUTPUT_FILENAME = "standard_auth_manage.csv"

_SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPT_ENV_PATH = _SCRIPT_DIR / "StandardAuthManage.env"

COLUMNS = [
    "user_id",
    "user_name",
    "full_name",
    "enabled",
    "in_excepted_group",
    "standard_auth_before",
    "standard_auth_after",
    "action",
    "status",
]


# ── Helpers ───────────────────────────────────────────────────────────────────


def _load_group_id(cli_group_id: str | None) -> str:
    """
    Resolve the excepted user group GUID.

    Priority: CLI ``--group-id``  →  ``StandardAuthManage.env``  →  error.
    """
    if cli_group_id:
        logger.info("Using group ID from --group-id: {id}", id=cli_group_id)
        return cli_group_id

    if SCRIPT_ENV_PATH.exists():
        env_vals = dotenv_values(SCRIPT_ENV_PATH)
        gid = env_vals.get("STANDARD_AUTH_GROUP_ID", "").strip()
        if gid:
            logger.info(
                "Using group ID from {file}: {id}",
                file=SCRIPT_ENV_PATH.name,
                id=gid,
            )
            return gid

    raise ValueError(
        f"No group ID specified.  Pass --group-id on the CLI or set "
        f"STANDARD_AUTH_GROUP_ID in {SCRIPT_ENV_PATH}"
    )


def _load_concurrency(cli_value: int | None) -> int:
    """
    Resolve the thread-pool size for concurrent operations.

    Priority: CLI ``--concurrency``  →  ``StandardAuthManage.env``  →  default.
    """
    if cli_value is not None:
        return cli_value

    if SCRIPT_ENV_PATH.exists():
        env_vals = dotenv_values(SCRIPT_ENV_PATH)
        val = env_vals.get("CONCURRENCY", "").strip()
        if val:
            try:
                return int(val)
            except ValueError:
                logger.warning(
                    "Invalid CONCURRENCY={v!r} in {file} — using default {d}",
                    v=val,
                    file=SCRIPT_ENV_PATH.name,
                    d=DEFAULT_CONCURRENCY,
                )

    return DEFAULT_CONCURRENCY


def _get_flat_member_ids(session, group_id: str) -> set[str]:
    """
    Return the set of user IDs that belong to *group_id*, expanded
    recursively via the REST API ``flatMembers`` endpoint.

    Uses ``GET /api/usergroups/{id}/members?flatMembers=true`` which resolves
    nested group memberships and returns every individual user.

    Handles pagination for groups with many members.
    """
    member_ids: set[str] = set()
    offset = 0
    page_size = 5000

    while True:
        r = session.get(
            f"/usergroups/{group_id}/members",
            params={
                "flatMembers": "true",
                "offset": str(offset),
                "limit": str(page_size),
            },
            scope="server",
        )
        r.raise_for_status()
        data = r.json()

        # Response may be {"members": [...]} or a bare list depending on
        # I-Server version.
        members = data.get("members", data) if isinstance(data, dict) else data
        if not members:
            break

        for m in members:
            mid = m.get("id") if isinstance(m, dict) else None
            if mid:
                member_ids.add(mid)

        if len(members) < page_size:
            break
        offset += page_size

    logger.info(
        "Excepted group ({id}) has {n} flat member(s).",
        id=group_id,
        n=len(member_ids),
    )
    return member_ids


def _last_run_key(env: str) -> str:
    """Return the .env variable name for the last-run timestamp of *env*."""
    return f"LAST_RUN_{env.upper()}"


def _load_last_run(env: str) -> datetime | None:
    """Load the last-run timestamp for *env* from StandardAuthManage.env."""
    if not SCRIPT_ENV_PATH.exists():
        return None

    try:
        env_vals = dotenv_values(SCRIPT_ENV_PATH)
        ts_str = env_vals.get(_last_run_key(env), "").strip()
        if ts_str:
            return datetime.fromisoformat(ts_str)
    except Exception as exc:
        logger.warning(
            "Could not read last-run from {path}: {exc}",
            path=SCRIPT_ENV_PATH,
            exc=exc,
        )
    return None


def _save_last_run(env: str, ts: datetime) -> None:
    """Persist the execution timestamp for *env* into StandardAuthManage.env."""
    # Ensure the file exists so set_key can append to it
    if not SCRIPT_ENV_PATH.exists():
        SCRIPT_ENV_PATH.touch()

    key = _last_run_key(env)
    value = ts.isoformat()
    set_key(str(SCRIPT_ENV_PATH), key, value)
    logger.debug(
        "Last-run timestamp saved: {key}={value} → {path}",
        key=key,
        value=value,
        path=SCRIPT_ENV_PATH,
    )


def _parse_datetime(value) -> datetime | None:
    """
    Normalise a ``date_modified`` value to a datetime.

    mstrio-py may return a datetime object, an ISO-format string, or None.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        logger.warning("Unrecognised date format: {v!r}", v=value)
    return None


# ── Concurrent fetch / apply ─────────────────────────────────────────────────


def _fetch_user(conn, user_id: str, user_name: str) -> User | None:
    """
    Fetch the full User object (including ``standard_auth``) by ID.

    Returns None on failure so a single bad user does not abort the run.
    """
    try:
        return User(conn, id=user_id)
    except Exception as exc:
        logger.warning(
            "Could not fetch user {name} ({id}): {exc}",
            name=user_name,
            id=user_id,
            exc=exc,
        )
        return None


def _fetch_users_concurrent(
    conn,
    basic_users: list,
    max_workers: int,
) -> tuple[list[User], list[dict]]:
    """
    Fetch full User details concurrently via ``User(conn, id=...)``.

    ``list_users()`` returns lightweight objects without ``standard_auth``.
    This function enriches them by fetching each user individually in a
    thread pool.

    Returns:
        (detailed_users, failed_fetches)
        failed_fetches: list of ``{"id": ..., "name": ...}`` dicts for users
            that could not be fetched (included in the CSV as errors).
    """
    total = len(basic_users)
    detailed: list[User] = []
    failed: list[dict] = []

    logger.info(
        "Fetching full details for {n} user(s) ({w} concurrent threads) …",
        n=total,
        w=max_workers,
    )

    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_basic = {
            pool.submit(_fetch_user, conn, u.id, u.name): u
            for u in basic_users
        }
        for future in as_completed(future_to_basic):
            completed += 1
            basic = future_to_basic[future]
            result = future.result()
            if result is not None:
                detailed.append(result)
            else:
                failed.append({"id": basic.id, "name": basic.name})

            if completed % 100 == 0 or completed == total:
                logger.info(
                    "  Fetched {done}/{total} user(s) …",
                    done=completed,
                    total=total,
                )

    logger.info(
        "Fetch complete: {ok} succeeded, {fail} failed.",
        ok=len(detailed),
        fail=len(failed),
    )
    return detailed, failed


def _alter_user(user: User, standard_auth: bool) -> tuple[str, str]:
    """
    Set ``standard_auth`` on a single user.

    Returns ``(user_id, status)`` where status is ``"success"`` or
    ``"error: <message>"``.
    """
    try:
        user.alter(standard_auth=standard_auth)
        return user.id, "success"
    except Exception as exc:
        logger.error(
            "Failed to alter standard_auth for {name} ({id}): {exc}",
            name=user.name,
            id=user.id,
            exc=exc,
        )
        return user.id, f"error: {exc}"


def _apply_changes_concurrent(
    changes: list[tuple[User, bool]],
    max_workers: int,
) -> dict[str, str]:
    """
    Apply ``standard_auth`` changes concurrently.

    Args:
        changes:     List of ``(user, target_standard_auth)`` tuples.
        max_workers: Thread-pool size.

    Returns:
        Dict mapping ``user_id → status`` (``"success"`` or ``"error: …"``).
    """
    results: dict[str, str] = {}
    total = len(changes)
    if not total:
        return results

    logger.info(
        "Applying {n} change(s) ({w} concurrent threads) …",
        n=total,
        w=max_workers,
    )

    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_user = {
            pool.submit(_alter_user, user, target): user
            for user, target in changes
        }
        for future in as_completed(future_to_user):
            completed += 1
            uid, status = future.result()
            results[uid] = status

            if completed % 50 == 0 or completed == total:
                logger.info(
                    "  Applied {done}/{total} change(s) …",
                    done=completed,
                    total=total,
                )

    successes = sum(1 for s in results.values() if s == "success")
    errors = sum(1 for s in results.values() if s != "success")
    logger.info(
        "Apply complete: {ok} succeeded, {fail} failed.",
        ok=successes,
        fail=errors,
    )
    return results


# ── Main ──────────────────────────────────────────────────────────────────────


def main(
    env: str,
    dry_run: bool = True,
    group_id: str | None = None,
    enabled_only: bool = False,
    enable_excepted: bool = False,
    since_last_run: bool = False,
    concurrency: int | None = None,
    output_dir: Path | None = None,
) -> None:
    """
    Manage standard authentication for users based on group membership.

    Args:
        env:              Environment ("dev", "qa", or "prod").
        dry_run:          Preview only — no server changes (default).
        group_id:         Override the excepted group GUID from CLI.
        enabled_only:     When True, only retrieve enabled user accounts
                          (uses the ``enabled`` API parameter on list_users).
        enable_excepted:  When True, also enable standard_auth for users
                          inside the excepted group (default: skip them).
        since_last_run:   When True, only process users whose
                          date_modified > last recorded run timestamp.
        concurrency:      Thread-pool size for fetch/apply (default from
                          .env or 10).
        output_dir:       Output directory (default: MstrConfig.output_dir).
    """
    resolved_group_id = _load_group_id(group_id)
    max_workers = _load_concurrency(concurrency)
    config = MstrConfig(environment=MstrEnvironment(env))
    out_dir = output_dir or config.output_dir
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    # ── Last-run filter ──────────────────────────────────────────────────────
    last_run_ts: datetime | None = None
    if since_last_run:
        last_run_ts = _load_last_run(env)
        if last_run_ts:
            logger.info(
                "Filtering to users modified after {ts}",
                ts=last_run_ts.isoformat(),
            )
        else:
            logger.warning(
                "No last-run timestamp found for {env} — processing all users.",
                env=env,
            )

    run_start = datetime.now()

    with MstrRestSession(config) as session:
        conn = session.mstrio_conn

        # ── 1. Resolve excepted group members (flat) ─────────────────────────
        excepted_ids = _get_flat_member_ids(session, resolved_group_id)

        # ── 2. List users (lightweight — id, name, username only) ────────────
        if enabled_only:
            basic_users = list_users(conn, enabled=True)
            logger.info(
                "Retrieved {n} enabled user(s) from {env}.",
                n=len(basic_users),
                env=env,
            )
        else:
            basic_users = list_users(conn)
            logger.info(
                "Retrieved {n} total user(s) from {env}.",
                n=len(basic_users),
                env=env,
            )

        # ── 3. Fetch full user details concurrently ──────────────────────────
        #   list_users returns lightweight objects without standard_auth.
        #   Each user must be fetched individually to read that attribute.
        users, failed_fetches = _fetch_users_concurrent(
            conn, basic_users, max_workers,
        )

        # ── 4. Filter by date_modified if --since-last-run ───────────────────
        if last_run_ts:
            before = len(users)
            filtered = []
            for u in users:
                mod = _parse_datetime(getattr(u, "date_modified", None))
                if mod is None or mod > last_run_ts:
                    filtered.append(u)
            users = filtered
            logger.info(
                "{n} user(s) modified after last run ({s} skipped).",
                n=len(users),
                s=before - len(users),
            )

        # ── 5. Determine actions ─────────────────────────────────────────────
        #   Plan: (user, in_group, auth_before, auth_after, action_label)
        plan: list[tuple] = []

        for u in users:
            in_group = u.id in excepted_ids
            auth_before = getattr(u, "standard_auth", None)

            if in_group:
                if enable_excepted and auth_before is not True:
                    plan.append((u, True, auth_before, True, "enable"))
                else:
                    plan.append((u, True, auth_before, auth_before, "skip"))
            else:
                if auth_before is not False:
                    plan.append((u, False, auth_before, False, "disable"))
                else:
                    plan.append((u, False, auth_before, auth_before, "skip"))

        to_disable = sum(1 for *_, a in plan if a == "disable")
        to_enable = sum(1 for *_, a in plan if a == "enable")
        to_skip = sum(1 for *_, a in plan if a == "skip")

        logger.info(
            "Plan: {dis} to disable | {en} to enable | {skip} unchanged (skip)",
            dis=to_disable,
            en=to_enable,
            skip=to_skip,
        )

        # ── 6. Execute (or preview) ─────────────────────────────────────────
        #   Build the list of changes to apply, then run concurrently.
        apply_results: dict[str, str] = {}

        if not dry_run:
            changes: list[tuple[User, bool]] = [
                (u, auth_after)
                for u, _, _, auth_after, action in plan
                if action in ("disable", "enable")
            ]
            if changes:
                apply_results = _apply_changes_concurrent(changes, max_workers)

        # ── 7. Build CSV rows ────────────────────────────────────────────────
        rows: list[list] = []

        for u, in_group, auth_before, auth_after, action_label in plan:
            if action_label == "skip":
                status = "skip"
            elif dry_run:
                status = "pending"
            else:
                status = apply_results.get(u.id, "error: not executed")

            rows.append([
                u.id,
                u.name,
                getattr(u, "full_name", "") or "",
                str(getattr(u, "enabled", "")),
                str(in_group),
                str(auth_before),
                str(auth_after),
                action_label,
                status,
            ])

        # Include failed fetches in the CSV for auditability
        for f in failed_fetches:
            rows.append([
                f["id"],
                f["name"],
                "",           # full_name — unknown
                "",           # enabled — unknown
                str(f["id"] in excepted_ids),
                "",           # standard_auth_before — unknown
                "",           # standard_auth_after — unknown
                "error",
                "error: could not fetch user details",
            ])

        # ── 8. Write CSV ────────────────────────────────────────────────────
        out_path = Path(out_dir) / OUTPUT_FILENAME
        write_csv(rows, columns=COLUMNS, path=out_path)

        n_errors = (
            sum(1 for s in apply_results.values() if s != "success")
            + len(failed_fetches)
        )

        if dry_run:
            logger.success("Preview written → {p}", p=out_path)
            logger.info(
                "Dry run — no changes applied.  "
                "Review {p} then re-run with --apply to proceed.",
                p=out_path,
            )
            if failed_fetches:
                logger.warning(
                    "{n} user(s) could not be fetched — see CSV for details.",
                    n=len(failed_fetches),
                )
        else:
            _save_last_run(env, run_start)

            disabled = sum(
                1 for uid, s in apply_results.items() if s == "success"
                and any(
                    u.id == uid and a == "disable"
                    for u, _, _, _, a in plan
                )
            )
            enabled_ct = sum(
                1 for uid, s in apply_results.items() if s == "success"
                and any(
                    u.id == uid and a == "enable"
                    for u, _, _, _, a in plan
                )
            )

            logger.success(
                "Done.  Disabled {dis} | Enabled {en} | Skipped {skip} | "
                "Errors {err}.  Results → {path}",
                dis=disabled,
                en=enabled_ct,
                skip=to_skip,
                err=n_errors,
                path=out_path,
            )


# ── CLI ───────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Manage standard authentication for MicroStrategy users based on "
            "user group membership.  Disables standard_auth for users NOT in "
            f"the excepted group (default: \"{DEFAULT_GROUP_NAME}\")."
        ),
    )
    parser.add_argument(
        "env",
        choices=ENVS,
        help="Environment to process.",
    )
    parser.add_argument(
        "--group-id",
        default=None,
        metavar="GUID",
        help=(
            "GUID of the user group that grants standard authentication.  "
            f"Overrides STANDARD_AUTH_GROUP_ID in {SCRIPT_ENV_PATH.name}."
        ),
    )
    parser.add_argument(
        "--apply",
        dest="dry_run",
        action="store_false",
        default=True,
        help="Apply changes to the server (default: dry run — preview only).",
    )
    parser.add_argument(
        "--enabled-only",
        action="store_true",
        default=False,
        help="Only scan enabled user accounts (default: all users).",
    )
    parser.add_argument(
        "--enable-excepted",
        action="store_true",
        default=False,
        help=(
            "Also enable standard_auth for users IN the excepted group "
            "(default: excepted users are only used as a skip list)."
        ),
    )
    parser.add_argument(
        "--since-last-run",
        action="store_true",
        default=False,
        help=(
            "Only process users modified since the last --apply run.  "
            "Defaults to all users — migrated users may retain timestamps "
            "from the source environment and would be skipped incorrectly.  "
            "Use only after a full initial review has been completed."
        ),
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        metavar="N",
        help=(
            f"Number of concurrent threads for user fetch and apply "
            f"(default: CONCURRENCY in {SCRIPT_ENV_PATH.name} or "
            f"{DEFAULT_CONCURRENCY})."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        metavar="PATH",
        help="Output directory for the CSV (default: MSTR_OUTPUT_DIR or c:/tmp).",
    )

    args = parser.parse_args()
    main(
        env=args.env,
        dry_run=args.dry_run,
        group_id=args.group_id,
        enabled_only=args.enabled_only,
        enable_excepted=args.enable_excepted,
        since_last_run=args.since_last_run,
        concurrency=args.concurrency,
        output_dir=args.output_dir,
    )
