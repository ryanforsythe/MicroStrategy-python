"""
UserGroupMemberManage.py — Bulk add or remove users from MicroStrategy user groups.

Accepts user identifiers (login usernames or GUIDs) from the command line or a
CSV file, and one or more user group targets.  Users can be added to or removed
from the specified groups.

Input modes
───────────
  --users LOGIN_OR_ID [...]   Pass user logins or GUIDs directly on the CLI.
  --csv   PATH                Read from a CSV file.  The file must contain a
                              column for the user identifier (any of: user,
                              login, username, user_id, id, guid).  An optional
                              group column (group_id, group, user_group,
                              user_group_id) overrides --group per-row.
  --excel PATH                Read from an Excel (.xlsx) file.  Same column
                              name rules as --csv.

Group resolution
────────────────
  Groups are specified by name or GUID via --group NAME_OR_ID [...]
  (required when using --users; optional with --csv if the CSV contains a
  group column).

  If the value is a 32-character hex string it is treated as a GUID; otherwise
  it is resolved by name.

User resolution
───────────────
  All users are fetched once via list_users() and indexed in memory.  If the
  input looks like a 32-character hex GUID, it is matched by ID; otherwise it
  is matched by username (case-insensitive).

Concurrency
───────────
  Group membership operations (add_users / remove_users) are performed
  concurrently across (user, group) pairs.  The default thread count is 10;
  override with --concurrency.

Output columns:
    user_id      – User GUID (or input value if unresolved)
    user_name    – Username / login
    user_input   – Original input value from CLI or CSV
    group_id     – User group GUID (or input value if unresolved)
    group_name   – User group name
    group_input  – Original group value from CLI or CSV
    action       – "add" / "remove"
    status       – "pending" (dry run) / "success" / "already_member" /
                   "not_member" / "error: ..." / "unresolved_user" /
                   "unresolved_group"

Usage:
    python UserGroupMemberManage.py add    <env> --users USER [USER ...] --group GROUP [GROUP ...]
                                      [--apply] [--concurrency N] [--output-dir PATH]

    python UserGroupMemberManage.py remove <env> --users USER [USER ...] --group GROUP [GROUP ...]
                                      [--apply] [--concurrency N] [--output-dir PATH]

    python UserGroupMemberManage.py add    <env> --csv PATH --group GROUP [GROUP ...]
                                      [--apply] [--concurrency N] [--output-dir PATH]

    python UserGroupMemberManage.py add    <env> --csv PATH
                                      [--apply] [--concurrency N] [--output-dir PATH]
                                      # (group_id column in CSV)

    python UserGroupMemberManage.py add    <env> --excel PATH --group GROUP [GROUP ...]
                                      [--apply] [--concurrency N] [--output-dir PATH]

Examples:
    # Preview — add two users to a group by login
    python UserGroupMemberManage.py add dev --users jsmith agarcia --group "Analysts"

    # Apply — add users by GUID to multiple groups
    python UserGroupMemberManage.py add prod --users ABC123 DEF456 \\
        --group "Analysts" "Report Viewers" --apply

    # Remove users listed in a CSV
    python UserGroupMemberManage.py remove qa --csv users_to_remove.csv \\
        --group "Old Group" --apply

    # Add users from CSV that includes a group_id column
    python UserGroupMemberManage.py add prod --csv bulk_assignments.csv --apply

    # Read from an Excel file
    python UserGroupMemberManage.py add prod --excel users.xlsx --group "Analysts" --apply

Run without --apply first to review the CSV output, then re-run with
--apply to commit the changes.
"""

import argparse
import csv
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from loguru import logger
from mstrio.users_and_groups import User, UserGroup, list_users

from mstrio_core import MstrConfig, get_mstrio_connection, read_excel, write_csv
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]
DEFAULT_CONCURRENCY = 10
OUTPUT_FILENAME = "user_group_member_manage.csv"

_GUID_RE = re.compile(r"^[0-9A-Fa-f]{32}$")

# Column name aliases accepted in the CSV for the user identifier
_USER_COL_ALIASES = {"user", "login", "username", "user_id", "id", "guid"}
# Column name aliases accepted in the CSV for the group identifier
_GROUP_COL_ALIASES = {"group_id", "group", "user_group", "user_group_id"}

COLUMNS = [
    "user_id",
    "user_name",
    "user_input",
    "group_id",
    "group_name",
    "group_input",
    "action",
    "status",
]


# ── Helpers ───────────────────────────────────────────────────────────────────


def _is_guid(value: str) -> bool:
    """Return True if *value* looks like a 32-character hex GUID."""
    return bool(_GUID_RE.match(value.strip()))


def _build_user_lookup(conn) -> tuple[dict, dict]:
    """
    Fetch all users once and return two lookup dicts:

        id_map:       {uppercase_GUID: User}
        username_map: {lowercase_username: User}

    ``list_users()`` returns lightweight User objects with ``id``, ``name``,
    and ``username`` — sufficient for ``add_users`` / ``remove_users``.
    """
    all_users = list_users(conn)
    logger.info("Loaded {n} user(s) for lookup.", n=len(all_users))

    id_map: dict[str, User] = {}
    username_map: dict[str, User] = {}
    for u in all_users:
        id_map[u.id.upper()] = u
        # u.name is the login/username in mstrio-py
        if u.name:
            username_map[u.name.lower()] = u

    return id_map, username_map


def _resolve_user(
    value: str,
    id_map: dict[str, User],
    username_map: dict[str, User],
) -> User | None:
    """
    Resolve a user input value to a User object.

    If *value* looks like a GUID, look up by ID; otherwise look up by
    username (case-insensitive).
    """
    value = value.strip()
    if _is_guid(value):
        return id_map.get(value.upper())
    return username_map.get(value.lower())


def _resolve_group(conn, value: str) -> UserGroup | None:
    """
    Resolve a group input to a UserGroup object.

    If *value* is a 32-hex GUID, resolve by ID; otherwise resolve by name.
    Returns None on failure.
    """
    value = value.strip()
    try:
        if _is_guid(value):
            return UserGroup(conn, id=value)
        return UserGroup(conn, name=value)
    except Exception as exc:
        logger.warning(
            "Could not resolve user group {v!r}: {exc}",
            v=value,
            exc=exc,
        )
        return None


def _find_csv_column(headers: list[str], aliases: set[str]) -> str | None:
    """
    Find the first CSV header that matches one of *aliases* (case-insensitive).
    """
    for h in headers:
        if h.strip().lower() in aliases:
            return h
    return None


def _read_csv_pairs(
    csv_path: Path,
    cli_groups: list[str] | None,
) -> list[tuple[str, str]]:
    """
    Read (user_input, group_input) pairs from a CSV file.

    If the CSV has a group column, those values are used per row.
    Otherwise, *cli_groups* supplies the group(s) for every row (each user
    is paired with every CLI-supplied group).

    Returns a list of ``(user_value, group_value)`` strings.
    """
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        # Auto-detect delimiter: semicolon (write_csv default) or comma.
        # Excel "Save As CSV" often produces files that csv.Sniffer cannot
        # parse (single-column or simple comma-only layout), so fall back
        # to comma — the most common Excel CSV dialect.
        sample = fh.read(4096)
        fh.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
        except csv.Error:
            logger.debug(
                "csv.Sniffer could not detect delimiter — defaulting to comma."
            )
            dialect = "excel"  # Python's built-in Excel CSV dialect (comma)
        reader = csv.DictReader(fh, dialect=dialect)

        headers = reader.fieldnames or []
        user_col = _find_csv_column(headers, _USER_COL_ALIASES)
        group_col = _find_csv_column(headers, _GROUP_COL_ALIASES)

        if not user_col:
            raise ValueError(
                f"CSV must contain a user column (one of: "
                f"{', '.join(sorted(_USER_COL_ALIASES))}). "
                f"Found columns: {headers}"
            )

        if not group_col and not cli_groups:
            raise ValueError(
                "No group column found in CSV and no --group specified.  "
                "Either add a group column to the CSV or pass --group."
            )

        pairs: list[tuple[str, str]] = []
        for row in reader:
            user_val = (row.get(user_col) or "").strip()
            if not user_val:
                continue

            if group_col:
                g_val = (row.get(group_col) or "").strip()
                if g_val:
                    pairs.append((user_val, g_val))
                elif cli_groups:
                    # Row has no group value — fall back to CLI groups
                    for g in cli_groups:
                        pairs.append((user_val, g))
            else:
                for g in cli_groups:  # type: ignore[union-attr]
                    pairs.append((user_val, g))

        logger.info(
            "Read {n} (user, group) pair(s) from {path}.",
            n=len(pairs),
            path=csv_path,
        )
        return pairs


def _read_excel_pairs(
    excel_path: Path,
    cli_groups: list[str] | None,
) -> list[tuple[str, str]]:
    """
    Read (user_input, group_input) pairs from an Excel (.xlsx) file.

    Column name resolution follows the same aliases as CSV.
    All cell values are cast to string and stripped.
    """
    df = read_excel(excel_path)

    # Normalise column lookup
    headers = list(df.columns)
    user_col = _find_csv_column(headers, _USER_COL_ALIASES)
    group_col = _find_csv_column(headers, _GROUP_COL_ALIASES)

    if not user_col:
        raise ValueError(
            f"Excel file must contain a user column (one of: "
            f"{', '.join(sorted(_USER_COL_ALIASES))}). "
            f"Found columns: {headers}"
        )

    if not group_col and not cli_groups:
        raise ValueError(
            "No group column found in Excel file and no --group specified.  "
            "Either add a group column or pass --group."
        )

    pairs: list[tuple[str, str]] = []
    for _, row in df.iterrows():
        user_val = str(row.get(user_col) or "").strip()
        if not user_val or user_val.lower() == "nan":
            continue

        if group_col:
            g_val = str(row.get(group_col) or "").strip()
            if g_val and g_val.lower() != "nan":
                pairs.append((user_val, g_val))
            elif cli_groups:
                for g in cli_groups:
                    pairs.append((user_val, g))
        else:
            for g in cli_groups:  # type: ignore[union-attr]
                pairs.append((user_val, g))

    logger.info(
        "Read {n} (user, group) pair(s) from {path}.",
        n=len(pairs),
        path=excel_path,
    )
    return pairs


# ── Concurrent membership operations ────────────────────────────────────────


def _add_user_to_group(
    group: UserGroup, user: User
) -> tuple[str, str, str]:
    """
    Add a single user to a group.

    Returns ``(user_id, group_id, status)``.
    """
    try:
        group.add_users(users=[user])
        return user.id, group.id, "success"
    except Exception as exc:
        msg = str(exc)
        # Some versions return a message when the user is already a member
        if "already" in msg.lower():
            return user.id, group.id, "already_member"
        logger.error(
            "Failed to add user {uname} ({uid}) to group {gname} ({gid}): {exc}",
            uname=user.name,
            uid=user.id,
            gname=group.name,
            gid=group.id,
            exc=exc,
        )
        return user.id, group.id, f"error: {exc}"


def _remove_user_from_group(
    group: UserGroup, user: User
) -> tuple[str, str, str]:
    """
    Remove a single user from a group.

    Returns ``(user_id, group_id, status)``.
    """
    try:
        group.remove_users(users=[user])
        return user.id, group.id, "success"
    except Exception as exc:
        msg = str(exc)
        if "not a member" in msg.lower() or "not found" in msg.lower():
            return user.id, group.id, "not_member"
        logger.error(
            "Failed to remove user {uname} ({uid}) from group {gname} ({gid}): {exc}",
            uname=user.name,
            uid=user.id,
            gname=group.name,
            gid=group.id,
            exc=exc,
        )
        return user.id, group.id, f"error: {exc}"


def _apply_concurrent(
    action: str,
    tasks: list[tuple[UserGroup, User]],
    max_workers: int,
) -> dict[tuple[str, str], str]:
    """
    Execute add or remove operations concurrently.

    Args:
        action:      ``"add"`` or ``"remove"``.
        tasks:       List of ``(group, user)`` tuples.
        max_workers: Thread-pool size.

    Returns:
        Dict mapping ``(user_id, group_id) → status``.
    """
    results: dict[tuple[str, str], str] = {}
    total = len(tasks)
    if not total:
        return results

    fn = _add_user_to_group if action == "add" else _remove_user_from_group

    logger.info(
        "Applying {n} {action} operation(s) ({w} concurrent threads) ...",
        n=total,
        action=action,
        w=max_workers,
    )

    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {
            pool.submit(fn, grp, usr): (usr, grp) for grp, usr in tasks
        }
        for future in as_completed(future_map):
            completed += 1
            uid, gid, status = future.result()
            results[(uid, gid)] = status

            if completed % 50 == 0 or completed == total:
                logger.info(
                    "  Completed {done}/{total} operation(s) ...",
                    done=completed,
                    total=total,
                )

    successes = sum(1 for s in results.values() if s == "success")
    errors = sum(1 for s in results.values() if s.startswith("error"))
    logger.info(
        "Apply complete: {ok} succeeded, {err} failed, {other} other.",
        ok=successes,
        err=errors,
        other=total - successes - errors,
    )
    return results


# ── Main ──────────────────────────────────────────────────────────────────────


def main(
    action: str,
    env: str,
    users_cli: list[str] | None = None,
    csv_path: Path | None = None,
    excel_path: Path | None = None,
    groups_cli: list[str] | None = None,
    dry_run: bool = True,
    concurrency: int = DEFAULT_CONCURRENCY,
    output_dir: Path | None = None,
) -> None:
    """
    Bulk add or remove users from user groups.

    Args:
        action:      ``"add"`` or ``"remove"``.
        env:         Environment to connect to (``"dev"``, ``"qa"``, ``"prod"``).
        users_cli:   User logins or GUIDs from the CLI (mutually exclusive
                     with *csv_path* and *excel_path*).
        csv_path:    Path to a CSV file with user (and optionally group) columns.
        excel_path:  Path to an Excel (.xlsx) file (same column rules as CSV).
        groups_cli:  User group names or GUIDs from the CLI.
        dry_run:     When True (default), write preview CSV only.
        concurrency: Thread-pool size.
        output_dir:  Directory for the output CSV.
    """
    config = MstrConfig(environment=MstrEnvironment(env))
    conn = get_mstrio_connection(config=config)
    out_dir = output_dir or config.output_dir
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    try:
        # ── 1. Build (user_input, group_input) pairs ────────────────────────
        if csv_path:
            pairs = _read_csv_pairs(csv_path, groups_cli)
        elif excel_path:
            pairs = _read_excel_pairs(excel_path, groups_cli)
        else:
            if not groups_cli:
                raise ValueError("--group is required when using --users.")
            pairs = [
                (u, g) for u in (users_cli or []) for g in groups_cli
            ]

        if not pairs:
            logger.warning("No (user, group) pairs to process — nothing to do.")
            return

        # Deduplicate — preserve first occurrence order
        seen: set[tuple[str, str]] = set()
        unique_pairs: list[tuple[str, str]] = []
        for pair in pairs:
            key = (pair[0].upper() if _is_guid(pair[0]) else pair[0].lower(),
                   pair[1].upper() if _is_guid(pair[1]) else pair[1].lower())
            if key not in seen:
                seen.add(key)
                unique_pairs.append(pair)

        if len(unique_pairs) < len(pairs):
            logger.info(
                "Deduplicated {orig} pair(s) → {dedup} unique.",
                orig=len(pairs),
                dedup=len(unique_pairs),
            )
        pairs = unique_pairs

        # ── 2. Build user lookup ────────────────────────────────────────────
        id_map, username_map = _build_user_lookup(conn)

        # ── 3. Resolve groups (cache to avoid re-fetching) ──────────────────
        group_inputs = list({p[1] for p in pairs})
        group_cache: dict[str, UserGroup | None] = {}
        for g_input in group_inputs:
            grp = _resolve_group(conn, g_input)
            if grp:
                logger.info(
                    "Resolved group: {name} ({id}) from input {inp!r}",
                    name=grp.name,
                    id=grp.id,
                    inp=g_input,
                )
            group_cache[g_input] = grp

        # ── 4. Resolve users and build action plan ──────────────────────────
        plan: list[dict] = []
        tasks_to_apply: list[tuple[UserGroup, User]] = []

        for user_input, group_input in pairs:
            user = _resolve_user(user_input, id_map, username_map)
            group = group_cache.get(group_input)

            row = {
                "user_input": user_input,
                "group_input": group_input,
                "action": action,
            }

            if not user:
                row.update({
                    "user_id": user_input,
                    "user_name": "",
                    "group_id": group.id if group else group_input,
                    "group_name": group.name if group else "",
                    "status": "unresolved_user",
                })
                logger.warning(
                    "Could not resolve user: {v!r}", v=user_input
                )
            elif not group:
                row.update({
                    "user_id": user.id,
                    "user_name": user.name,
                    "group_id": group_input,
                    "group_name": "",
                    "status": "unresolved_group",
                })
                logger.warning(
                    "Could not resolve group: {v!r}", v=group_input
                )
            else:
                row.update({
                    "user_id": user.id,
                    "user_name": user.name,
                    "group_id": group.id,
                    "group_name": group.name,
                    "status": "pending",
                })
                tasks_to_apply.append((group, user))

            plan.append(row)

        unresolved = sum(
            1 for r in plan
            if r["status"] in ("unresolved_user", "unresolved_group")
        )
        logger.info(
            "Plan: {total} pair(s) — {ok} resolvable, {bad} unresolved.",
            total=len(plan),
            ok=len(tasks_to_apply),
            bad=unresolved,
        )

        # ── 5. Execute (or preview) ─────────────────────────────────────────
        apply_results: dict[tuple[str, str], str] = {}

        if not dry_run and tasks_to_apply:
            apply_results = _apply_concurrent(
                action, tasks_to_apply, concurrency
            )

        # ── 6. Build CSV rows ───────────────────────────────────────────────
        rows: list[list] = []
        for r in plan:
            if r["status"] == "pending" and not dry_run:
                status = apply_results.get(
                    (r["user_id"], r["group_id"]),
                    "error: not executed",
                )
            else:
                status = r["status"]

            rows.append([
                r["user_id"],
                r["user_name"],
                r["user_input"],
                r["group_id"],
                r["group_name"],
                r["group_input"],
                r["action"],
                status,
            ])

        # ── 7. Write CSV ───────────────────────────────────────────────────
        out_path = Path(out_dir) / OUTPUT_FILENAME
        write_csv(rows, columns=COLUMNS, path=out_path)

        if dry_run:
            logger.success("Preview written -> {p}", p=out_path)
            logger.info(
                "Dry run — no changes applied.  "
                "Review {p} then re-run with --apply to proceed.",
                p=out_path,
            )
        else:
            successes = sum(
                1 for s in apply_results.values() if s == "success"
            )
            errors = sum(
                1 for s in apply_results.values() if s.startswith("error")
            )
            logger.success(
                "Done.  {action}: {ok} succeeded | {err} errors | "
                "{unres} unresolved.  Results -> {path}",
                action=action.capitalize(),
                ok=successes,
                err=errors,
                unres=unresolved,
                path=out_path,
            )

    finally:
        conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Bulk add or remove users from MicroStrategy user groups.  "
            "Accepts user logins or GUIDs from the CLI, a CSV file, "
            "or an Excel (.xlsx) file."
        ),
    )
    subparsers = parser.add_subparsers(dest="action", required=True)

    for action_name in ("add", "remove"):
        sp = subparsers.add_parser(
            action_name,
            help=f"{action_name.capitalize()} users to/from user group(s).",
        )
        sp.add_argument(
            "env",
            choices=ENVS,
            help="Environment to process.",
        )

        input_group = sp.add_mutually_exclusive_group(required=True)
        input_group.add_argument(
            "--users",
            nargs="+",
            metavar="LOGIN_OR_ID",
            help=(
                "One or more user logins or GUIDs.  "
                "32-hex-character values are treated as GUIDs; "
                "all others as login usernames."
            ),
        )
        input_group.add_argument(
            "--csv",
            type=Path,
            metavar="PATH",
            help=(
                "Path to a CSV file with a user column "
                "(user, login, username, user_id, id, or guid).  "
                "An optional group column (group_id, group, user_group, "
                "user_group_id) overrides --group per-row.  "
                "Works with Excel 'Save As CSV' files."
            ),
        )
        input_group.add_argument(
            "--excel",
            type=Path,
            metavar="PATH",
            help=(
                "Path to an Excel (.xlsx) file.  Same column name rules "
                "as --csv.  Use this when a CSV saved from Excel causes "
                "delimiter detection issues."
            ),
        )

        sp.add_argument(
            "--group",
            nargs="+",
            metavar="NAME_OR_ID",
            help=(
                "One or more user group names or GUIDs.  "
                "Required when using --users; optional with --csv/--excel "
                "if the file has a group column."
            ),
        )
        sp.add_argument(
            "--apply",
            dest="dry_run",
            action="store_false",
            default=True,
            help="Apply changes to the server (default: dry run — preview only).",
        )
        sp.add_argument(
            "--concurrency",
            type=int,
            default=DEFAULT_CONCURRENCY,
            metavar="N",
            help=(
                f"Number of concurrent threads (default: {DEFAULT_CONCURRENCY})."
            ),
        )
        sp.add_argument(
            "--output-dir",
            type=Path,
            default=None,
            metavar="PATH",
            help="Output directory for the CSV (default: MSTR_OUTPUT_DIR or c:/tmp).",
        )

    args = parser.parse_args()

    # Validate: --users requires --group
    if args.users and not args.group:
        parser.error("--group is required when using --users.")

    main(
        action=args.action,
        env=args.env,
        users_cli=getattr(args, "users", None),
        csv_path=getattr(args, "csv", None),
        excel_path=getattr(args, "excel", None),
        groups_cli=getattr(args, "group", None),
        dry_run=args.dry_run,
        concurrency=args.concurrency,
        output_dir=args.output_dir,
    )
