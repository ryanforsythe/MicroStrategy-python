"""
SchedulesActivate.py — Clear the stop_date for any schedule whose stop_date
falls within a given date range (inclusive), reactivating those schedules so
they resume running.

Optionally, if a schedule's name starts with "DEPRECATE-" (set by
SchedulesExpire.py), pass --restore-name to strip that prefix and restore
the original name.

Schedules outside the specified date range, or with no stop_date at all,
are left untouched.

Usage:
    python SchedulesActivate.py <env> <start_date> <end_date>  [--apply]
                                [--restore-name]  [--output-dir PATH]

    # Preview — no changes (start and end may be the same for a single date)
    python SchedulesActivate.py dev  2025-03-01 2025-03-31
    python SchedulesActivate.py prod 2025-03-04 2025-03-04

    # Apply changes
    python SchedulesActivate.py prod 2025-03-01 2025-03-31 --apply
    python SchedulesActivate.py prod 2025-03-01 2025-03-31 --apply --restore-name

Run without --apply first to review the CSV output, then re-run with
--apply to commit the changes.
"""

import argparse
from datetime import date, datetime
from pathlib import Path

from loguru import logger
from mstrio.distribution_services.schedule import Schedule
from mstrio.object_management.search_operations import full_search

from mstrio_core import MstrConfig, get_mstrio_connection, write_csv
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]
DEPRECATE_PREFIX = "DEPRECATE-"
OUTPUT_FILENAME = "activated_schedules.csv"

# Candidate field names that indicate an active/enabled subscription.
# The mstrio-py Subscription object does not expose an active flag directly;
# these are checked against the raw dict returned by to_dictionary=True.
_ACTIVE_FIELD_CANDIDATES = ("active", "enabled", "isActive", "is_active")

COLUMNS = [
    "id",
    "name",
    "schedule_type",
    "current_stop_date",
    "subscription_count",
    "active_subscriptions",
    "inactive_subscriptions",
    "actions",
]

# ── Helpers ───────────────────────────────────────────────────────────────────


def _list_all_schedules(conn) -> list:
    """
    Return all schedules (including hidden) from the Configuration domain.

    Uses full_search with domain=CONFIGURATION (4) and object_types=51
    (SCHEDULE_TRIGGER) instead of list_schedules(), which only returns
    non-hidden schedules.
    """
    results = full_search(
        conn, project=None, domain=4, object_types=51, to_dictionary=True,
    )
    logger.debug(
        "full_search returned {n} schedule dict(s) from Configuration domain.",
        n=len(results),
    )
    schedules = []
    for r in results:
        try:
            schedules.append(Schedule(conn, id=r["id"]))
        except Exception as exc:
            logger.warning(
                "Could not instantiate Schedule for {name!r} ({id}): {exc}",
                name=r.get("name", "?"),
                id=r.get("id", "?"),
                exc=exc,
            )
    return schedules


def _to_date(value) -> date | None:
    """
    Normalise a stop_date value returned by mstrio-py to a date object.
    mstrio-py may return a datetime, a date, or a yyyy-MM-dd string.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value:
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d").date()
        except ValueError:
            logger.warning("Unrecognised date format: {v!r}", v=value)
    return None


def _get_subscriptions(schedule) -> list[dict]:
    """
    Return all subscriptions dependent on *schedule* as raw dicts.

    Uses to_dictionary=True so we get the full REST API payload, which may
    include fields (e.g. 'active') that the Subscription Python object does
    not expose as properties.
    """
    try:
        return schedule.list_related_subscriptions(to_dictionary=True)
    except Exception as exc:
        logger.warning(
            "Could not list subscriptions for {name} ({id}): {exc}",
            name=schedule.name,
            id=schedule.id,
            exc=exc,
        )
        return []


def _active_field(sub_dicts: list[dict]) -> str | None:
    """
    Detect which field name (if any) carries the active/enabled flag.
    Inspects the first non-empty dict and returns the first matching
    candidate field name, or None if none are found.
    """
    for d in sub_dicts:
        if not d:
            continue
        for candidate in _ACTIVE_FIELD_CANDIDATES:
            if candidate in d:
                return candidate
    return None


def _categorize_subscriptions(
    sub_dicts: list[dict],
) -> tuple[int, int | None, int | None]:
    """
    Return (total, active, inactive).

    *active* and *inactive* are None when no active/enabled field can be
    found in the raw subscription dicts (i.e. the API does not expose it).
    """
    total = len(sub_dicts)
    if total == 0:
        return 0, 0, 0

    field = _active_field(sub_dicts)
    if field is None:
        return total, None, None

    active = sum(
        1 for d in sub_dicts
        if d.get(field) in (True, 1, "true", "True", "ACTIVE", "active")
    )
    return total, active, total - active


def _restore_name(name: str) -> str:
    """Strip the DEPRECATE- prefix if present, restoring the original name."""
    if name.startswith(DEPRECATE_PREFIX):
        return name[len(DEPRECATE_PREFIX):]
    return name


def _fmt_count(value: int | None) -> str:
    """Format an optional count for CSV output."""
    return str(value) if value is not None else "N/A"


# ── Main ──────────────────────────────────────────────────────────────────────


def main(
    env: str,
    start_date: date,
    end_date: date,
    dry_run: bool = True,
    restore_name: bool = False,
    output_dir: Path | None = None,
) -> None:
    """
    Activate schedules whose stop_date falls within [start_date, end_date].

    Args:
        env:          Environment to connect to ("dev", "qa", or "prod").
        start_date:   Earliest stop_date to match (inclusive).
        end_date:     Latest stop_date to match (inclusive).
        dry_run:      When True (default), write the preview CSV but make no
                      server changes.  Pass False to apply.
        restore_name: When True, strip the DEPRECATE- prefix from any schedule
                      name that has one.
        output_dir:   Directory for the output CSV.  Defaults to
                      MstrConfig.output_dir (MSTR_OUTPUT_DIR env var, c:/tmp).
    """
    if start_date > end_date:
        raise ValueError(
            f"start_date ({start_date}) must not be after end_date ({end_date})."
        )

    config = MstrConfig(environment=MstrEnvironment(env))

    conn = get_mstrio_connection(config=config)
    try:
        all_schedules = _list_all_schedules(conn)
        logger.info(
            "Retrieved {n} total schedule(s) from {env}.",
            n=len(all_schedules),
            env=env,
        )
        logger.info(
            "Scanning for schedules with stop_date between {start} and {end} (inclusive).",
            start=start_date,
            end=end_date,
        )

        # ── Scan: identify schedules in scope ─────────────────────────────────
        # Each entry: (schedule, stop_date, sub_dicts, total, active, inactive)
        in_scope = []
        skipped = 0

        for s in all_schedules:
            stop = _to_date(getattr(s, "stop_date", None))

            if stop is None or stop < start_date or stop > end_date:
                skipped += 1
                continue

            sub_dicts = _get_subscriptions(s)
            total, active, inactive = _categorize_subscriptions(sub_dicts)

            # Log per-schedule subscription detail
            if active is not None:
                logger.debug(
                    "  {name} ({id}): stop_date={stop}, {total} subscription(s) — "
                    "{active} active, {inactive} inactive",
                    name=s.name,
                    id=s.id,
                    stop=stop,
                    total=total,
                    active=active,
                    inactive=inactive,
                )
            else:
                logger.debug(
                    "  {name} ({id}): stop_date={stop}, {total} subscription(s) "
                    "(active/inactive status not available from API)",
                    name=s.name,
                    id=s.id,
                    stop=stop,
                    total=total,
                )

            in_scope.append((s, stop, sub_dicts, total, active, inactive))

        logger.info(
            "{n} schedule(s) in scope | {s} outside date range (skipped).",
            n=len(in_scope),
            s=skipped,
        )

        # Summary: did the API expose active/inactive?
        if in_scope:
            sample_total = in_scope[0][3]
            sample_active = in_scope[0][4]
            if sample_active is None and sample_total > 0:
                logger.info(
                    "Note: active/inactive subscription status is not available "
                    "from the REST API for this server version — "
                    "subscription_count only."
                )

        # ── Build CSV rows ────────────────────────────────────────────────────
        rows = []
        for s, stop, sub_dicts, total, active, inactive in in_scope:
            actions = ["clear stop_date"]
            if restore_name and s.name.startswith(DEPRECATE_PREFIX):
                actions.append(f"restore name → {_restore_name(s.name)!r}")

            rows.append([
                s.id,
                s.name,
                str(getattr(s, "schedule_type", "")),
                str(stop),
                str(total),
                _fmt_count(active),
                _fmt_count(inactive),
                "; ".join(actions),
            ])

        out_dir = output_dir or config.output_dir
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        out_path = Path(out_dir) / OUTPUT_FILENAME

        if rows:
            write_csv(rows, columns=COLUMNS, path=out_path)
            logger.success("Schedule list written → {p}", p=out_path)
        else:
            logger.info(
                "No schedules found with stop_date between {start} and {end}.",
                start=start_date,
                end=end_date,
            )

        if dry_run:
            logger.info(
                "Dry run — no changes applied. "
                "Review {p} then re-run with --apply to proceed.",
                p=out_path,
            )
            return

        # ── Apply ─────────────────────────────────────────────────────────────
        activated = 0
        restored = 0
        errors = 0

        for s, stop, sub_dicts, total, active, inactive in in_scope:
            alter_kwargs: dict = {"stop_date": None}
            if restore_name and s.name.startswith(DEPRECATE_PREFIX):
                alter_kwargs["name"] = _restore_name(s.name)

            try:
                s.alter(**alter_kwargs)

                if "name" in alter_kwargs:
                    sub_detail = (
                        f"{total} subscription(s)"
                        if active is None
                        else f"{total} subscription(s): {active} active, {inactive} inactive"
                    )
                    logger.info(
                        "Activated + restored name: {old!r} → {new!r} ({id}) — {detail}",
                        old=s.name,
                        new=alter_kwargs["name"],
                        id=s.id,
                        detail=sub_detail,
                    )
                    restored += 1
                else:
                    sub_detail = (
                        f"{total} subscription(s)"
                        if active is None
                        else f"{total} subscription(s): {active} active, {inactive} inactive"
                    )
                    logger.info(
                        "Activated: {name} ({id}) — {detail}",
                        name=s.name,
                        id=s.id,
                        detail=sub_detail,
                    )

                activated += 1

            except Exception as exc:
                logger.error(
                    "Failed to activate {name} ({id}): {exc}",
                    name=s.name,
                    id=s.id,
                    exc=exc,
                )
                errors += 1

        logger.success(
            "Done. Activated {act}/{total} schedule(s) | "
            "{res} name(s) restored | Errors: {err}.",
            act=activated,
            total=len(in_scope),
            res=restored,
            err=errors,
        )

    finally:
        conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────────


def _parse_date(value: str) -> date:
    """Parse a YYYY-MM-DD date string from the CLI."""
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date {value!r} — expected YYYY-MM-DD."
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Activate MicroStrategy schedules by clearing the stop_date for "
            "any schedule whose stop_date falls within the given date range. "
            "Optionally restores names that were prefixed with "
            f"'{DEPRECATE_PREFIX}' by ExpireSchedules.py."
        )
    )
    parser.add_argument(
        "env",
        choices=ENVS,
        help="Environment to process.",
    )
    parser.add_argument(
        "start_date",
        type=_parse_date,
        metavar="START_DATE",
        help="Earliest stop_date to match (YYYY-MM-DD, inclusive).",
    )
    parser.add_argument(
        "end_date",
        type=_parse_date,
        metavar="END_DATE",
        help="Latest stop_date to match (YYYY-MM-DD, inclusive). "
             "Use the same value as START_DATE to target a single date.",
    )
    parser.add_argument(
        "--apply",
        dest="dry_run",
        action="store_false",
        default=True,
        help="Apply changes to the server (default: dry run — preview only).",
    )
    parser.add_argument(
        "--restore-name",
        action="store_true",
        default=False,
        help=(
            f"Strip the '{DEPRECATE_PREFIX}' prefix from schedule names when "
            "activating (useful when reversing changes made by ExpireSchedules.py)."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        metavar="PATH",
        help="Output directory for the preview CSV (default: MSTR_OUTPUT_DIR or c:/tmp).",
    )

    args = parser.parse_args()
    main(
        env=args.env,
        start_date=args.start_date,
        end_date=args.end_date,
        dry_run=args.dry_run,
        restore_name=args.restore_name,
        output_dir=args.output_dir,
    )
