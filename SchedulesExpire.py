"""
SchedulesExpire.py — Set the stop_date to today for any schedule whose
stop_date is either NULL (runs forever) or in the future (> today).

Additionally, if a schedule has no related subscriptions, it is renamed to
"DEPRECATE-<original name>" to flag it as an orphan.

Schedules whose stop_date is already in the past are already expired and
are left untouched.

Usage:
    python SchedulesExpire.py <env>  [--apply]  [--output-dir PATH]

    python SchedulesExpire.py dev                  # dry run — preview only
    python SchedulesExpire.py prod --apply         # apply changes to prod

Run without --apply first to review the CSV output, then re-run with
--apply to commit the changes.
"""

import argparse
from datetime import date, datetime
from pathlib import Path

from loguru import logger
from mstrio.distribution_services.schedule import list_schedules

from mstrio_core import MstrConfig, get_mstrio_connection, write_csv
from mstrio_core.config import MstrEnvironment

# ── Constants ─────────────────────────────────────────────────────────────────

ENVS = [e.value for e in MstrEnvironment]
DEPRECATE_PREFIX = "DEPRECATE-"
OUTPUT_FILENAME = "expired_schedules.csv"

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

    Returns an empty list on any error so that a failed lookup never causes
    an accidental rename.
    """
    try:
        return schedule.list_related_subscriptions(to_dictionary=True)
    except Exception as exc:
        logger.warning(
            "Could not list subscriptions for {name} ({id}) — assuming has "
            "dependents to prevent accidental rename: {exc}",
            name=schedule.name,
            id=schedule.id,
            exc=exc,
        )
        # Return a sentinel so the caller treats this as "has dependents".
        return [{}]


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


def _deprecate_name(name: str) -> str:
    """Return the DEPRECATE-prefixed name, avoiding double-prefixing."""
    if name.startswith(DEPRECATE_PREFIX):
        return name
    return DEPRECATE_PREFIX + name


def _fmt_count(value: int | None) -> str:
    """Format an optional count for CSV output."""
    return str(value) if value is not None else "N/A"


# ── Main ──────────────────────────────────────────────────────────────────────


def main(
    env: str,
    dry_run: bool = True,
    output_dir: Path | None = None,
) -> None:
    """
    Expire schedules (and rename orphans) for the given environment.

    Args:
        env:        Environment to connect to ("dev", "qa", or "prod").
        dry_run:    When True (default), write the preview CSV but make no
                    server changes.  Pass False to apply.
        output_dir: Directory for the output CSV.  Defaults to
                    MstrConfig.output_dir (MSTR_OUTPUT_DIR env var, c:/tmp).
    """
    config = MstrConfig(environment=MstrEnvironment(env))
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")

    conn = get_mstrio_connection(config=config)
    try:
        all_schedules = list_schedules(conn)
        logger.info(
            "Retrieved {n} total schedule(s) from {env}.",
            n=len(all_schedules),
            env=env,
        )

        # ── Scan: identify schedules in scope ─────────────────────────────────
        # Each entry: (schedule, stop_date, sub_dicts, total, active, inactive)
        in_scope = []
        skipped = 0

        for s in all_schedules:
            stop = _to_date(getattr(s, "stop_date", None))

            if stop is not None and stop <= today:
                skipped += 1
                continue

            sub_dicts = _get_subscriptions(s)
            total, active, inactive = _categorize_subscriptions(sub_dicts)

            # Log per-schedule subscription detail
            if active is not None:
                logger.debug(
                    "  {name} ({id}): {total} subscription(s) — "
                    "{active} active, {inactive} inactive",
                    name=s.name,
                    id=s.id,
                    total=total,
                    active=active,
                    inactive=inactive,
                )
            else:
                logger.debug(
                    "  {name} ({id}): {total} subscription(s) "
                    "(active/inactive status not available from API)",
                    name=s.name,
                    id=s.id,
                    total=total,
                )

            in_scope.append((s, stop, sub_dicts, total, active, inactive))

        logger.info(
            "{n} schedule(s) in scope | {s} already expired (skipped).",
            n=len(in_scope),
            s=skipped,
        )

        # Summary: did the API expose active/inactive?
        if in_scope:
            sample_total, sample_active, _ = in_scope[0][3], in_scope[0][4], None
            if sample_active is None and sample_total > 0:
                logger.info(
                    "Note: active/inactive subscription status is not available "
                    "from the REST API for this server version — "
                    "subscription_count only."
                )

        # ── Build CSV rows ────────────────────────────────────────────────────
        rows = []
        for s, stop, sub_dicts, total, active, inactive in in_scope:
            actions = [f"set stop_date={today_str}"]
            if total == 0:
                actions.append(f"rename → {_deprecate_name(s.name)!r}")

            rows.append([
                s.id,
                s.name,
                str(getattr(s, "schedule_type", "")),
                str(stop) if stop else "",
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
            logger.info("No schedules require expiry — nothing to write.")

        if dry_run:
            logger.info(
                "Dry run — no changes applied. "
                "Review {p} then re-run with --apply to proceed.",
                p=out_path,
            )
            return

        # ── Apply ─────────────────────────────────────────────────────────────
        expired = 0
        renamed = 0
        errors = 0

        for s, stop, sub_dicts, total, active, inactive in in_scope:
            alter_kwargs: dict = {"stop_date": today_str}
            if total == 0:
                alter_kwargs["name"] = _deprecate_name(s.name)

            try:
                s.alter(**alter_kwargs)

                if "name" in alter_kwargs:
                    logger.info(
                        "Expired + renamed: {old!r} → {new!r} ({id})",
                        old=s.name,
                        new=alter_kwargs["name"],
                        id=s.id,
                    )
                    renamed += 1
                else:
                    sub_detail = (
                        f"{total} subscription(s)"
                        if active is None
                        else f"{total} subscription(s): {active} active, {inactive} inactive"
                    )
                    logger.info(
                        "Expired: {name} ({id}) — {detail}",
                        name=s.name,
                        id=s.id,
                        detail=sub_detail,
                    )

                expired += 1

            except Exception as exc:
                logger.error(
                    "Failed to update {name} ({id}): {exc}",
                    name=s.name,
                    id=s.id,
                    exc=exc,
                )
                errors += 1

        logger.success(
            "Done. Expired {exp}/{total} schedule(s) | "
            "{ren} renamed with '{prefix}' prefix | Errors: {err}.",
            exp=expired,
            total=len(in_scope),
            ren=renamed,
            prefix=DEPRECATE_PREFIX,
            err=errors,
        )

    finally:
        conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Expire MicroStrategy schedules with no stop_date or a future "
            "stop_date.  Orphaned schedules (no related subscriptions) are also "
            f"renamed with a '{DEPRECATE_PREFIX}' prefix."
        )
    )
    parser.add_argument(
        "env",
        choices=ENVS,
        help="Environment to process.",
    )
    parser.add_argument(
        "--apply",
        dest="dry_run",
        action="store_false",
        default=True,
        help="Apply changes to the server (default: dry run — preview only).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        metavar="PATH",
        help="Output directory for the preview CSV (default: MSTR_OUTPUT_DIR or c:/tmp).",
    )

    args = parser.parse_args()
    main(env=args.env, dry_run=args.dry_run, output_dir=args.output_dir)
