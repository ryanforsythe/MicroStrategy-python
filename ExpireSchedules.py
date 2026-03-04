"""
ExpireSchedules.py — Set the stop_date to today for any schedule whose
stop_date is either NULL (runs forever) or in the future (> today).

Additionally, if a schedule has no dependents, it is renamed to
"DEPRECATE-<original name>" to flag it as an orphan.

Schedules whose stop_date is already in the past are already expired and
are left untouched.

Usage:
    python ExpireSchedules.py <env>  [--apply]  [--output-dir PATH]

    python ExpireSchedules.py dev                  # dry run — preview only
    python ExpireSchedules.py prod --apply         # apply changes to prod

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
COLUMNS = [
    "id",
    "name",
    "schedule_type",
    "current_stop_date",
    "has_dependents",
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


def _check_dependents(schedule) -> bool:
    """
    Call has_dependents() on a schedule.  Returns True on any error so that
    a failed check never causes an accidental rename.
    """
    try:
        return schedule.has_dependents()
    except Exception as exc:
        logger.warning(
            "Could not check dependents for {name} ({id}) — assuming True: {exc}",
            name=schedule.name,
            id=schedule.id,
            exc=exc,
        )
        return True


def _deprecate_name(name: str) -> str:
    """Return the DEPRECATE-prefixed name, avoiding double-prefixing."""
    if name.startswith(DEPRECATE_PREFIX):
        return name
    return DEPRECATE_PREFIX + name


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
        in_scope = []
        skipped = 0

        for s in all_schedules:
            stop = _to_date(getattr(s, "stop_date", None))

            if stop is not None and stop <= today:
                skipped += 1
                continue

            has_deps = _check_dependents(s)
            in_scope.append((s, stop, has_deps))

        logger.info(
            "{n} schedule(s) in scope | {s} already expired (skipped).",
            n=len(in_scope),
            s=skipped,
        )

        # ── Build CSV rows ────────────────────────────────────────────────────
        rows = []
        for s, stop, has_deps in in_scope:
            actions = [f"set stop_date={today_str}"]
            if not has_deps:
                actions.append(f"rename → {_deprecate_name(s.name)!r}")

            rows.append([
                s.id,
                s.name,
                str(getattr(s, "schedule_type", "")),
                str(stop) if stop else "",
                str(has_deps),
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

        for s, stop, has_deps in in_scope:
            alter_kwargs: dict = {"stop_date": today_str}
            if not has_deps:
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
                    logger.info("Expired: {name} ({id})", name=s.name, id=s.id)

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
            "stop_date.  Orphaned schedules (no dependents) are also renamed "
            f"with a '{DEPRECATE_PREFIX}' prefix."
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
