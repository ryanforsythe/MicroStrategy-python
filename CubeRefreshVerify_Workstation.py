# Refresh an event's cube subscriptions, verify via cube-cache job id, then fire the completion event.
"""CubeRefreshVerify_Workstation — trigger a cube refresh and verify it by job id.

MicroStrategy Workstation script. Authenticates with a username/password (falling
back to the injected `connectionData` session when credentials are not supplied).

Flow
----
  1. Work back from the refresh event via Event.list_related_subscriptions().
     Cube-update subscriptions carry exactly ONE content each — that content is
     the cube to verify.
  2. Baseline: record the cube-subscription jobs already in the monitor, and each
     cube's current cache `last_update_job` id.
  3. Invalidate ALL report/content caches in the project.
  4. Trigger the refresh event.
  5. Monitor `list_jobs(object_type=CUBE, type=SUBSCRIPTION, project=...)`. Jobs
     that are NOT in the baseline are the ones this run kicked off. The ongoing
     monitor does NOT exclude errored jobs, so a job that fails is detected.
  6. When one of our jobs leaves the monitor, verify it: the cube's cache
     `last_update_job` must equal that job id. That proves the cube cache was
     rebuilt BY OUR JOB — not by a timestamp guess and not by cube state
     (Intelligent Cubes unload after publishing, so status reads 0).
  7. Trigger the completion event only if every job we started finished and was
     verified against its cube cache.

Note: Job.id is composite ("<jobNumber>:<node>") while the cube cache's
`last_update_job` is the bare job number, so ids are normalized before comparing.

This script does NOT delete cube caches — it only reads them for verification.

Inputs
------
    project_name         text    Project to connect to (required)
    event_id             text    Refresh event GUID (required). Its related
                                 subscriptions define the cubes; it is also the
                                 event that gets triggered to refresh them.
    completion_event_id  text    Event GUID fired after all cubes verify.
                                 Default D5B52B8E4F6376E252F4AA91F6BF66A9.
    timeout_seconds      num     Max seconds to wait for the jobs (default 3600)
    poll_seconds         num     Seconds between job polls (default 15)

    Credential login (used instead of connectionData when username+password set):
    user_name            text    Login/username to authenticate as
    password             secret  Password for user_name
    base_url             text    Library URL. Optional — derived from
                                 connectionData when blank.
    login_mode           num     1=Standard, 16=SAML, 64=LDAP (default 1)

Workstation "unset" tokens (case-insensitive): '', '-', 'null', 'blank', 'empty'.
"""

import sys
import time

from mstrio.connection import Connection, get_connection
from mstrio.distribution_services import Event
from mstrio.distribution_services.subscription import list_subscriptions
from mstrio.project_objects import list_cube_caches
from mstrio.project_objects.content_cache import ContentCache
from mstrio.server import JobStatus, JobType, ObjectType, Project, list_jobs

_EMPTY_SENTINELS = {"", "-", "null", "blank", "empty"}
# How long to wait for our cube-refresh jobs to APPEAR before concluding none ran.
START_GRACE_SECONDS = 180


def _blank(value, default=None):
    if value is None:
        return default
    if isinstance(value, str) and value.strip().lower() in _EMPTY_SENTINELS:
        return default
    return value


# --- Substituted by Workstation (text -> 'quoted') --------------------------
PROJECT_NAME = _blank($project_name)                # noqa: F821
EVENT_ID = _blank($event_id)                        # noqa: F821
COMPLETION_EVENT_ID = _blank($completion_event_id, "D5B52B8E4F6376E252F4AA91F6BF66A9")  # noqa: F821
TIMEOUT_SECONDS = int(_blank($timeout_seconds, 3600))  # noqa: F821
POLL_SECONDS = int(_blank($poll_seconds, 15))       # noqa: F821
USER_NAME = _blank($user_name)                      # noqa: F821
PASSWORD = _blank($password)                        # noqa: F821 (secret var type)
BASE_URL = _blank($base_url)                        # noqa: F821
LOGIN_MODE = int(_blank($login_mode, 1))            # noqa: F821
# ----------------------------------------------------------------------------


def _connect():
    """Credential login when username+password are supplied, else connectionData."""
    if USER_NAME and PASSWORD:
        base_url = BASE_URL
        if not base_url:
            try:  # reuse the environment URL from the injected session data
                base_url = connectionData["defaultEnvironment"]["url"]  # noqa: F821
            except Exception:  # noqa: BLE001
                base_url = None
        if not base_url:
            raise ValueError("Credential login needs base_url (none provided/derivable).")
        print(f"Connecting as '{USER_NAME}' (credential login).")
        return Connection(base_url=base_url, username=USER_NAME, password=PASSWORD,
                          project_name=PROJECT_NAME, login_mode=LOGIN_MODE)
    print("Connecting via connectionData session.")
    return get_connection(connectionData, project_name=PROJECT_NAME)  # noqa: F821


def _job_num(job_id):
    """Job.id is '<jobNumber>:<node>'; the cube cache stores only <jobNumber>."""
    return str(job_id).split(":", 1)[0].strip()


def _cube_contents(subscription):
    """Cube contents of a subscription. Cube-update subscriptions carry exactly
    one content; anything else is reported so it can be looked at."""
    cubes = [(c.id, getattr(c, "name", "") or "")
             for c in (subscription.contents or [])
             if str(getattr(c.type, "value", c.type)).lower() == "cube"]
    if len(subscription.contents or []) > 1:
        print(f"  NOTE subscription '{subscription.name}' has "
              f"{len(subscription.contents)} contents (expected 1).")
    return cubes


def _cubes_from_event(conn, event, project_id):
    """{cube_id: cube_name} across the event's related subscriptions (one project)."""
    related_ids = {r.get("id") for r in event.list_related_subscriptions(to_dictionary=True)
                   if r.get("id")}
    cube_map, sub_names = {}, []
    for s in list_subscriptions(conn, project_id=project_id):
        if s.id in related_ids:
            sub_names.append(s.name)
            for cid, cname in _cube_contents(s):
                cube_map[cid] = cname
    return cube_map, sub_names


def _cube_sub_jobs(conn, project):
    """Cube-subscription jobs currently in the monitor (errors NOT excluded).

    `project` must be a Project OBJECT — list_jobs treats a bare string as a
    project NAME and would fail on a GUID.
    """
    return list_jobs(conn, project=project,
                     object_type=ObjectType.CUBE, type=JobType.SUBSCRIPTION)


def _cube_cache_job_nums(conn, cube_id):
    """The set of `last_update_job` ids across this cube's caches (all nodes)."""
    nums = set()
    try:
        caches = list_cube_caches(conn, cube_id=cube_id, to_dictionary=True)
    except Exception as exc:  # noqa: BLE001
        print(f"  WARN could not read cube caches for {cube_id}: {exc}")
        return nums
    for c in caches:
        job = c.get("lastUpdateJob", c.get("last_update_job"))
        if job is not None:
            nums.add(_job_num(job))
    return nums


def _invalidate_report_caches(conn, project_id):
    """Invalidate ALL report/content caches in the project (all cluster nodes).
    to_dictionary avoids the CacheSource.Type enum crash on unknown values."""
    try:
        caches = ContentCache.list_caches(conn, project_id=project_id, to_dictionary=True)
        cache_ids = [c.get("id") for c in caches if c.get("id")]
    except TypeError:  # older signature without to_dictionary/project_id
        caches = ContentCache.list_caches(conn)
        cache_ids = [getattr(c, "id", None) for c in caches if getattr(c, "id", None)]
    if cache_ids:
        ContentCache.invalidate_caches(conn, cache_ids=cache_ids)
    print(f"Invalidated {len(cache_ids)} report/content cache(s).")


def main():
    if not PROJECT_NAME:
        raise ValueError("project_name is required.")
    if not EVENT_ID:
        raise ValueError("event_id is required.")

    conn = _connect()
    project_id = conn.project_id
    # list_jobs resolves a string `project` as a NAME, so pass a Project object.
    project = Project(conn, id=project_id)

    # 1) event -> related subscriptions -> cubes
    event = Event(conn, id=EVENT_ID)
    cube_map, sub_names = _cubes_from_event(conn, event, project_id)
    name_by_id = dict(cube_map)
    print(f"Event {getattr(event, 'name', EVENT_ID)} ({EVENT_ID}): "
          f"{len(sub_names)} related subscription(s), {len(cube_map)} distinct cube(s).")
    if not cube_map:
        print("No cube contents on the event's related subscriptions — nothing to verify.")
        return 0

    # 2) baseline: jobs already in the monitor + each cube's current cache job id.
    #    Identity uses ALL pre-existing job ids (including errored ones) so a stale
    #    job can never be misattributed to this run.
    baseline = _cube_sub_jobs(conn, project)
    before_job_nums = {_job_num(j.id) for j in baseline}
    running_now = [j for j in baseline if j.status != JobStatus.ERROR]
    print(f"Baseline: {len(baseline)} cube-subscription job(s) in the monitor "
          f"({len(running_now)} not in Error).")
    before_cache_jobs = {}
    for cid, cname in cube_map.items():
        before_cache_jobs[cid] = _cube_cache_job_nums(conn, cid)
        print(f"  cube {cname} ({cid}) cache last_update_job={sorted(before_cache_jobs[cid])}")

    # 3) invalidate all report/content caches
    try:
        _invalidate_report_caches(conn, project_id)
    except Exception as exc:  # noqa: BLE001
        print(f"WARN report cache invalidation failed: {exc}")

    # 4) trigger the refresh event
    event.trigger()
    print(f"Triggered refresh event: {EVENT_ID}")

    # 5/6) monitor our jobs, then verify each against its cube cache
    our_jobs = {}            # job_num -> {'job_id','cube_id','cube_name'}
    finished, verified, errored = set(), set(), set()
    start_deadline = time.time() + START_GRACE_SECONDS
    deadline = time.time() + TIMEOUT_SECONDS
    while time.time() < deadline:
        try:
            jobs = _cube_sub_jobs(conn, project)
        except Exception as exc:  # noqa: BLE001
            print(f"  [poll] could not list jobs: {exc}")
            jobs = []
        current = {_job_num(j.id): j for j in jobs}

        # jobs not in the baseline are the ones this run started
        for num, j in current.items():
            if num in before_job_nums:
                continue
            if num not in our_jobs:
                our_jobs[num] = {"job_id": j.id, "cube_id": j.object_id,
                                 "cube_name": getattr(j, "object_name", "") or
                                 name_by_id.get(j.object_id, "")}
                print(f"  [job] started {num} -> {our_jobs[num]['cube_name']} "
                      f"({j.object_id}) status={getattr(j.status, 'value', j.status)}")
            if j.status == JobStatus.ERROR:
                errored.add(num)

        # a job is done when it leaves the monitor (or reports Error)
        for num in our_jobs:
            if num not in current or num in errored:
                finished.add(num)

        # verify finished jobs against the cube cache's last_update_job
        for num in sorted(finished - verified - errored):
            cube_id = our_jobs[num]["cube_id"]
            if num in _cube_cache_job_nums(conn, cube_id):
                verified.add(num)
                print(f"  [verified] job {num} rebuilt cube "
                      f"{our_jobs[num]['cube_name']} ({cube_id})")

        if our_jobs and finished == set(our_jobs) and (verified | errored) >= finished:
            break
        if not our_jobs and time.time() >= start_deadline:
            print("  [poll] no cube-subscription jobs started within the grace period.")
            break

        left = int(deadline - time.time())
        pend = set(our_jobs) - finished
        if pend:
            names = ", ".join(sorted(our_jobs[n]["cube_name"] or n for n in pend))
            print(f"  [poll] {left}s left — {len(pend)} job(s) running: {names}")
        elif our_jobs:
            unver = sorted(finished - verified - errored)
            print(f"  [poll] {left}s left — awaiting cache verification for: {unver}")
        else:
            print(f"  [poll] {left}s left — waiting for cube jobs to start...")
        time.sleep(POLL_SECONDS)

    # 7) report + completion event
    print("=== Cube refresh verification ===")
    for num, info in sorted(our_jobs.items()):
        if num in errored:
            state = "ERROR"
        elif num in verified:
            state = "VERIFIED"
        elif num in finished:
            state = "FINISHED but cache job id did not match"
        else:
            state = "STILL RUNNING (timeout)"
        print(f"  [{state}] job {num} — {info['cube_name']} ({info['cube_id']})")
    no_job = [cid for cid in cube_map
              if cid not in {i["cube_id"] for i in our_jobs.values()}]
    for cid in no_job:
        print(f"  [no job observed] {name_by_id.get(cid, '')} ({cid})")

    all_ok = bool(our_jobs) and not errored and verified == set(our_jobs)
    if not all_ok:
        print("=== NOT all cube jobs verified — completion event NOT triggered. ===")
        return 1
    print(f"=== All {len(verified)} cube job(s) finished and verified against the "
          f"cube cache. ===")
    if COMPLETION_EVENT_ID:
        Event(conn, id=COMPLETION_EVENT_ID).trigger()
        print(f"Triggered completion event: {COMPLETION_EVENT_ID}")
    return 0


# The host exec()s this script as a string, so the __main__ guard is unreliable —
# call main() unconditionally and surface a non-zero exit on failure.
_rc = main()
if _rc:
    sys.exit(_rc)
