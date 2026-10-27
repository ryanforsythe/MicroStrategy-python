# The comment written in the first line will be displayed in the consumption mode pop-up
"""
DSInstance_FindProjects.py

Find which loaded projects use a given MicroStrategy Database Instance.

Configuration
─────────────
  ENV         — MSTR environment key (dev / qa / prod) used by mstrio_core.
                Ignored when using the Workstation connection.
  INSTANCE_ID — GUID of the DatasourceInstance to look up.

Connection
──────────
  Default : mstrio_core / .env  (CLI / PyCharm)
  Optional: Workstation — search "WORKSTATION" below to swap.
"""

from mstrio.datasources import DatasourceInstance
from mstrio.server import Environment

# ── CONNECTION ────────────────────────────────────────────────────────────────

# ── Option A: mstrio_core (.env / CLI) ────────────────────────────────────────
from mstrio_core import MstrConfig, get_mstrio_connection
from mstrio_core.config import MstrEnvironment

ENV = 'qa'
config = MstrConfig(environment=MstrEnvironment(ENV))
conn = get_mstrio_connection()

# ── Option B: Workstation (WORKSTATION) ───────────────────────────────────────
# Comment out Option A above and uncomment below when running from Workstation.
# from mstrio.connection import get_connection
# conn = get_connection(workstationData)  # noqa: F821

# ── CONFIG ────────────────────────────────────────────────────────────────────

INSTANCE_ID = 'PASTE_DATASOURCEINSTANCE_GUID_HERE'

# ── LOOK UP INSTANCE ──────────────────────────────────────────────────────────

instance = DatasourceInstance(conn, id=INSTANCE_ID)
print(f"\nInstance : {instance.name}  ({instance.id})")
print(f"DBMS     : {instance.dbms.name}")

# ── PRIMARY — project_names attribute ────────────────────────────────────────
# mstrio-py maps the API's projectList field to project_names on the object.
project_names = getattr(instance, 'project_names', None) or []

if project_names:
    print(f"\nAssociated projects ({len(project_names)}):")
    for name in project_names:
        print(f"  {name}")

else:
    # ── FALLBACK — scan every loaded project via REST ─────────────────────────
    # GET /api/databases with X-MSTR-ProjectID returns the database instances
    # visible within that project's scope.
    all_projects = Environment(conn).list_projects()
    print(f"\nproject_names not populated — scanning {len(all_projects)} loaded project(s)...")

    auth_headers = {
        'X-MSTR-AuthToken': conn.auth_token,
        'Content-Type': 'application/json',
    }

    found = []
    for project in all_projects:
        try:
            r = conn.session.get(
                f'{conn.base_url}/api/databases',
                params={'projectId': project.id},
                headers={**auth_headers, 'X-MSTR-ProjectID': project.id},
            )
            if r.ok:
                ids = [(db.get('id') or '').upper() for db in (r.json() or [])]
                if INSTANCE_ID.upper() in ids:
                    found.append(f"{project.name}  ({project.id})")
            else:
                print(f"  [{project.name}] HTTP {r.status_code} — skipped")
        except Exception as exc:
            print(f"  [{project.name}] error: {exc}")

    print(f"\n'{instance.name}' found in {len(found)} project(s):")
    for entry in found:
        print(f"  {entry}")
