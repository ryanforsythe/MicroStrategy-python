# The comment written in the first line will be displayed in the consumption mode pop-up
"""
ConfigureDBInstance_JDBC.py

Test script: migrate a single MicroStrategy Database Instance connection
from DSN-based (ODBC) to DSN-less JDBC.

Steps applied to the DatasourceConnection:
  1. Convert to DSN-less         — convert_ds_connection_to_dsn_less()
  2. Switch to JDBC string       — alter(connection_string=...)
  3. Apply matching login        — alter(datasource_login=...)
  4. Disable parameterized queries — alter(parameterized_queries=False)

CONFIGURATION
─────────────
• INSTANCE_ID  — GUID of the DatasourceInstance to update
• TARGET_ENV   — 'Prod' or 'Stage' (selects DSN_CONFIG entry)
• DO_APPLY     — False = dry-run (print what would happen, change nothing)
                 True  = commit all steps

Each step checks whether the change is already in place and skips if so.

Usage
─────
Run from MicroStrategy Workstation as an embedded Python script.
workstationData is injected automatically by the host environment.
"""

from loguru import logger
from mstrio.connection import get_connection
from mstrio.datasources import (
    DatasourceConnection,
    DatasourceInstance,
    list_datasource_logins,
)

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION — edit here for each test run
# ══════════════════════════════════════════════════════════════════════════════

INSTANCE_ID = 'B1913336469D067F981ED6BBD8C11DAB'  # Bayer Analytics
TARGET_ENV  = 'Prod'                              # 'Prod' or 'Stage'
DO_APPLY    = True                                # True to commit changes

# MicroStrategy SQL Server JDBC driver class name
JDBC_DRIVER = 'com.microstrategy.jdbc.sqlserver.SQLServerDriver'

# ── Server IP map (from ServerPorts sheet) ────────────────────────────────────
# Keys are Host\Instance strings exactly as they appear in DSN_Mapping_JSON
# (after JSON decode, i.e. single backslash).  Comparison is case-insensitive.
SERVER_IP_MAP: dict[str, str] = {
    r'CANSVBPIDEVSQL9\bpi':          '10.80.30.88',
    r'CANSVCAGSTGSQL1\CAN1':         '10.80.30.94',
    r'CANSVCAHDEVSQL9\CANADAIVET':   '10.80.30.89',
    r'CANSVCAHSTGSQL9\CANADAIVET':   '10.80.30.90',
    r'CDCSPAGSTGSQL1\EBIZ':          '10.20.30.48',
    r'cdcsvbiDEVsql4\bi':            '10.20.31.155',
    r'cdcsvbistgsql4\bi':            '10.20.31.233',
    r'CDCSVDWDEVSQL7\DW1':           '10.20.30.168',
    r'CDCSVMPMSTGSQL1\MPM1':         '10.20.30.33',
    r'cdcsvmrdevsql9\MARKETRESEARCH': '10.20.52.96',
    r'rdcbiprosql4\bi':              '10.20.77.241',
    r'RDCBPIPROSQL9\BPI':            '10.20.77.232',
    r'rdccahprosql9\CanadaIvet':     '10.20.77.56',
    r'RDCCANPROSQL1\CAN1':           '10.20.77.238',
    r'RDCDOWPROSQL9\DAS':            '10.20.77.242',
    r'RDCDWPROSQL7\DW1':             '10.20.77.19',
    r'RDCMPMPROSQL1\MPM1':           '10.20.77.216',
    r'rdcspagprosql1\ebiz':          '10.20.77.139',
}

# ── DSN_Mapping_JSON for Bayer Analytics (from DBInstances sheet) ─────────────
DSN_CONFIG: dict[str, dict] = {
    'Prod': {
                "Environment": "Prod",
                "DSN_Name": "WH - DW1 Program Reporting",
                "Host": "10.20.77.19",
                "Port": "53956",
                "Database": "DW1",
                "Trusted": "1",
                "UserName": r"AGDATA\SVCBIPROAPP"
            },
    'Stage':{
                "Environment": "Stage",
                "DSN_Name": "WH - DW1 Program Reporting",
                "Host": "10.20.30.168",
                "Port": "50314",
                "Database": "DW1",
                "Trusted": "1",
                "UserName": r"AGDATA\SVCAGDEVAPPBI"
            },
}

# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _ip_for_host(host: str) -> str | None:
    """Return IP address for a Host\\Instance name (case-insensitive)."""
    for k, v in SERVER_IP_MAP.items():
        if k.lower() == host.lower():
            return v
    return None


def _build_jdbc_string(ip: str, port: str, database: str, trusted: bool) -> str:
    """
    Build the MicroStrategy JDBC connection string.

    Trusted=True  → AuthenticationMethod=ntlmjava  / MSTR_AUTH=ntlm
    Trusted=False → no AuthenticationMethod param   / MSTR_AUTH=standard

    Format validated against manually configured instances:
      JDBC;DRIVER={<driver>};URL={jdbc:microstrategy:sqlserver://<ip>:<port>;
      DatabaseName=<db>[;AuthenticationMethod=ntlmjava];fetchTWFSasTime=TRUE};
      MSTR_AUTH=<ntlm|standard>;
    """
    base = (
        f'JDBC;DRIVER={{{JDBC_DRIVER}}};'
        f'URL={{jdbc:microstrategy:sqlserver://{ip}:{port};'
        f'DatabaseName={database};'
    )
    if trusted:
        return base + 'AuthenticationMethod=ntlmjava;fetchTWFSasTime=TRUE};MSTR_AUTH=ntlm;'
    return base + 'fetchTWFSasTime=TRUE};MSTR_AUTH=standard;'


def _action(label: str, do_apply: bool, apply_fn, dry_msg: str) -> None:
    """Execute apply_fn or print dry-run message, then log the outcome."""
    if do_apply:
        apply_fn()
        logger.success('    ✓ {l}', l=label)
    else:
        logger.info('    DRY-RUN → {m}', m=dry_msg)


# ══════════════════════════════════════════════════════════════════════════════
#  CONNECT
# ══════════════════════════════════════════════════════════════════════════════

conn = get_connection(workstationData)  # noqa: F821 — injected by Workstation

# ══════════════════════════════════════════════════════════════════════════════
#  LOAD INSTANCE & RESOLVE CONFIG
# ══════════════════════════════════════════════════════════════════════════════

ds_instance = DatasourceInstance(connection=conn, id=INSTANCE_ID)
ds_conn: DatasourceConnection = ds_instance.datasource_connection

cfg          = DSN_CONFIG[TARGET_ENV]
host         = cfg['Host']
port         = cfg['Port']
database     = cfg['Database']
trusted      = cfg['Trusted'] == '1'
target_user  = cfg['UserName']
ip           = cfg['Host']
auth_desc    = 'Windows/NTLM' if trusted else 'Standard'

current_cs       = ds_conn.connection_string or ''
current_login    = ds_conn.datasource_login
current_login_id = getattr(current_login, 'id', str(current_login)) if current_login else None

is_dsn  = current_cs.upper().startswith('DSN=')
is_jdbc = 'JDBC;DRIVER=' in current_cs
new_cs  = _build_jdbc_string(ip, port, database, trusted) if ip else None

# ── Header ────────────────────────────────────────────────────────────────────
print('=' * 72)
print(f'Instance              : {ds_instance.name}  ({ds_instance.id})')
print(f'Connection            : {ds_conn.name}  ({ds_conn.id})')
print(f'DBMS                  : {ds_instance.dbms.name}')
print(f'Current login         : {getattr(current_login, "name", current_login)}  ({current_login_id})')
print(f'Current conn string   : {current_cs}')
print(f'Parameterized queries : {ds_conn.parameterized_queries}')
print(f'Target environment    : {TARGET_ENV}')
print(f'Apply changes         : {DO_APPLY}')
print('=' * 72)

if not ip:
    logger.warning(
        'Host {h!r} not found in SERVER_IP_MAP — '
        'JDBC string cannot be built; Steps 2 will be skipped.',
        h=host,
    )

# ══════════════════════════════════════════════════════════════════════════════
#  STEP 1 — DSN-less check (informational)
# ══════════════════════════════════════════════════════════════════════════════
# DatasourceConnection does not expose convert_ds_connection_to_dsn_less() in
# this version of mstrio-py.  Switching to a JDBC connection string (Step 2)
# is itself a DSN-less change, so no separate conversion step is needed.

print(f'\n[1] DSN-less check (informational)')
print(f'    DSN-based : {is_dsn}   JDBC : {is_jdbc}')
if is_dsn:
    print('    Connection is currently DSN-based — Step 2 will replace it with JDBC.')
else:
    print('    Connection is already DSN-less.')

# ══════════════════════════════════════════════════════════════════════════════
#  STEP 2 — Switch to JDBC connection string
# ══════════════════════════════════════════════════════════════════════════════

print(f'\n[2] Switch to JDBC connection string')
print(f'    Host     : {host}')
print(f'    IP       : {ip}')
print(f'    Port     : {port}')
print(f'    Database : {database}')
print(f'    Auth     : {auth_desc}')
print(f'    New CS   : {new_cs}')

if new_cs is None:
    print('    — IP not resolved, skipping')
elif is_jdbc and current_cs == new_cs:
    print('    — already correct JDBC string, skipping')
else:
    _action(
        label     = f'connection string updated to JDBC ({auth_desc})',
        do_apply  = DO_APPLY,
        apply_fn  = lambda: ds_conn.alter(connection_string=new_cs),
        dry_msg   = f'ds_conn.alter(connection_string={new_cs!r})',
    )

# ══════════════════════════════════════════════════════════════════════════════
#  STEP 3 — Match and apply database login
# ══════════════════════════════════════════════════════════════════════════════

print(f'\n[3] Match database login  (target username: {target_user!r})')
print(f'    Current : {getattr(current_login, "name", current_login)}  ({current_login_id})')

all_logins = list_datasource_logins(conn)
matched_login = next(
    (l for l in all_logins
     if (getattr(l, 'username', '') or '').lower() == target_user.lower()),
    None,
)

if not matched_login:
    logger.warning(
        "No DatasourceLogin found with username {u!r} — listing all logins for reference:",
        u=target_user,
    )
    for l in all_logins:
        print(
            f'    {getattr(l, "name", "?"):40s}'
            f'  username={getattr(l, "username", "?")}'
            f'  id={l.id}'
        )
elif current_login_id == matched_login.id:
    print(f'    Matched : {matched_login.name}  ({matched_login.id})')
    print('    — login already correct, skipping')
else:
    print(f'    Matched : {matched_login.name}  ({matched_login.id})')
    _action(
        label     = f'login updated → {matched_login.name}  ({matched_login.id})',
        do_apply  = DO_APPLY,
        apply_fn  = lambda: ds_conn.alter(datasource_login=matched_login),
        dry_msg   = f'ds_conn.alter(datasource_login=<{matched_login.name}>)',
    )

# ══════════════════════════════════════════════════════════════════════════════
#  STEP 4 — Disable parameterized queries
# ══════════════════════════════════════════════════════════════════════════════

print(f'\n[4] Disable parameterized queries')
print(f'    Current : parameterized_queries = {ds_conn.parameterized_queries}')

if not ds_conn.parameterized_queries:
    print('    — already disabled, skipping')
else:
    _action(
        label     = 'parameterized_queries set to False',
        do_apply  = DO_APPLY,
        apply_fn  = lambda: ds_conn.alter(parameterized_queries=False),
        dry_msg   = 'ds_conn.alter(parameterized_queries=False)',
    )

# ══════════════════════════════════════════════════════════════════════════════
#  SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

print('\n' + '=' * 72)
print(f'Run complete  |  instance={ds_instance.name}  env={TARGET_ENV}  applied={DO_APPLY}')
print('=' * 72)
