"""
DBInstance_Configure_JDBC.py

Migrate MicroStrategy Database Instance connections from DSN-based (ODBC)
to DSN-less JDBC, in bulk, driven by a JSON configuration file.

Steps applied per DatasourceConnection:
  1. DSN check      — informational; reports whether current string is DSN-based
  2. JDBC string    — alter(connection_string=...) with host/port/db/auth from config
  3. Login match    — alter(datasource_login=...) matched by UserName from DSN config
  4. Param queries  — alter(parameterized_queries=False)

Each step checks the current state first and skips if already correct.

Input JSON format (DBInstance_DSN_Config.json)
──────────────────────────────────────────────
[
  {
    "instance_id":    "GUID",
    "instance_name":  "Bayer Analytics",
    "connection_id":  "GUID",
    "connection_name": "WH - DW1 Program Reporting",
    "login_id":       "GUID",          // existing login — used as fallback
    "login_name":     "Service Account",
    "login_username": "",
    "Application":    "Bayer Analytics",
    "DSN_Prod":  { "Host": "SERVER\\INST", "Port": "53956", "Database": "DW1",
                   "Trusted": "1", "UserName": "DOMAIN\\user" },
    "DSN_Stage": { "Host": "SERVER\\INST", "Port": "50314", "Database": "DW1",
                   "Trusted": "1", "UserName": "DOMAIN\\user" }
  },
  ...
]

Usage
─────
  python DBInstance_Configure_JDBC.py <env> --dsn-env Stage [--apply]
  python DBInstance_Configure_JDBC.py qa   --dsn-env Prod  [--apply]
  python DBInstance_Configure_JDBC.py qa   --dsn-env Stage --instance-id GUID [GUID ...] [--apply]
  python DBInstance_Configure_JDBC.py qa   --dsn-env Stage --input path/to/config.json [--apply]

Workstation
───────────
  Comment out the mstrio_core connection block and uncomment the Workstation
  block (search "WORKSTATION" below).  The workstationData variable is injected
  by the host environment and is not defined in CLI execution.
"""

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger
from mstrio.datasources import (
    DatasourceConnection,
    DatasourceInstance,
    list_datasource_logins,
)

from mstrio_core import MstrConfig, get_mstrio_connection, write_csv
from mstrio_core.config import MstrEnvironment

# ── Workstation import (uncomment when running from Workstation) ───────────────
# from mstrio.connection import get_connection

# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

JDBC_DRIVER = 'com.microstrategy.jdbc.sqlserver.SQLServerDriver'

DEFAULT_INPUT  = Path(__file__).parent / 'DBInstance_DSN_Config.json'
DEFAULT_OUTPUT = 'dbinstance_configure_jdbc_{env}_{dsn_env}_{ts}.csv'

# Columns written to the results CSV
RESULT_COLUMNS = [
    'instance_id',
    'instance_name',
    'connection_id',
    'connection_name',
    'dsn_env',
    'dsn_host',
    'dsn_ip',
    'jdbc_string',
    'target_login_user',
    'matched_login_id',
    'matched_login_name',
    'step1_dsn_check',
    'step2_jdbc',
    'step3_login',
    'step4_param_queries',
    'overall_status',
    'notes',
]

# Server IP map — Host\Instance → IP address (from ServerPorts sheet)
# Comparison is case-insensitive.
SERVER_IP_MAP: dict[str, str] = {
    r'CANSVBPIDEVSQL9\bpi':           '10.80.30.88',
    r'CANSVCAGSTGSQL1\CAN1':          '10.80.30.94',
    r'CANSVCAHDEVSQL9\CANADAIVET':    '10.80.30.89',
    r'CANSVCAHSTGSQL9\CANADAIVET':    '10.80.30.90',
    r'CDCSPAGSTGSQL1\EBIZ':           '10.20.30.48',
    r'cdcsvbiDEVsql4\bi':             '10.20.31.155',
    r'cdcsvbistgsql4\bi':             '10.20.31.233',
    r'CDCSVDWDEVSQL7\DW1':            '10.20.30.168',
    r'CDCSVMPMSTGSQL1\MPM1':          '10.20.30.33',
    r'cdcsvmrdevsql9\MARKETRESEARCH':  '10.20.52.96',
    r'rdcbiprosql4\bi':               '10.20.77.241',
    r'RDCBPIPROSQL9\BPI':             '10.20.77.232',
    r'rdccahprosql9\CanadaIvet':      '10.20.77.56',
    r'RDCCANPROSQL1\CAN1':            '10.20.77.238',
    r'RDCDOWPROSQL9\DAS':             '10.20.77.242',
    r'RDCDWPROSQL7\DW1':              '10.20.77.19',
    r'RDCMPMPROSQL1\MPM1':            '10.20.77.216',
    r'rdcspagprosql1\ebiz':           '10.20.77.139',
}

_IP_RE = re.compile(r'^\d{1,3}(\.\d{1,3}){3}$')

# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _ip_for_host(host: str) -> Optional[str]:
    """
    Return IP for a Host\\Instance name.
    If the value is already an IP address it is returned as-is.
    Otherwise SERVER_IP_MAP is searched case-insensitively.
    """
    if _IP_RE.match(host):
        return host
    for k, v in SERVER_IP_MAP.items():
        if k.lower() == host.lower():
            return v
    return None


def _build_jdbc_string(ip: str, port: str, database: str, trusted: bool) -> str:
    """
    Build the MicroStrategy JDBC connection string.

    Trusted=True  → AuthenticationMethod=ntlmjava  / MSTR_AUTH=ntlm
    Trusted=False → no AuthenticationMethod param   / MSTR_AUTH=standard

    Validated format:
      JDBC;DRIVER={<driver>};
      URL={jdbc:microstrategy:sqlserver://<ip>:<port>;
           DatabaseName=<db>[;AuthenticationMethod=ntlmjava];
           fetchTWFSasTime=TRUE};
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


def _result_row(entry: dict, dsn_env: str) -> dict:
    """Return a blank result row pre-populated with identity fields."""
    dsn_key = f'DSN_{dsn_env}'
    dsn_cfg = entry.get(dsn_key) or {}
    return {
        'instance_id':          entry.get('instance_id', ''),
        'instance_name':        entry.get('instance_name', ''),
        'connection_id':        entry.get('connection_id', ''),
        'connection_name':      entry.get('connection_name', ''),
        'dsn_env':              dsn_env,
        'dsn_host':             dsn_cfg.get('Host', ''),
        'dsn_ip':               '',
        'jdbc_string':          '',
        'target_login_user':    dsn_cfg.get('UserName', ''),
        'matched_login_id':     '',
        'matched_login_name':   '',
        'step1_dsn_check':      '',
        'step2_jdbc':           '',
        'step3_login':          '',
        'step4_param_queries':  '',
        'overall_status':       '',
        'notes':                '',
    }


# ══════════════════════════════════════════════════════════════════════════════
#  PER-INSTANCE CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

def _configure_instance(
    conn,
    entry: dict,
    dsn_env: str,
    do_apply: bool,
    all_logins: list,
) -> dict:
    """
    Run all four configuration steps for a single DatasourceInstance.
    Returns a result dict suitable for the output CSV.
    """
    result = _result_row(entry, dsn_env)
    notes: list[str] = []

    dsn_key = f'DSN_{dsn_env}'
    dsn_cfg = entry.get(dsn_key) or {}
    instance_id = entry['instance_id']
    instance_name = entry.get('instance_name', instance_id)

    # ── Resolve DSN config fields ─────────────────────────────────────────────
    host        = dsn_cfg.get('Host', '')
    port        = dsn_cfg.get('Port', '')
    database    = dsn_cfg.get('Database', '')
    trusted     = dsn_cfg.get('Trusted', '0') == '1'
    target_user = dsn_cfg.get('UserName', '')
    auth_desc   = 'Windows/NTLM' if trusted else 'Standard'

    ip = _ip_for_host(host) if host else None
    result['dsn_ip'] = ip or ''

    new_cs = _build_jdbc_string(ip, port, database, trusted) if ip else None
    result['jdbc_string'] = new_cs or ''

    if not ip:
        msg = f'Host {host!r} not in SERVER_IP_MAP — JDBC string cannot be built'
        logger.warning('{n}: {m}', n=instance_name, m=msg)
        notes.append(msg)

    # ── Load instance & connection ────────────────────────────────────────────
    try:
        ds_instance = DatasourceInstance(connection=conn, id=instance_id)
        ds_conn: DatasourceConnection = ds_instance.datasource_connection
        result['connection_name'] = ds_conn.name or result['connection_name']
    except Exception as exc:
        msg = f'Failed to load instance: {exc}'
        logger.error('{n}: {m}', n=instance_name, m=msg)
        result['overall_status'] = 'error'
        result['notes'] = msg
        return result

    current_cs       = ds_conn.connection_string or ''
    current_login    = ds_conn.datasource_login
    current_login_id = getattr(current_login, 'id', str(current_login)) if current_login else None

    is_dsn  = current_cs.upper().startswith('DSN=')
    is_jdbc = 'JDBC;DRIVER=' in current_cs

    # ── Step 1: DSN check (informational) ────────────────────────────────────
    # DatasourceConnection.convert_ds_connection_to_dsn_less() is not available
    # in this mstrio-py version.  Switching to JDBC in Step 2 is itself a
    # DSN-less change so no separate conversion call is needed.
    if is_dsn:
        result['step1_dsn_check'] = 'DSN-based → will convert via JDBC (Step 2)'
    elif is_jdbc:
        result['step1_dsn_check'] = 'already JDBC'
    else:
        result['step1_dsn_check'] = f'DSN-less (non-JDBC): {current_cs[:60]}'

    logger.debug(
        '{n}: current_cs={cs!r}  is_dsn={d}  is_jdbc={j}',
        n=instance_name, cs=current_cs[:80], d=is_dsn, j=is_jdbc,
    )

    # ── Step 2: Switch to JDBC connection string ──────────────────────────────
    if new_cs is None:
        result['step2_jdbc'] = 'skipped — IP not resolved'
        notes.append('Step 2 skipped: IP not resolved')
    elif is_jdbc and current_cs == new_cs:
        result['step2_jdbc'] = 'already correct'
        logger.info('{n}: Step 2 — already correct JDBC string', n=instance_name)
    else:
        try:
            if do_apply:
                ds_conn.alter(connection_string=new_cs)
                result['step2_jdbc'] = f'applied ({auth_desc})'
                logger.success('{n}: Step 2 — JDBC string updated ({a})', n=instance_name, a=auth_desc)
            else:
                result['step2_jdbc'] = f'dry-run ({auth_desc})'
                logger.info('{n}: Step 2 — DRY-RUN: would set JDBC ({a})', n=instance_name, a=auth_desc)
        except Exception as exc:
            msg = f'Step 2 error: {exc}'
            result['step2_jdbc'] = 'error'
            notes.append(msg)
            logger.error('{n}: {m}', n=instance_name, m=msg)

    # ── Step 3: Match and apply database login ────────────────────────────────
    matched_login = None
    if target_user:
        matched_login = next(
            (l for l in all_logins
             if (getattr(l, 'username', '') or '').lower() == target_user.lower()),
            None,
        )

    if not target_user:
        result['step3_login'] = 'skipped — no UserName in DSN config'
    elif not matched_login:
        msg = f'No DatasourceLogin found for username {target_user!r}'
        result['step3_login'] = 'not found'
        notes.append(msg)
        logger.warning('{n}: Step 3 — {m}', n=instance_name, m=msg)
    else:
        result['matched_login_id']   = matched_login.id
        result['matched_login_name'] = getattr(matched_login, 'name', '')

        if current_login_id == matched_login.id:
            result['step3_login'] = 'already correct'
            logger.info('{n}: Step 3 — login already correct ({ln})', n=instance_name, ln=matched_login.name)
        else:
            try:
                if do_apply:
                    ds_conn.alter(datasource_login=matched_login)
                    result['step3_login'] = f'applied → {matched_login.name}'
                    logger.success('{n}: Step 3 — login updated → {ln}', n=instance_name, ln=matched_login.name)
                else:
                    result['step3_login'] = f'dry-run → {matched_login.name}'
                    logger.info('{n}: Step 3 — DRY-RUN: would set login → {ln}', n=instance_name, ln=matched_login.name)
            except Exception as exc:
                msg = f'Step 3 error: {exc}'
                result['step3_login'] = 'error'
                notes.append(msg)
                logger.error('{n}: {m}', n=instance_name, m=msg)

    # ── Step 4: Disable parameterized queries ─────────────────────────────────
    try:
        if not ds_conn.parameterized_queries:
            result['step4_param_queries'] = 'already disabled'
            logger.info('{n}: Step 4 — parameterized_queries already False', n=instance_name)
        elif do_apply:
            ds_conn.alter(parameterized_queries=False)
            result['step4_param_queries'] = 'applied'
            logger.success('{n}: Step 4 — parameterized_queries set to False', n=instance_name)
        else:
            result['step4_param_queries'] = 'dry-run'
            logger.info('{n}: Step 4 — DRY-RUN: would set parameterized_queries=False', n=instance_name)
    except Exception as exc:
        msg = f'Step 4 error: {exc}'
        result['step4_param_queries'] = 'error'
        notes.append(msg)
        logger.error('{n}: {m}', n=instance_name, m=msg)

    # ── Overall status ────────────────────────────────────────────────────────
    if any('error' in str(v).lower() for v in [
        result['step2_jdbc'], result['step3_login'], result['step4_param_queries']
    ]):
        result['overall_status'] = 'error'
    else:
        result['overall_status'] = 'applied' if do_apply else 'dry-run'

    result['notes'] = '; '.join(notes)
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main(
    env: str,
    dsn_env: str,
    input_path: Path,
    instance_ids: Optional[list[str]],
    do_apply: bool,
    output_dir: Optional[Path],
) -> int:

    config = MstrConfig(environment=MstrEnvironment(env))
    out_dir = output_dir or config.output_dir

    # ── CONNECTION ─────────────────────────────────────────────────────────────

    # ── Option A: mstrio_core / .env (default for CLI) ────────────────────────
    conn = get_mstrio_connection()

    # ── Option B: Workstation (WORKSTATION) ───────────────────────────────────
    # Comment out Option A above and uncomment the two lines below when
    # running as an embedded Workstation script.
    # conn = get_connection(workstationData)  # noqa: F821

    # ── Load input config ─────────────────────────────────────────────────────
    logger.info('Reading config from {p}', p=input_path)
    entries: list[dict] = json.loads(input_path.read_text(encoding='utf-8'))
    logger.info('Loaded {n} entries from config', n=len(entries))

    # Filter by instance_id if provided
    if instance_ids:
        ids_upper = {i.upper() for i in instance_ids}
        entries = [e for e in entries if e.get('instance_id', '').upper() in ids_upper]
        logger.info('Filtered to {n} instance(s) by --instance-id', n=len(entries))

    # Filter to entries that have a config for the requested DSN environment
    dsn_key = f'DSN_{dsn_env}'
    valid   = [e for e in entries if e.get(dsn_key)]
    skipped = len(entries) - len(valid)
    if skipped:
        logger.warning(
            '{s} entry/entries have no {k} — skipped',
            s=skipped, k=dsn_key,
        )
    entries = valid

    if not entries:
        logger.error('No entries to process for DSN env {d}', d=dsn_env)
        return 1

    logger.info(
        'Processing {n} instance(s)  |  DSN env={d}  apply={a}',
        n=len(entries), d=dsn_env, a=do_apply,
    )

    # Pre-fetch all database logins once
    logger.info('Fetching all DatasourceLogins...')
    all_logins = list_datasource_logins(conn)
    logger.info('Found {n} DatasourceLogin(s)', n=len(all_logins))

    # ── Process each instance ─────────────────────────────────────────────────
    results: list[dict] = []
    for i, entry in enumerate(entries, 1):
        name = entry.get('instance_name', entry.get('instance_id', '?'))
        logger.info('[{i}/{total}] {n}', i=i, total=len(entries), n=name)
        result = _configure_instance(conn, entry, dsn_env, do_apply, all_logins)
        results.append(result)

    # ── Summary ───────────────────────────────────────────────────────────────
    applied  = sum(1 for r in results if r['overall_status'] == 'applied')
    dry_runs = sum(1 for r in results if r['overall_status'] == 'dry-run')
    errors   = sum(1 for r in results if r['overall_status'] == 'error')

    logger.info(
        'Complete — applied={a}  dry-run={d}  errors={e}  total={t}',
        a=applied, d=dry_runs, e=errors, t=len(results),
    )

    # ── Write results CSV ─────────────────────────────────────────────────────
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_file = out_dir / DEFAULT_OUTPUT.format(env=env, dsn_env=dsn_env.lower(), ts=ts)
    rows = [[r.get(c, '') for c in RESULT_COLUMNS] for r in results]
    write_csv(rows, columns=RESULT_COLUMNS, path=out_file)
    logger.success('Results written to {p}', p=out_file)

    return 0 if errors == 0 else 1


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=(
            'Configure MicroStrategy Database Instance connections to use JDBC. '
            'Reads instance / DSN configuration from a JSON file and applies '
            'connection string, login, and parameterized-query settings.'
        )
    )
    parser.add_argument(
        'env',
        choices=[e.value for e in MstrEnvironment],
        help='MicroStrategy environment to connect to (dev / qa / prod).',
    )
    parser.add_argument(
        '--dsn-env',
        choices=['Prod', 'Stage'],
        default='Stage',
        help='Which DSN config block to use from the JSON (default: Stage).',
    )
    parser.add_argument(
        '--input', '-i',
        type=Path,
        default=DEFAULT_INPUT,
        help=f'Path to the JSON config file (default: {DEFAULT_INPUT.name}).',
    )
    parser.add_argument(
        '--instance-id',
        nargs='+',
        metavar='GUID',
        dest='instance_ids',
        help='Limit processing to one or more instance GUIDs.',
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Commit changes (default: dry-run).',
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=None,
        help='Directory for the results CSV (default: MSTR_OUTPUT_DIR from .env).',
    )

    args = parser.parse_args()

    raise SystemExit(
        main(
            env=args.env,
            dsn_env=args.dsn_env,
            input_path=args.input,
            instance_ids=args.instance_ids,
            do_apply=args.apply,
            output_dir=args.output_dir,
        )
    )
