"""SQL Server — execute a statement and return the result as JSON.

A basic MicroStrategy Workstation script for testing DB connectivity + query
execution. Modeled on the mstrio-py `psycopg2_all_actions_template.py`, which
injects connection parameters as `$`-prefixed placeholders that Workstation
substitutes at run time (text -> quoted string, number -> bare literal), and
recommends the `secret` variable type for passwords.

Define these variables in the Workstation script's variable panel:

    $host           text     e.g. myserver.database.windows.net
    $port           number   e.g. 1433
    $database       text     e.g. MyDatabase
    $user_name      text     SQL login
    $user_password  secret   SQL password        <-- declare as type "secret"
    $sql            text     the SQL statement to execute

Requires an ODBC driver + pyodbc in the Workstation Python runtime. If pyodbc
is unavailable, swap the connect() block for the pymssql variant noted below.
"""

import json
import datetime
import decimal

import pyodbc


def _json_default(value):
    """Make non-JSON-native SQL types serializable."""
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return str(value)


# --- Substituted by Workstation: text -> 'quoted', number -> bare ------------
server_host = $host
server_port = $port
database    = $database
user_name   = $user_name
password    = $user_password
sql         = $sql
# ----------------------------------------------------------------------------

connection = None
try:
    connection = pyodbc.connect(
        driver='{ODBC Driver 18 for SQL Server}',   # change to 17 if that's what's installed
        server=f'{server_host},{server_port}',
        database=database,
        uid=user_name,
        pwd=password,
        Encrypt='yes',
        TrustServerCertificate='yes',                # relax for self-signed certs; drop in prod
    )

    # pymssql alternative (if pyodbc/ODBC driver is unavailable):
    #   import pymssql
    #   connection = pymssql.connect(server=server_host, port=str(server_port),
    #                                database=database, user=user_name, password=password)

    cursor = connection.cursor()
    cursor.execute(sql)

    if cursor.description:  # SELECT-style statement -> rows
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        result = {'columns': columns, 'row_count': len(rows), 'rows': rows}
    else:                   # INSERT / UPDATE / DELETE / DDL -> affected count
        connection.commit()
        result = {'columns': [], 'row_count': cursor.rowcount, 'rows': []}

    print(json.dumps(result, default=_json_default, indent=2))

except Exception as exc:
    print(json.dumps({'error': str(exc)}))
    raise

finally:
    if connection is not None:
        connection.close()
