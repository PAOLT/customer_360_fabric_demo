# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6eff52ef-cbfa-4180-9cee-1af760cc9c80",
# META       "default_lakehouse_name": "cx360_data",
# META       "default_lakehouse_workspace_id": "157c6e22-ba36-4c2d-ad4e-ff71251c1bed",
# META       "known_lakehouses": [
# META         {
# META           "id": "6eff52ef-cbfa-4180-9cee-1af760cc9c80"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Theta customer 360 v2 - CSVs to typed Delta and Kusto tables
# 
# **Run notebook 01 first.** Attach the **same default Lakehouse**.
# All setup, parameter, dry-run and refresh instructions are embedded below;
# no separate Markdown file or shared notebook session is needed.
# This notebook reads a completed OneLake CSV snapshot, not notebook 01's
# in-memory variables.
# 
# Edit the executable **parameters** cell, not the examples in Markdown.
# Leave implementation cells unchanged.
# 
# Default names and paths are separate from v1: eleven
# `theta360_v2_*` Delta tables and one `theta360_v2_product_telemetry`
# Kusto table. Missing tables are created. Existing schemas must match;
# incompatible types are never silently coerced.
# 
# Start with DryRun=True. To refresh existing tables, set DryRun=False and
# ReplaceConfirmation="REPLACE THETA DEMO DATA". This replaces entire
# configured tables. Stop concurrent writers. There is no cross-table or
# cross-service transaction. A later failure can leave earlier tables updated.


# MARKDOWN ********************

# ## 1. Prepare Fabric
# 
# Use a workspace with active Fabric capacity, a Lakehouse, and a KQL database
# in an Eventhouse. Create these items if necessary.
# 
# Your notebook execution identity needs permission to read/write Lakehouse
# Files, create/replace Delta tables, and administer/create/ingest/replace
# Kusto tables. The notebooks do not create workspaces, Lakehouses, schemas,
# or KQL databases.
# 
# Import both `.ipynb` files into Fabric. Attach and pin the **same intended
# default Lakehouse** to both notebooks. Restart their Spark sessions if you
# change the default Lakehouse.


# MARKDOWN ********************

# ## 3. Check the table refresh
# 
# Open notebook **02** and configure its first code cell:
# 
# | Parameter | Set to |
# |---|---|
# | `BasePath` | The same value used in notebook 01 |
# | `SourceRoot` | Empty to use the latest completed snapshot; otherwise its printed `Files/.../runs/...` path |
# | `DeltaSchema` | Empty for unqualified names, or an existing schema such as `"dbo"` |
# | `DeltaTablePrefix` | Keep `"theta360_v2_"` to isolate v2 from v1 |
# | `KustoQueryUri` | The KQL database's HTTPS **query URI**, not its ingestion URI |
# | `KustoDatabase` | Your actual KQL database name |
# | `KustoTable` | Keep `"theta360_v2_product_telemetry"` to isolate v2 |
# | `DryRun` | `True` |
# | `ReplaceConfirmation` | `""` |
# 
# Run all cells. The notebook reads a single completed snapshot from OneLake;
# it does not depend on notebook 01's session or in-memory variables.
# It checks file hashes, explicitly casts CSV columns, validates keys and
# references, and checks every destination schema before printing a load plan.
# Dry run does **not** create or change destination tables.


# PARAMETERS CELL ********************

BasePath = "Files/theta360_v2"
SourceRoot = ""  # Empty resolves latest.json; or use a completed run's Files/.../runs/... path.
DeltaSchema = ""  # Empty for unqualified names; or an existing schema such as dbo.
DeltaTablePrefix = ""
DeltaOverrides = {}  # Optional: {"customers": {"table": "dbo.customers_v2", "columns": {}}}.
KustoQueryUri = "https://trd-uk4zk7q38r6903vtd0.z9.kusto.fabric.microsoft.com"  # The HTTPS query URI, not the ingestion URI.
KustoDatabase = "cx360_telemetry"
KustoTable = "product_telemetry"
KustoColumnMappings = {}
DryRun = False
ReplaceConfirmation = "REPLACE THETA DEMO DATA"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""Approved physical schemas, shared without a Spark dependency."""
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
NULL = '\\N'

@dataclass(frozen=True)
class Column:
    name: str
    kind: str = 'string'
    nullable: bool = False

def columns(*items):
    return tuple((Column(item) if isinstance(item, str) else Column(*item) for item in items))
SCHEMAS = {'customers': columns('customer_id', 'name', 'country'), 'customer_account_executives': columns('customer_id', 'account_executive_id'), 'products': columns('product_id', 'name', 'description'), 'employees': columns('employee_id', 'name', 'role'), 'employee_certifications': columns('employee_id', 'product_id', ('expiry_date', 'date')), 'sales_opportunities': columns('opportunity_id', 'customer_id', 'product_id', 'account_executive_id', ('start_date', 'date'), ('end_date', 'date'), 'state', ('monthly_recurring_revenue', 'money'), 'currency'), 'sales_milestones': columns('milestone_id', 'opportunity_id', ('activity_at', 'timestamp'), 'objective', 'outcome', 'sales_engineer_id'), 'support_tickets': columns('ticket_id', 'customer_id', 'product_id', ('opened_at', 'timestamp'), 'state', ('closed_at', 'timestamp', True), 'severity', 'description'), 'support_activities': columns('activity_id', 'ticket_id', 'technical_engineer_id', ('started_at', 'timestamp'), ('ended_at', 'timestamp', True), 'objective', ('outcome', 'string', True)), 'subscriptions': columns('subscription_id', 'customer_id', 'product_id', ('start_date', 'date'), ('end_date', 'date'), ('monthly_fee', 'money'), 'currency'), 'subscription_payments': columns('payment_id', 'subscription_id', ('billing_month', 'date'), ('paid_on', 'date'), ('amount', 'money'), 'currency'), 'product_telemetry': columns('event_id', 'customer_id', 'product_id', ('event_at', 'timestamp'), 'action', 'state', ('error_message', 'string', True))}
PRIMARY_KEYS = {'customers': ('customer_id',), 'customer_account_executives': ('customer_id',), 'products': ('product_id',), 'employees': ('employee_id',), 'employee_certifications': ('employee_id', 'product_id'), 'sales_opportunities': ('opportunity_id',), 'sales_milestones': ('milestone_id',), 'support_tickets': ('ticket_id',), 'support_activities': ('activity_id',), 'subscriptions': ('subscription_id',), 'subscription_payments': ('payment_id',), 'product_telemetry': ('event_id',)}
FOREIGN_KEYS = (('customer_account_executives', 'customer_id', 'customers', 'customer_id'), ('customer_account_executives', 'account_executive_id', 'employees', 'employee_id'), ('employee_certifications', 'employee_id', 'employees', 'employee_id'), ('employee_certifications', 'product_id', 'products', 'product_id'), ('sales_opportunities', 'customer_id', 'customers', 'customer_id'), ('sales_opportunities', 'product_id', 'products', 'product_id'), ('sales_opportunities', 'account_executive_id', 'employees', 'employee_id'), ('sales_milestones', 'opportunity_id', 'sales_opportunities', 'opportunity_id'), ('sales_milestones', 'sales_engineer_id', 'employees', 'employee_id'), ('support_tickets', 'customer_id', 'customers', 'customer_id'), ('support_tickets', 'product_id', 'products', 'product_id'), ('support_activities', 'ticket_id', 'support_tickets', 'ticket_id'), ('support_activities', 'technical_engineer_id', 'employees', 'employee_id'), ('subscriptions', 'customer_id', 'customers', 'customer_id'), ('subscriptions', 'product_id', 'products', 'product_id'), ('subscription_payments', 'subscription_id', 'subscriptions', 'subscription_id'), ('product_telemetry', 'customer_id', 'customers', 'customer_id'), ('product_telemetry', 'product_id', 'products', 'product_id'))
DELTA_TABLES = tuple((name for name in SCHEMAS if name != 'product_telemetry'))

def encode(value):
    if value is None:
        return NULL
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%dT%H:%M:%SZ')
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, '.2f')
    return str(value)

def decode(value, column):
    if value == NULL:
        if not column.nullable:
            raise ValueError(f'{column.name} must not be null')
        return None
    if column.kind == 'date':
        return date.fromisoformat(value)
    if column.kind == 'timestamp':
        from datetime import timezone
        return datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
    if column.kind == 'money':
        amount = Decimal(value)
        if not amount.is_finite() or amount != amount.quantize(Decimal('0.01')):
            raise ValueError(f'Invalid money value for {column.name}: {value!r}')
        return amount
    return value


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""Call load(spark, notebookutils, config) from a Microsoft Fabric notebook."""
import csv
import hashlib
import io
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
LOG = logging.getLogger('theta360.fabric_load')
CONFIRMATION = 'REPLACE THETA DEMO DATA'
IDENTIFIER = re.compile('[A-Za-z_][A-Za-z0-9_]*\\Z')
SPARK_TYPES = {'string': 'string', 'date': 'date', 'timestamp': 'timestamp', 'money': 'decimal(18,2)'}
KUSTO_TYPES = {'string': 'string', 'timestamp': 'datetime'}

def identifier(value):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValueError(f'Expected a simple SQL/KQL identifier, got {value!r}')
    return value

def delta_identifier(value):
    if not isinstance(value, str):
        raise ValueError('Delta table name must be a string')
    parts = value.split('.')
    if len(parts) not in (1, 2):
        raise ValueError('Use table or existing_schema.table in the default Lakehouse')
    return '.'.join((f'`{identifier(part)}`' for part in parts))

def kql_identifier(value):
    return f"['{identifier(value)}']"

def column_mapping(table, configured):
    if not isinstance(configured, dict):
        raise ValueError(f'{table}: columns must be an object')
    expected = {column.name for column in SCHEMAS[table]}
    if set(configured) - expected:
        raise ValueError(f'{table}: column mapping contains unknown source columns')
    result = {column.name: identifier(configured.get(column.name, column.name)) for column in SCHEMAS[table]}
    if len({value.lower() for value in result.values()}) != len(result):
        raise ValueError(f'{table}: destination column names must be unique')
    return result

def validate_schema(expected, actual, label):
    if expected != actual:
        raise ValueError(f'{label}: incompatible schema; expected {expected}, found {actual}')

def require_confirmation(existing, confirmation, dry_run):
    if existing and (not dry_run) and (confirmation != CONFIRMATION):
        raise ValueError(f"Replacing existing tables requires replace_confirmation={CONFIRMATION!r}; existing targets: {', '.join(existing)}")

def validate_config(config):
    if not isinstance(config.get('dry_run'), bool):
        raise ValueError('dry_run must explicitly be true or false')
    root = config.get('source_root', '')
    if not isinstance(root, str) or not root.startswith('Files/') or any((part in ('', '.', '..') for part in root.split('/'))):
        raise ValueError('source_root must be a relative default-Lakehouse Files/... folder')
    targets = config.get('delta_tables', {})
    if set(targets) != set(DELTA_TABLES):
        raise ValueError('delta_tables must map exactly the eleven non-telemetry tables')
    names = []
    for source, target in targets.items():
        names.append(delta_identifier(target['table']).lower())
        column_mapping(source, target.get('columns', {}))
    if len(set(names)) != len(names):
        raise ValueError('Delta destination tables must be distinct')
    kusto = config.get('kusto', {})
    identifier(kusto.get('table'))
    column_mapping('product_telemetry', kusto.get('columns', {}))
    if not isinstance(kusto.get('database'), str) or not kusto['database'].strip():
        raise ValueError('Configure the Kusto database name')
    uri = urlsplit(kusto.get('query_uri', ''))
    if uri.scheme != 'https' or uri.username or uri.password or (uri.port not in (None, 443)) or (uri.path not in ('', '/')) or uri.query or uri.fragment or (not uri.hostname) or (not uri.hostname.endswith(('.kusto.fabric.microsoft.com', '.kusto.windows.net'))):
        raise ValueError('Configure a public-cloud Fabric/ADX HTTPS Kusto query URI')

def check_delta_namespaces(spark, targets):
    requested = {target['table'].split('.')[0] for target in targets.values() if '.' in target['table']}
    if not requested:
        return
    available = {row[0] for row in spark.sql('SHOW SCHEMAS').collect()}
    if not requested <= available:
        raise ValueError(f'Missing Delta schemas: {sorted(requested - available)}. Use an existing schema on a schema-enabled default Lakehouse, or unqualified table names on a non-schema Lakehouse.')

class KustoRest:

    def __init__(self, notebookutils, config):
        self.notebookutils = notebookutils
        self.uri = config['query_uri'].rstrip('/')
        self.database = config['database']

    def execute(self, command):
        endpoint = 'mgmt' if command.lstrip().startswith('.') else 'query'
        request = Request(f'{self.uri}/v1/rest/{endpoint}', data=json.dumps({'db': self.database, 'csl': command}).encode('utf-8'), headers={'Authorization': 'Bearer ' + self.notebookutils.credentials.getToken('kusto'), 'Content-Type': 'application/json'}, method='POST')
        try:
            with urlopen(request, timeout=180) as response:
                result = json.load(response)
        except HTTPError as error:
            detail = error.read().decode('utf-8', errors='replace')
            raise RuntimeError(f'Kusto HTTP {error.code}: {detail[:3000]}') from error
        if result.get('error') or result.get('Exceptions'):
            raise RuntimeError(f"Kusto service error: {result.get('error') or result['Exceptions']}")
        if not result.get('Tables'):
            raise RuntimeError('Kusto returned no result table')
        table = result['Tables'][0]
        columns = [column['ColumnName'] for column in table['Columns']]
        rows = [dict(zip(columns, row)) for row in table['Rows']]
        if any((row.get('HasErrors') for row in rows)):
            raise RuntimeError(f'Kusto command reported ingestion errors: {rows}')
        return rows

    def table_schema(self, table):
        names = {row['TableName'] for row in self.execute('.show tables | project TableName')}
        if table not in names:
            return None
        result = self.execute(f'.show table {kql_identifier(table)} schema as json')
        if len(result) != 1 or 'Schema' not in result[0]:
            raise RuntimeError(f'Could not inspect Kusto schema for {table}')
        schema = json.loads(result[0]['Schema'])
        return {column['Name']: column['CslType'] for column in schema['OrderedColumns']}

def spark_schema(table):
    from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType, TimestampType
    types = {'string': StringType(), 'date': DateType(), 'timestamp': TimestampType(), 'money': DecimalType(18, 2)}
    return StructType([StructField(column.name, types[column.kind], True) for column in SCHEMAS[table]])

def inline_batches(rows, names, max_rows=2000, max_bytes=512000):
    lines = []
    size = 0
    for row in rows:
        values = row.asDict() if hasattr(row, 'asDict') else row
        buffer = io.StringIO(newline='')
        writer = csv.writer(buffer, lineterminator='\n')
        fields = []
        for name in names:
            value = values[name]
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    value = value.replace(tzinfo=timezone.utc)
                value = value.astimezone(timezone.utc)
            fields.append('' if value is None else encode(value))
        writer.writerow(fields)
        line = buffer.getvalue()
        line_size = len(line.encode('utf-8'))
        if line_size > max_bytes:
            raise ValueError('A telemetry row exceeds the maximum inline ingestion batch size')
        if lines and (len(lines) >= max_rows or size + line_size > max_bytes):
            yield ''.join(lines)
            lines, size = ([], 0)
        lines.append(line)
        size += line_size
    if lines:
        yield ''.join(lines)

def verify_kusto_rows(client, table, mapping, expected):
    column = lambda name: kql_identifier(mapping[name])
    required = ('event_id', 'customer_id', 'product_id', 'action', 'state')
    invalid = ' or '.join((f'isempty({column(name)})' for name in required))
    invalid += f" or isnull({column('event_at')}) or {column('state')} !in ('success', 'failed') or ({column('state')} == 'failed' and isempty({column('error_message')})) or ({column('state')} == 'success' and isnotempty({column('error_message')}))"
    rows = client.execute(f"{kql_identifier(table)} | summarize row_count=count(), distinct_ids=count_distinct({column('event_id')}), invalid_rows=countif({invalid})")
    if len(rows) != 1 or (rows[0]['row_count'] != expected or rows[0]['distinct_ids'] != expected or rows[0]['invalid_rows'] != 0):
        raise RuntimeError(f'{table}: Kusto ingestion verification failed: {rows}')

def publish_telemetry(client, frame, target, mapping, expected, target_order=None, replace_existing=False):
    stage = identifier(f'_theta360_stage_{uuid.uuid4().hex}')
    LOG.warning('Kusto staging table: %s; retained if ingestion/publication fails', stage)
    schema = SCHEMAS['product_telemetry']
    declarations = ', '.join((f'{kql_identifier(mapping[column.name])}:{KUSTO_TYPES[column.kind]}' for column in schema))
    client.execute(f'.create table {kql_identifier(stage)} ({declarations})')
    ingestion_mapping = json.dumps([{'column': mapping[column.name], 'datatype': KUSTO_TYPES[column.kind], 'Properties': {'Ordinal': index}} for index, column in enumerate(schema)], separators=(',', ':'))
    names = [column.name for column in schema]
    for batch in inline_batches(frame.select(*names).toLocalIterator(), names):
        client.execute(f".ingest inline into table {kql_identifier(stage)} with (format='csv', ingestionMapping='{ingestion_mapping}') <|\n{batch}")
    verify_kusto_rows(client, stage, mapping, expected)
    ordered = ', '.join((kql_identifier(name) for name in (target_order if target_order is not None else [mapping[column.name] for column in schema])))
    command = '.set-or-replace' if replace_existing else '.set'
    client.execute(f'{command} {kql_identifier(target)} <| {kql_identifier(stage)} | project {ordered}')
    verify_kusto_rows(client, target, mapping, expected)
    client.execute(f'.drop table {kql_identifier(stage)} ifexists')
    LOG.warning('Published %s telemetry rows to Kusto %s', expected, target)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import hashlib
import json
import logging
import re
import uuid
from datetime import datetime, timezone

LOG = logging.getLogger("theta360.v2")
logging.basicConfig(level=logging.INFO)


def files_root(value):
    if (
        not isinstance(value, str) or not value.startswith("Files/")
        or any(part in ("", ".", "..") for part in value.split("/"))
        or "\\" in value
    ):
        raise ValueError("Use a relative default-Lakehouse folder such as Files/theta360_v2")
    return value


def lakehouse_context(notebookutils):
    context = notebookutils.runtime.context
    if not context.get("defaultLakehouseId"):
        raise ValueError("Attach and pin the intended default Lakehouse before running")
    return {
        "lakehouse_id": context["defaultLakehouseId"],
        "lakehouse_workspace_id": (
            context.get("defaultLakehouseWorkspaceId") or context["currentWorkspaceId"]
        ),
    }


def file_bytes(spark, path):
    payloads = spark.read.format("binaryFile").load(path).select("content").collect()
    if len(payloads) != 1:
        raise ValueError(f"Expected exactly one file: {path}")
    return bytes(payloads[0]["content"])


def resolve_snapshot(spark, notebookutils, base_path, source_root=""):
    base_path = files_root(base_path)
    identity = lakehouse_context(notebookutils)
    pointer = None
    if not source_root:
        pointer_path = f"{base_path}/latest.json"
        if not notebookutils.fs.exists(pointer_path):
            raise FileNotFoundError("No completed v2 snapshot found; run notebook 01 first")
        pointer = json.loads(notebookutils.fs.head(pointer_path, 65_536))
        source_root = pointer["source_root"]
    source_root = files_root(source_root)
    prefix = f"{base_path}/runs/"
    if not source_root.startswith(prefix) or "/" in source_root[len(prefix):]:
        raise ValueError("SourceRoot must identify one immutable run beneath BasePath/runs")
    payload = file_bytes(spark, f"{source_root}/manifest.json")
    if pointer is not None and hashlib.sha256(payload).hexdigest() != pointer["manifest_sha256"]:
        raise ValueError("The published snapshot manifest hash does not match latest.json")
    manifest = json.loads(payload)
    if manifest.get("format_version") != 2 or manifest.get("source_root") != source_root:
        raise ValueError("The selected manifest is not a v2 snapshot for this source path")
    for key, value in identity.items():
        if manifest.get(key) != value:
            raise ValueError(f"Snapshot {key} differs from the attached default Lakehouse")
    if set(manifest["tables"]) != set(SCHEMAS) or manifest["null_marker"] != NULL:
        raise ValueError("Snapshot must contain the twelve approved CSV schemas")
    for table, schema in SCHEMAS.items():
        expected = [
            {"name": column.name, "type": column.kind, "nullable": column.nullable}
            for column in schema
        ]
        if manifest["tables"][table]["columns"] != expected:
            raise ValueError(f"{table}: manifest column definitions differ from the approved model")
    return source_root, manifest


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Type handling
# 
# CSVs are first read using a fixed **string** schema, not inferred types.
# Canonical formats are checked and every column is explicitly cast:
# 
# | Data | Spark / Delta type | Kusto telemetry type |
# |---|---|---|
# | IDs, names, descriptions, states | `STRING` | `string` |
# | Subscription dates, opportunity dates, expiry dates, billing/payment dates | `DATE` | Not loaded to Kusto |
# | Activity, ticket, and event timestamps | `TIMESTAMP`, UTC | `datetime` for telemetry |
# | Fees, payments, opportunity MRR | `DECIMAL(18,2)` | Not loaded to Kusto |
# 
# CSV null is `\N`. ANSI mode rejects invalid date/timestamp values and
# overflows, including values in nullable columns. Money with excessive scale
# is rejected before casting rather than silently rounded. Telemetry timestamps
# are formatted in Spark's UTC session for Kusto transport, avoiding driver-local
# timezone conversions.
# 
# Existing table names, columns and types must match after any configured
# renaming; incompatible schemas stop the refresh rather than being overwritten.
# Column order may differ.


# CELL ********************

def cast_csv_frame(raw, table):
    from pyspark.sql import functions as F

    schema = SCHEMAS[table]
    expected_names = [column.name for column in schema]
    if raw.columns != expected_names:
        raise ValueError(f"{table}: CSV header does not match the approved schema")
    patterns = {
        "date": r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
        "timestamp": r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
        "money": r"^[0-9]{1,16}\.[0-9]{2}$",
    }
    checks = []
    for column in schema:
        value = F.col(column.name)
        invalid = F.lit(False)
        if not column.nullable:
            invalid = value.isNull() | (value == "")
        if column.kind in patterns:
            invalid = invalid | (value.isNotNull() & ~value.rlike(patterns[column.kind]))
        checks.append(F.sum(F.when(invalid, 1).otherwise(0)).alias(column.name))
    problems = raw.agg(*checks).first().asDict()
    invalid_columns = [name for name, count in problems.items() if count]
    if invalid_columns:
        raise ValueError(f"{table}: missing values or noncanonical input in {invalid_columns}")
    target_schema = spark_schema(table)
    # ANSI mode makes invalid dates, timestamp values and overflows fail, including nullable columns.
    return raw.select(*[
        F.col(column.name).cast(target_schema[column.name].dataType).alias(column.name)
        for column in schema
    ])


def read_cast_sources(spark, source_root, manifest, cached):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    frames = {}
    for table, schema in SCHEMAS.items():
        path = f"{source_root}/csv/{table}.csv"
        if hashlib.sha256(file_bytes(spark, path)).hexdigest() != manifest["tables"][table]["sha256"]:
            raise ValueError(f"{table}: CSV hash mismatch")
        raw_schema = StructType([StructField(column.name, StringType(), True) for column in schema])
        raw = (
            spark.read.schema(raw_schema).option("header", "true")
            .option("enforceSchema", "false").option("mode", "FAILFAST")
            .option("nullValue", NULL).option("quote", '"').option("escape", '"')
            .option("multiLine", "true").csv(path)
        )
        frame = cast_csv_frame(raw, table).persist()
        cached.append(frame)
        stats = [
            F.count(F.lit(1)).alias("__rows"),
            F.countDistinct(*PRIMARY_KEYS[table]).alias("__keys"),
        ]
        # Count every typed column so bad casts in optional fields cannot be skipped by pruning.
        stats.extend(F.count(F.col(column.name)).alias(column.name) for column in schema)
        counts = frame.agg(*stats).first().asDict()
        expected = manifest["tables"][table]["row_count"]
        if expected <= 0 or counts["__rows"] != expected or counts["__keys"] != expected:
            raise ValueError(f"{table}: row count or primary-key uniqueness check failed")
        for column in schema:
            if not column.nullable and counts[column.name] != expected:
                raise ValueError(f"{table}.{column.name}: null introduced during casting")
        validate_schema(
            {column.name: SPARK_TYPES[column.kind] for column in schema},
            {field.name: field.dataType.simpleString() for field in frame.schema},
            f"{table}: cast result",
        )
        frames[table] = frame
    for child, source, parent, target in FOREIGN_KEYS:
        missing = (
            frames[child].select(source).distinct().join(
                frames[parent].select(F.col(target).alias(source)).distinct(),
                on=source, how="left_anti",
            ).limit(1).count()
        )
        if missing:
            raise ValueError(f"{child}.{source}: broken reference to {parent}.{target}")
    return frames


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def refresh_tables(spark, notebookutils, base_path, config):
    from pyspark.sql import functions as F

    # Set these before reading/casting. No permissive null-on-error conversion is allowed.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    source_root, manifest = resolve_snapshot(
        spark, notebookutils, base_path, config.get("source_root", "")
    )
    config = dict(config, source_root=source_root)
    validate_config(config)
    check_delta_namespaces(spark, config["delta_tables"])
    client = KustoRest(notebookutils, config["kusto"])
    cached = []
    try:
        frames = read_cast_sources(spark, source_root, manifest, cached)
        prepared = {}
        existing = []
        for source in DELTA_TABLES:
            target_config = config["delta_tables"][source]
            table = delta_identifier(target_config["table"])
            mapping = column_mapping(source, target_config.get("columns", {}))
            frame = frames[source].select(*[
                F.col(column.name).alias(mapping[column.name]) for column in SCHEMAS[source]
            ])
            exists = spark.catalog.tableExists(table)
            if exists:
                target_schema = spark.table(table).schema
                validate_schema(
                    {mapping[column.name]: SPARK_TYPES[column.kind] for column in SCHEMAS[source]},
                    {field.name: field.dataType.simpleString() for field in target_schema}, table,
                )
                if spark.sql(f"DESCRIBE DETAIL {table}").first().asDict()["format"].lower() != "delta":
                    raise ValueError(f"{table}: destination is not Delta")
                for field in target_schema:
                    if not field.nullable and frame.filter(F.col(field.name).isNull()).limit(1).count():
                        raise ValueError(f"{table}.{field.name}: source violates destination NOT NULL")
                frame = frame.select(*[field.name for field in target_schema])
                existing.append(table)
            prepared[source] = (table, frame, exists)
        kusto_config = config["kusto"]
        kusto_mapping = column_mapping("product_telemetry", kusto_config.get("columns", {}))
        actual = client.table_schema(kusto_config["table"])
        if actual is not None:
            validate_schema(
                {kusto_mapping[column.name]: KUSTO_TYPES[column.kind] for column in SCHEMAS["product_telemetry"]},
                actual, f"Kusto {kusto_config['table']}",
            )
            existing.append(f"Kusto {kusto_config['table']}")
        require_confirmation(existing, config.get("replace_confirmation"), config["dry_run"])
        plan = {
            "source_root": source_root, "reference_date": manifest["reference_date"],
            "dry_run": config["dry_run"],
            "delta_tables": {source: table for source, (table, _, _) in prepared.items()},
            "kusto_database": kusto_config["database"], "kusto_table": kusto_config["table"],
            "row_counts": {source: item["row_count"] for source, item in manifest["tables"].items()},
            "existing_destinations": existing,
        }
        print(json.dumps(plan, indent=2))
        if config["dry_run"]:
            return plan
        for source, (table, frame, exists) in prepared.items():
            LOG.warning("%s Delta table %s", "Replacing" if exists else "Creating", table)
            (
                frame.write.format("delta").mode("overwrite" if exists else "errorifexists")
                .option("overwriteSchema", "false").option("partitionOverwriteMode", "static")
                .saveAsTable(table)
            )
            written = spark.table(table)
            validate_schema(
                {field.name: field.dataType.simpleString() for field in frame.schema},
                {field.name: field.dataType.simpleString() for field in written.schema}, table,
            )
            if written.count() != manifest["tables"][source]["row_count"]:
                raise RuntimeError(f"{table}: post-write row count mismatch")
        # Format in Spark's UTC session before crossing into Python driver-local datetime handling.
        telemetry_transport = frames["product_telemetry"].withColumn(
            "event_at", F.date_format(F.col("event_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )
        publish_telemetry(
            client, telemetry_transport, kusto_config["table"], kusto_mapping,
            manifest["tables"]["product_telemetry"]["row_count"],
            target_order=list(actual) if actual is not None else None,
            replace_existing=actual is not None,
        )
        summary_path = f"{base_path}/load_{manifest['run_id']}_{uuid.uuid4().hex[:8]}.json"
        if notebookutils.fs.put(summary_path, json.dumps(plan, indent=2) + "\n", False) is False:
            raise IOError(f"Tables were loaded but the load receipt could not be saved: {summary_path}")
        return plan
    finally:
        for frame in cached:
            frame.unpersist()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

namespace = f"{identifier(DeltaSchema)}." if DeltaSchema else ""
if DeltaTablePrefix:
    identifier(DeltaTablePrefix + "table")
if not isinstance(DeltaOverrides, dict) or set(DeltaOverrides) - set(DELTA_TABLES):
    raise ValueError("DeltaOverrides may only contain approved non-telemetry table names")
delta_targets = {
    source: {
        "table": f"{namespace}{DeltaTablePrefix}{source}",
        "columns": {},
    }
    for source in DELTA_TABLES
}
for source, override in DeltaOverrides.items():
    delta_targets[source].update(override)
load_config = {
    "source_root": SourceRoot,
    "dry_run": DryRun,
    "replace_confirmation": ReplaceConfirmation,
    "delta_tables": delta_targets,
    "kusto": {
        "query_uri": KustoQueryUri, "database": KustoDatabase,
        "table": KustoTable, "columns": KustoColumnMappings,
    },
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Create or refresh tables
# 
# After a successful dry run, change these two parameters in notebook 02 and
# **Run all** again:
# 
# ```python
# DryRun = False
# ReplaceConfirmation = "REPLACE THETA DEMO DATA"
# ```
# 
# Missing tables are created. Existing tables are replaced only with the exact
# confirmation above. By default, the result is:
# 
# - Eleven Lakehouse Delta tables named `theta360_v2_<source_table>`.
# - One Kusto table named `theta360_v2_product_telemetry`.
# - A successful-load receipt under `Files/theta360_v2`.
# 
# All twelve CSVs remain in Lakehouse Files, but telemetry is **not** loaded into
# a Delta table. Kusto telemetry is staged and verified before publication;
# failed staging tables are retained for diagnosis.
# 
# Authentication uses `notebookutils.credentials.getToken("kusto")`; no secrets
# belong in notebook parameters. Successful events have a null `error_message`
# in CSV/Spark and an empty string in Kusto, which does not preserve string nulls.
# 
# **Replacement affects the entire configured table. Stop concurrent writers.
# There is no transaction spanning all tables and services:** a later failure
# can leave earlier Delta tables updated. Inspect errors before rerunning.
# Post-write row counts and Delta types are checked automatically.


# CELL ********************

import notebookutils

load_plan = refresh_tables(spark, notebookutils, BasePath, load_config)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Regenerate later
# 
# Run notebook **01** again with an empty `ReferenceDate`, then run notebook
# **02**, dry run first and confirmed refresh second. The new snapshot becomes
# the latest; previous snapshots remain available.
# 
# Use `SourceRoot` to reload an earlier completed snapshot. The loader checks
# the manifest's Lakehouse identity to prevent accidentally loading files copied
# from a different Lakehouse.
# 
# ### Optional destination mappings
# 
# `DeltaOverrides` supports explicit table and column mappings, for example:
# 
# ```python
# DeltaOverrides = {
#     "customers": {
#         "table": "dbo.theta360_v2_customers",
#         "columns": {"customer_id": "CustomerId"},
#     }
# }
# ```
# 
# Unspecified tables and columns retain their defaults. `KustoColumnMappings`
# supports the same source-to-target column renaming for telemetry.
# Do not point these settings at v1 or production tables unless replacement is
# intentional. The defaults never modify v1 paths or table names.
# 
# These are import-ready notebook artifacts, not an already executed Fabric
# deployment. Both notebooks use Fabric's existing `spark` session. They target
# the default Lakehouse and public-cloud Fabric/ADX Kusto query endpoints.

