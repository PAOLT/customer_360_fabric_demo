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

# # Theta customer 360 v2 - OneLake table and column descriptions
# 
# Run this notebook **after notebook 02 has successfully created/refreshed the
# tables**. It contains generated descriptions for **11 tables and 57 columns**.
# This corrected version uses the documented **OneLake additional table metadata
# API (public preview)**, which stores table/column descriptions separately from
# Spark/Delta comments. The earlier notebook updated Delta column comments only;
# those comments do not populate the Description fields in Lakehouse Overview.
# It also omitted table descriptions. Refreshing the UI cannot fix that mismatch.
# 
# This notebook writes the OneLake metadata used for table/column descriptions.
# It does not insert, delete, overwrite, or profile rows, alter column types,
# or modify existing Delta comments. Telemetry is in Kusto and is excluded.
# 
# **Recovery fix:** OneLake responses can include a server-returned `columnId`
# alongside `columnName`. This version accepts that field on reads and verifies
# descriptions/tags independently of server identifiers. Requests use the documented
# `columnName` contract, not `columnId`. Existing descriptions and tags are retained.
# If an earlier run stopped on "Unexpected OneLake column metadata fields",
# rerun this version in dry-run mode first: any descriptions already applied
# are recognized and skipped. No data regeneration is needed.
# 
# ## 1. Prepare and configure
# 
# Import this notebook into Fabric and attach/pin the **same default Lakehouse**
# used by notebooks 01 and 02. Use its existing PySpark session; no package
# installation, external description file or CSV snapshot is needed.
# Authentication uses `notebookutils.credentials.getToken("storage")`.
# Your identity needs read access to target tables and write access to the
# Lakehouse item and OneLake location. The public-preview API must be available.
# HTTP errors are reported explicitly; no private/undocumented API is used.
# Restart the session if you change the default Lakehouse.
# 
# Copy `DeltaSchema`, `DeltaTablePrefix`, and `DeltaOverrides` from notebook 02
# if you changed its destination settings. Table overrides must use `table` or
# `schema.table` in the default Lakehouse, not a path or cross-Lakehouse name.
# The defaults target only `theta360_v2_*`, not v1 or production tables.
# Review the generated descriptions in the embedded dictionary if desired.
# 
# **For tables named `dbo.subscriptions`, `dbo.customers`, etc. without the
# `theta360_v2_` prefix, set `DeltaSchema = "dbo"` and `DeltaTablePrefix = ""`.**
# This matches the unprefixed tables in the reported Lakehouse screenshot.
# A blank schema resolves to `dbo`; non-schema-enabled Lakehouses must expose
# their tables in that namespace through the table API, otherwise preflight fails.
# 
# Workspace and Lakehouse GUIDs come from the attached default Lakehouse's
# runtime context (including its own workspace when different from the notebook).
# There are no hardcoded workspace or Lakehouse IDs.


# PARAMETERS CELL ********************

DeltaSchema = "dbo"  # Match the schema used by notebook 02.
DeltaTablePrefix = ""  # Use "" for unprefixed tables such as dbo.subscriptions.
DeltaOverrides = {}  # Same optional table/column mappings as notebook 02.
DryRun = False
OverwriteExistingDescriptions = False
UpdateConfirmation = "UPDATE THETA ONELAKE DESCRIPTIONS"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Preview, then apply
# 
# Leave `DryRun = True` and select **Run all**. The notebook checks **all eleven
# tables** exist, are Delta, and have the expected mapped column names/types
# and reads the current OneLake metadata before building a preview with one
# table-description row (blank column name) and one row per column:
# `update`, `unchanged`, or `preserve_existing`. Missing/incompatible tables
# or failed metadata reads stop preflight before any writes. Column order
# does not matter. The preview includes old and generated descriptions.
# 
# Existing nonempty descriptions are preserved by default. To intentionally
# replace differing descriptions with the generated versions, set
# `OverwriteExistingDescriptions = True`. Identical comments are always skipped.
# 
# After reviewing the preview, edit the **parameters cell** to set:
# 
# ```python
# DryRun = False
# UpdateConfirmation = "UPDATE THETA ONELAKE DESCRIPTIONS"
# ```
# 
# Select **Run all** again. The notebook rebuilds the plan, saves a verified
# before/after backup in `Files/theta360_v2_description_backups/`, then uses a
# read-modify-write request for each changed table and verifies the result with
# another GET. Existing table/column tags and unmodified descriptions are retained.
# Matching descriptions are skipped; repeated runs do not duplicate metadata.
# 
# Stop concurrent schema/metadata updates and notebook 02 refreshes while this
# runs. The API **replaces the full additional metadata document**, not just the
# submitted fields, so retaining tags and other descriptions is essential.
# The notebook rechecks metadata before writing, but the API documents no
# conditional-write lock: a concurrent writer can still race the update.
# There is no cross-table transaction; failures can leave earlier tables updated.
# No automatic POST retries are used. After any uncertain write, rerun with
# `DryRun = True` to inspect current metadata before applying again.


# CELL ********************

EXPECTED_SCHEMAS = {'customers': {'customer_id': 'string', 'name': 'string', 'country': 'string'},
 'customer_account_executives': {'customer_id': 'string', 'account_executive_id': 'string'},
 'products': {'product_id': 'string', 'name': 'string', 'description': 'string'},
 'employees': {'employee_id': 'string', 'name': 'string', 'role': 'string'},
 'employee_certifications': {'employee_id': 'string', 'product_id': 'string', 'expiry_date': 'date'},
 'sales_opportunities': {'opportunity_id': 'string',
                         'customer_id': 'string',
                         'product_id': 'string',
                         'account_executive_id': 'string',
                         'start_date': 'date',
                         'end_date': 'date',
                         'state': 'string',
                         'monthly_recurring_revenue': 'decimal(18,2)',
                         'currency': 'string'},
 'sales_milestones': {'milestone_id': 'string',
                      'opportunity_id': 'string',
                      'activity_at': 'timestamp',
                      'objective': 'string',
                      'outcome': 'string',
                      'sales_engineer_id': 'string'},
 'support_tickets': {'ticket_id': 'string',
                     'customer_id': 'string',
                     'product_id': 'string',
                     'opened_at': 'timestamp',
                     'state': 'string',
                     'closed_at': 'timestamp',
                     'severity': 'string',
                     'description': 'string'},
 'support_activities': {'activity_id': 'string',
                        'ticket_id': 'string',
                        'technical_engineer_id': 'string',
                        'started_at': 'timestamp',
                        'ended_at': 'timestamp',
                        'objective': 'string',
                        'outcome': 'string'},
 'subscriptions': {'subscription_id': 'string',
                   'customer_id': 'string',
                   'product_id': 'string',
                   'start_date': 'date',
                   'end_date': 'date',
                   'monthly_fee': 'decimal(18,2)',
                   'currency': 'string'},
 'subscription_payments': {'payment_id': 'string',
                           'subscription_id': 'string',
                           'billing_month': 'date',
                           'paid_on': 'date',
                           'amount': 'decimal(18,2)',
                           'currency': 'string'}}

COLUMN_DESCRIPTIONS = {'customers': {'customer_id': 'Unique synthetic identifier of a Theta customer; primary key used to join '
                              'customer facts and account ownership.',
               'name': 'Synthetic name of the restaurant-chain customer organization subscribing to Theta '
                       'software.',
               'country': 'Country of the customer organization in Western Europe; stored as a country '
                          'name.'},
 'customer_account_executives': {'customer_id': 'Customer assigned to the account executive; references '
                                                'customers.customer_id and uniquely identifies this '
                                                'assignment.',
                                 'account_executive_id': 'Employee responsible for the customer account; '
                                                         'references employees.employee_id with role AE. One '
                                                         'account executive is assigned per customer.'},
 'products': {'product_id': 'Unique identifier of a Theta SaaS software module; primary key for '
                            'product-related facts and certifications.',
              'name': 'Software module name: Warehouse, HR, Compliance, Fidelity Cards, Restaurant Bookings, '
                      'or Finance.',
              'description': 'Business capabilities provided by the software module.'},
 'employees': {'employee_id': 'Unique synthetic employee identifier; primary key for account ownership, '
                              'sales engineering, support work, and certifications.',
               'name': 'Synthetic employee full name.',
               'role': 'Employee role code: AE = Account Executive, SE = Sales Engineer, TE = Technical '
                       'Engineer.'},
 'employee_certifications': {'employee_id': 'Certified employee; references employees.employee_id. Together '
                                            'with product_id, identifies one employee-module certification.',
                             'product_id': 'Software module covered by the certification; references '
                                           'products.product_id.',
                             'expiry_date': 'Last calendar date on which the module certification is valid, '
                                            'inclusive. Compare with the activity date for historical '
                                            'qualification or the analysis date for current eligibility.'},
 'sales_opportunities': {'opportunity_id': 'Unique identifier of a sales opportunity; primary key referenced '
                                           'by sales milestones.',
                         'customer_id': 'Customer associated with the sales opportunity; references '
                                        'customers.customer_id.',
                         'product_id': 'Software module being sold in the opportunity; references '
                                       'products.product_id.',
                         'account_executive_id': 'Account Executive leading the opportunity; references '
                                                 'employees.employee_id with role AE and matches the '
                                                 'customer account assignment.',
                         'start_date': 'Calendar date on which the sales opportunity started.',
                         'end_date': 'Expected close date when state is open; actual close date when state '
                                     'is win, lost, or closed. Generated expected dates span reference date '
                                     '+10 to +120 days; actual dates span the preceding three calendar '
                                     'months, excluding the reference date.',
                         'state': 'Sales status: open = actively pursued; win = won; lost = rejected or '
                                  'competitor selected; closed = abandoned without a decision.',
                         'monthly_recurring_revenue': 'Monthly recurring revenue (MRR) associated with the '
                                                      'opportunity, in EUR with two decimal places. This is '
                                                      'potential opportunity revenue, not a payment or an '
                                                      'amount to add to subscribed MRR.',
                         'currency': 'ISO currency code for opportunity monthly recurring revenue; EUR in '
                                     'this dataset.'},
 'sales_milestones': {'milestone_id': 'Unique identifier of a sales activity or milestone; primary key.',
                      'opportunity_id': 'Sales opportunity to which the milestone belongs; references '
                                        'sales_opportunities.opportunity_id.',
                      'activity_at': 'UTC timestamp of the sales activity. Use the latest milestone per '
                                     'opportunity to assess recency and sentiment.',
                      'objective': 'Short text describing the purpose of the sales activity; consider with '
                                   'outcome when interpreting customer sentiment.',
                      'outcome': 'Short text describing the sales activity result and customer response; '
                                 'sentiment is inferred from this evidence, not stored as a flag.',
                      'sales_engineer_id': 'Sales Engineer leading the milestone; references '
                                           'employees.employee_id with role SE and a valid certification for '
                                           'the opportunity module on the activity date.'},
 'support_tickets': {'ticket_id': 'Unique identifier of a customer support ticket; primary key referenced by '
                                  'support activities.',
                     'customer_id': 'Customer reporting the issue; references customers.customer_id.',
                     'product_id': 'Software module affected by the ticket; references products.product_id.',
                     'opened_at': 'UTC timestamp when the support ticket was opened; use to calculate ticket '
                                  'age.',
                     'state': 'Support ticket status: open = unresolved ticket; close = closed ticket. The '
                              'closed-state value is close, not closed.',
                     'closed_at': 'UTC timestamp when the ticket was closed; null while the ticket is open.',
                     'severity': 'Support impact category: high, medium, or low. Open medium/high tickets '
                                 'for the same customer and module contribute to opportunity risk.',
                     'description': 'Customer issue description, including an error code or failure '
                                    'signature where applicable; can be compared with telemetry errors and '
                                    'historical resolutions.'},
 'support_activities': {'activity_id': 'Unique identifier of a support work activity; primary key.',
                        'ticket_id': 'Support ticket being worked on; references support_tickets.ticket_id.',
                        'technical_engineer_id': 'Technical Engineer performing this activity; references '
                                                 'employees.employee_id with role TE and a valid module '
                                                 'certification at the activity start. Engineer ownership is '
                                                 'recorded on activities, not tickets.',
                        'started_at': 'UTC timestamp when the support activity started.',
                        'ended_at': 'UTC timestamp when the support activity ended; null denotes ongoing '
                                    'work. An engineer with any ongoing activity is busy.',
                        'objective': 'Short text describing the investigation or remediation goal for the '
                                     'support activity.',
                        'outcome': 'Textual result of completed support work, including successful '
                                   'resolution evidence where applicable; null for an ongoing activity. '
                                   'Similar successful outcomes support engineer recommendations.'},
 'subscriptions': {'subscription_id': 'Unique identifier of a customer-module subscription; primary key '
                                      'referenced by subscription payments.',
                   'customer_id': 'Customer holding the subscription; references customers.customer_id.',
                   'product_id': 'Subscribed software module; references products.product_id.',
                   'start_date': 'First calendar date of the subscription period, inclusive.',
                   'end_date': 'Last calendar date of the subscription period, inclusive; use with '
                               'start_date to determine whether the subscription is active on an analysis '
                               'date.',
                   'monthly_fee': 'Monthly subscription fee in EUR with two decimal places. Sum for '
                                  'subscriptions active on the analysis date to calculate subscribed MRR.',
                   'currency': 'ISO currency code for the subscription monthly fee; EUR in this dataset.'},
 'subscription_payments': {'payment_id': 'Unique identifier of a recorded monthly subscription payment; '
                                         'primary key.',
                           'subscription_id': 'Subscription covered by the payment; references '
                                              'subscriptions.subscription_id. Join through subscriptions for '
                                              'customer and software module.',
                           'billing_month': 'First calendar day of the month covered by this subscription '
                                            'payment; represents the billing month, not the payment date.',
                           'paid_on': 'Calendar date on which the monthly subscription payment was made.',
                           'amount': 'Recorded payment amount in EUR with two decimal places; a historical '
                                     'payment, not a current MRR measure.',
                           'currency': 'ISO currency code for the payment amount; EUR in this dataset.'}}

TABLE_DESCRIPTIONS = {'customers': "Synthetic restaurant-chain customers of Theta's SaaS product. One row per customer "
              'organization, identified by customer_id, with name and Western European country. Join '
              'customer facts by customer_id; account ownership is in customer_account_executives.',
 'customer_account_executives': 'Current account ownership for Theta customers. One row per customer links '
                                'customer_id to an employee with role AE (Account Executive). Kept separate '
                                'from customer master data.',
 'products': 'Theta SaaS software-module catalog: Warehouse, HR, Compliance, Fidelity Cards, Restaurant '
             'Bookings, and Finance. One row per module, identified by product_id.',
 'employees': 'Synthetic Theta employees with roles AE (Account Executive), SE (Sales Engineer), or TE '
              '(Technical Engineer). Module qualifications and expiry dates are in employee_certifications.',
 'employee_certifications': 'Employee qualifications for individual software modules. One row per '
                            'employee/module pair with an inclusive expiry date. Use the activity date to '
                            'assess historical qualification, or the analysis date for current eligibility.',
 'sales_opportunities': 'CRM opportunities for customer/module sales, including AE, dates, status, and '
                        'potential monthly recurring revenue in EUR. end_date is expected for open '
                        'opportunities and actual otherwise. Derive slippage risk from milestone inactivity, '
                        'latest milestone sentiment, and open medium/high support tickets for the same '
                        'customer/module.',
 'sales_milestones': 'CRM sales activities led by module-certified Sales Engineers, linked to sales '
                     'opportunities. Timestamp, objective, and outcome provide evidence of engagement '
                     'recency and customer sentiment; derived risk and sentiment flags are not stored.',
 'support_tickets': 'Customer support issues by software module, with severity, UTC open/close timestamps, '
                    'state, and issue description. Open tickets have null closed_at. Engineer work is '
                    'recorded in support_activities rather than assigned on tickets.',
 'support_activities': 'Technical Engineer work on support tickets, with UTC start/end timestamps, '
                       'objectives, and textual outcomes. Null ended_at denotes ongoing work and makes the '
                       'engineer busy. Completed successful resolutions, together with current module '
                       'certification and availability, support engineer recommendations.',
 'subscriptions': 'Customer subscriptions to individual Theta software modules, with inclusive start/end '
                  'dates and monthly fees in EUR. One row per subscription. Sum monthly_fee for '
                  'subscriptions active on an analysis date to calculate subscribed MRR; payments are '
                  'recorded separately.',
 'subscription_payments': 'Historical monthly subscription payments in EUR. One row per recorded payment '
                          'references a subscription, billing_month, and paid_on date. Join subscriptions '
                          'for customer and module. These are payments, not a measure of current subscribed '
                          'MRR.'}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import copy
import json
import logging
import re
import uuid
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import HTTPRedirectHandler, Request, build_opener

LOG = logging.getLogger("theta360.descriptions")
logging.basicConfig(level=logging.INFO)


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid identifier: {value!r}")
    return value


def table_sql(value):
    if not isinstance(value, str):
        raise ValueError("Table names must be strings")
    parts = value.split(".")
    if len(parts) not in (1, 2):
        raise ValueError("Use table or schema.table in the attached default Lakehouse")
    return ".".join(f"`{identifier(part)}`" for part in parts)


def targets(delta_schema, prefix, overrides):
    namespace = identifier(delta_schema) if delta_schema else "dbo"
    if not isinstance(prefix, str):
        raise ValueError("DeltaTablePrefix must be a string")
    if not isinstance(overrides, dict) or set(overrides) - set(EXPECTED_SCHEMAS):
        raise ValueError("DeltaOverrides may only contain approved non-telemetry tables")
    result = {}
    seen_tables = set()
    for source, expected in EXPECTED_SCHEMAS.items():
        override = overrides.get(source, {})
        if not isinstance(override, dict) or set(override) - {"table", "columns"}:
            raise ValueError(f"Invalid override settings for {source}")
        table = override.get("table", f"{namespace}.{prefix}{source}")
        table_sql(table)
        if "." not in table:
            table = f"{namespace}.{table}"
        if table.lower() in seen_tables:
            raise ValueError(f"Duplicate target table: {table}")
        seen_tables.add(table.lower())
        mapping = override.get("columns", {})
        if not isinstance(mapping, dict) or set(mapping) - set(expected):
            raise ValueError(f"Invalid source columns in mapping for {source}")
        columns = {name: identifier(mapping.get(name, name)) for name in expected}
        if len(set(name.lower() for name in columns.values())) != len(columns):
            raise ValueError(f"Duplicate target columns for {source}")
        result[source] = {"table": table, "columns": columns}
    return result


def inspect_table(spark, table, expected):
    quoted = table_sql(table)
    if not spark.catalog.tableExists(table):
        raise ValueError(f"Missing table {table}; run notebook 02 to create tables first")
    detail = spark.sql(f"DESCRIBE DETAIL {quoted}").first().asDict()
    if detail.get("format", "").lower() != "delta" or not detail.get("id"):
        raise ValueError(f"Expected a physical Delta table: {table}")
    spark.catalog.refreshTable(table)
    fields = {field.name: field for field in spark.table(table).schema.fields}
    actual = {name: field.dataType.simpleString() for name, field in fields.items()}
    if actual != expected:
        raise ValueError(f"Schema mismatch for {table}: expected {expected}, found {actual}")
    return detail["id"], fields


def canonical_metadata(value):
    if not isinstance(value, dict) or set(value) - {"description", "tags", "columns"}:
        raise ValueError("Unexpected OneLake metadata shape; refusing a potentially lossy replacement")

    def description(item):
        text = item.get("description", "")
        if not isinstance(text, str) or len(text) > 1024:
            raise ValueError("Metadata descriptions must be strings of at most 1024 characters")
        return text

    def tags(item):
        result = item.get("tags", {})
        if not isinstance(result, dict) or any(
            not isinstance(key, str) or not isinstance(text, str)
            or len(key) > 128 or len(text) > 256 for key, text in result.items()
        ):
            raise ValueError("Unexpected OneLake tag format or tag length")
        return dict(result)

    columns = value.get("columns", [])
    if not isinstance(columns, list):
        raise ValueError("OneLake metadata columns must be an array")
    normalized = {}
    for column in columns:
        if not isinstance(column, dict):
            raise ValueError("OneLake column metadata must be an object")
        unknown = set(column) - {"columnId", "columnName", "description", "tags"}
        if unknown:
            raise ValueError(
                f"Unexpected OneLake column metadata fields: {sorted(unknown)}; "
                "refusing to discard metadata"
            )
        name = column.get("columnName")
        if not isinstance(name, str) or not name or name in normalized:
            raise ValueError("Missing or duplicate OneLake column metadata name")
        normalized[name] = {"description": description(column), "tags": tags(column)}
    return {"description": description(value), "tags": tags(value), "columns": normalized}


def writable_metadata(value):
    canonical_metadata(value)
    result = copy.deepcopy(value)
    for column in result.get("columns", []):
        # OneLake returns columnId; the documented POST contract identifies columns by name.
        column.pop("columnId", None)
    return result


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class MetadataClient:
    def __init__(self, notebookutils):
        self.notebookutils = notebookutils
        context = notebookutils.runtime.context
        self.workspace_id = str(uuid.UUID(
            context.get("defaultLakehouseWorkspaceId") or context["currentWorkspaceId"]
        ))
        self.item_id = str(uuid.UUID(context["defaultLakehouseId"]))
        self.opener = build_opener(NoRedirect())

    def request(self, method, table, payload=None):
        parts = table.split(".")
        if len(parts) != 2:
            raise ValueError("The OneLake API needs schema.table")
        schema, name = (identifier(part) for part in parts)
        url = (
            "https://onelake.table.fabric.microsoft.com/common/v1/"
            f"{self.workspace_id}/{self.item_id}/schemas/{quote(schema, safe='')}/"
            f"tables/{quote(name, safe='')}/additionalMetadata"
        )
        headers = {
            "Authorization": "Bearer " + self.notebookutils.credentials.getToken("storage"),
            "Accept": "application/json",
            "x-ms-root-activity-id": str(uuid.uuid4()),
        }
        data = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(writable_metadata(payload), ensure_ascii=True).encode("utf-8")
        request = Request(url, headers=headers, data=data, method=method)
        try:
            with self.opener.open(request, timeout=60) as response:
                if response.status != 200:
                    raise RuntimeError(f"OneLake {method} {table}: expected HTTP 200, got {response.status}")
                result = json.load(response)
        except HTTPError as error:
            try:
                detail = error.read(8192).decode("utf-8", errors="replace")
                activity = error.headers.get("x-ms-root-activity-id", "")
            finally:
                error.close()
            raise RuntimeError(
                f"OneLake {method} {table} failed: HTTP {error.code}; activity {activity}; {detail}. "
                "Check preview API availability, storage-token access, item/OneLake write permissions, "
                "and the configured schema/table name. No automatic POST retry was attempted."
            ) from None
        except URLError as error:
            raise RuntimeError(
                f"OneLake {method} {table} network failure: {error.reason}. "
                "If a POST was sent, its outcome is uncertain; rerun the dry run to inspect current metadata."
            ) from None
        canonical_metadata(result)
        return result


def build_plan(spark, client, delta_schema, prefix, overrides, overwrite_existing):
    if not isinstance(overwrite_existing, bool):
        raise ValueError("OverwriteExistingDescriptions must be True or False")
    plan = []
    for source, target in targets(delta_schema, prefix, overrides).items():
        table = target["table"]
        expected = {target["columns"][name]: kind for name, kind in EXPECTED_SCHEMAS[source].items()}
        table_id, fields = inspect_table(spark, table, expected)
        before = client.request("GET", table)
        normalized = canonical_metadata(before)
        if set(normalized["columns"]) - set(fields):
            raise ValueError(f"Metadata references columns absent from {table}; inspect before replacing")
        after = copy.deepcopy(before)
        columns = after.setdefault("columns", [])
        by_name = {column["columnName"]: column for column in columns}
        entries = []
        proposed = [("", TABLE_DESCRIPTIONS[source])] + [
            (target_column, COLUMN_DESCRIPTIONS[source][source_column])
            for source_column, target_column in target["columns"].items()
        ]
        for target_column, description in proposed:
            if not isinstance(description, str) or not description.strip() or len(description) > 1024:
                raise ValueError(f"Invalid description for {source}.{target_column}")
            if target_column:
                destination = by_name.get(target_column)
                previous = destination.get("description", "") if destination else ""
            else:
                destination = after
                previous = after.get("description", "")
            if previous == description:
                action = "unchanged"
            elif previous and not overwrite_existing:
                action = "preserve_existing"
            else:
                action = "update"
            if action == "update":
                if destination is None:
                    destination = {"columnName": target_column}
                    columns.append(destination)
                    by_name[target_column] = destination
                destination["description"] = description
            entries.append({
                "source_table": source, "table": table, "column": target_column,
                "previous_description": previous, "generated_description": description,
                "action": action,
            })
        canonical_metadata(after)
        plan.append({
            "table": table, "id": table_id, "schema": expected,
            "before": before, "after": after, "entries": entries,
        })
    return plan


def check_current(spark, client, target):
    table_id, _ = inspect_table(spark, target["table"], target["schema"])
    if table_id != target["id"]:
        raise RuntimeError(f"Table was recreated after preflight: {target['table']}; rerun")
    current = client.request("GET", target["table"])
    if canonical_metadata(current) != canonical_metadata(target["before"]):
        raise RuntimeError(f"OneLake metadata changed after preflight: {target['table']}; rerun")


def apply_plan(spark, client, notebookutils, plan, confirmation):
    if confirmation != "UPDATE THETA ONELAKE DESCRIPTIONS":
        raise ValueError('Set UpdateConfirmation = "UPDATE THETA ONELAKE DESCRIPTIONS" to apply')
    for target in plan:
        check_current(spark, client, target)
    if not any(entry["action"] == "update" for target in plan for entry in target["entries"]):
        return 0
    backup_path = f"Files/theta360_v2_description_backups/{uuid.uuid4().hex}.json"
    notebookutils.fs.mkdirs("Files/theta360_v2_description_backups")
    backup = json.dumps({
        "workspace_id": client.workspace_id, "lakehouse_id": client.item_id, "tables": plan
    }, indent=2)
    if notebookutils.fs.put(backup_path, backup, False) is False:
        raise IOError("Could not save pre-update metadata backup; no API writes attempted")
    if notebookutils.fs.head(backup_path, len(backup.encode("utf-8")) + 1) != backup:
        raise IOError("Metadata backup readback failed; no API writes attempted")
    print(f"Before/after metadata backup: {backup_path}")
    updated = 0
    for target in plan:
        count = sum(entry["action"] == "update" for entry in target["entries"])
        if not count:
            continue
        check_current(spark, client, target)
        LOG.info("Updating OneLake table/column descriptions: %s", target["table"])
        client.request("POST", target["table"], target["after"])
        verified = client.request("GET", target["table"])
        if canonical_metadata(verified) != canonical_metadata(target["after"]):
            raise RuntimeError(
                f"OneLake metadata readback failed: {target['table']}; inspect backup and rerun dry run"
            )
        table_id, _ = inspect_table(spark, target["table"], target["schema"])
        if table_id != target["id"]:
            raise RuntimeError(f"Table was recreated during metadata update: {target['table']}")
        updated += count
    return updated


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils

if not isinstance(DryRun, bool):
    raise ValueError("DryRun must be True or False")
context = notebookutils.runtime.context
if not context.get("defaultLakehouseId"):
    raise ValueError("Attach and pin the same default Lakehouse used by notebook 02")
print(f"Default Lakehouse: {context.get('defaultLakehouseName', '')} "
      f"({context['defaultLakehouseId']})")
metadata_client = MetadataClient(notebookutils)
print(f"OneLake API target: workspace {metadata_client.workspace_id}; "
      f"Lakehouse {metadata_client.item_id}")
description_plan = build_plan(
    spark, metadata_client, DeltaSchema, DeltaTablePrefix,
    DeltaOverrides, OverwriteExistingDescriptions
)
preview_rows = [entry for target in description_plan for entry in target["entries"]]
display(spark.createDataFrame(
    preview_rows,
    "source_table string, table string, column string, previous_description string, "
    "generated_description string, action string",
))
if DryRun:
    print("Dry run only: no descriptions changed. Review the preview before applying.")
else:
    updated_count = apply_plan(
        spark, metadata_client, notebookutils, description_plan, UpdateConfirmation
    )
    print(f"Verified OneLake descriptions for {len(description_plan)} tables and "
          f"{len(preview_rows) - len(description_plan)} columns; "
          f"updated {updated_count} descriptions. Refresh Lakehouse Overview to inspect.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Where the descriptions are stored
# 
# After successful API readback, reopen/refresh the table's **Lakehouse Overview**
# and inspect its Summary Description and Columns Description fields.
# The authoritative verification in this notebook is GET of the same OneLake
# additional metadata API, not Spark `DESCRIBE TABLE`.
# If the UI is still blank, confirm the printed Lakehouse ID and schema/table
# match the item you opened. An API error or a dry run is not an applied update.
# 
# OneLake additional metadata is separate from Delta comments, SQL endpoint
# extended properties, Power BI model descriptions, Kusto docstrings, and Purview
# annotations. This notebook does not update those other metadata stores.
# Do not assume propagation between them. The API is in public preview; if it is
# unavailable or rejects access, retain the error/activity ID and use the portal's
# **Add description** controls rather than treating a Delta comment as a substitute.
# 
# API contract and replacement semantics:
# [Microsoft Learn - additional table metadata](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/additional-table-metadata).
# 
# This download has not been run against your Fabric workspace. Import it and
# use the dry-run workflow above before applying descriptions.

