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

# # Theta customer 360 v2 - generate CSVs in OneLake
# 
# **Start here.** This notebook contains the setup and generation guide;
# notebook **02** contains the table-loading guide. No separate Markdown
# file, local Python installation, package upload or external config is needed.
# 
# Read the instructions below, edit the executable **parameters** cell,
# then select **Run all**. Markdown examples are documentation, not executable
# parameter cells. Leave implementation cells unchanged.
# 
# This notebook writes CSV snapshots only. Notebook **02** creates or refreshes
# the Delta and Kusto tables after you review a dry run.


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

# ## 2. Generate CSVs
# 
# Open notebook **01**, review its first code cell, then **Run all**:
# 
# ```python
# BasePath = "Files/theta360_v2"
# ReferenceDate = ""
# Seed = 42
# TelemetryEvents = 100_000
# RunId = ""
# ```
# 
# An empty `ReferenceDate` uses today's UTC date. The reference instant is
# midnight UTC; historical events are earlier than that instant. An empty
# `RunId` creates a unique run, so refreshing the demo never overwrites a
# previous CSV snapshot.
# 
# To reproduce a dated dataset, set `ReferenceDate = "2026-11-19"` and retain
# the same seed. Leave `RunId` empty to save it as a new snapshot.
# 
# The notebook generates and validates all scenarios, writes CSVs and metadata,
# then verifies each uploaded file's SHA-256 hash by reading it back from
# OneLake. A small temporary directory on the Fabric driver is used only while
# serializing/uploading files and is cleaned automatically.
# 
# ```text
# Files
#   theta360_v2
#     latest.json
#     runs
#       <unique-run-id>
#         csv
#           customers.csv
#           ...eleven other CSV files
#         manifest.json
#         validation_report.json
# ```
# 
# `latest.json` is updated only after the snapshot files have been verified.
# The notebook prints the completed snapshot path. It does **not** create or
# change Delta or Kusto tables.


# PARAMETERS CELL ********************

BasePath = "Files/theta360_v2"
ReferenceDate = ""  # Empty means today's UTC date; otherwise YYYY-MM-DD.
Seed = 42
TelemetryEvents = 100_000
RunId = ""  # Empty creates a unique run; existing runs are never overwritten.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dataset and date rules
# 
# V2 is separate from v1. All runtime Python, schemas, generation, validation,
# and loading logic is embedded in **two self-contained Fabric notebooks**.
# No local Python, package installation, resource upload, or external config file
# is required.
# 
# | Notebook | Purpose |
# |---|---|
# | `01_Generate_Theta360_v2.ipynb` | Generate twelve CSVs, a manifest, and scenario evidence directly into Lakehouse Files |
# | `02_Refresh_Theta360_v2.ipynb` | Read the CSVs, explicitly cast their types, and create/refresh eleven Delta tables plus telemetry in Kusto |
# 
# The approved model and business scenarios are unchanged: 20 synthetic
# customers, six modules, a rolling 12-month historical window, 100,000 telemetry
# events by default, EUR MRR, certification expiry, separate customer/AE pairing,
# and TE availability derived from ongoing activities. Risk, sentiment, usage
# trend, and availability flags are not added to factual CSVs. Documents remain
# deferred.
# 
# Opportunity close dates are distributed as follows:
# 
# - **Open:** expected close dates between reference date +10 and +120 days,
#   inclusive, spread across four portions of that horizon.
# - **Win, lost, closed:** actual close dates from reference date minus three
#   calendar months through the day before the reference date, spread across
#   all three rolling calendar-month intervals. Month-end subtraction is clamped
#   correctly; this is not a fixed 90-day window.
# 
# Dates are seeded and reproducible. Closed-opportunity milestones occur five
# days before the actual close, with opportunity starts adjusted when necessary
# to preserve chronology. Other source data and the table schemas are unchanged.


# MARKDOWN ********************

# ## Embedded schema, generation and business checks
# The following cells are implementation definitions; run them unchanged.
# Monetary values are EUR Decimal(18,2), dates are ISO dates, timestamps
# are UTC, and the CSV null marker is `\N`. The snapshot reference instant
# is 00:00 UTC on ReferenceDate.


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

"""Generate a deterministic, reference-date anchored customer-360 snapshot."""
import argparse
import calendar
import csv
import hashlib
import json
import random
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
MODULES = (('Warehouse', 'Fresh goods, canned goods, drinks and restaurant materials'), ('HR', 'Cooks, waiters, sommeliers, staffing and employee administration'), ('Compliance', 'Restaurant health, safety and regulatory compliance'), ('Fidelity Cards', 'Loyalty membership, customer rewards and fidelity cards'), ('Restaurant Bookings', 'Restaurant reservations, capacity and guest bookings'), ('Finance', 'Restaurant financial operations and reconciliation'))
COUNTRIES = ('France', 'Germany', 'Belgium', 'Netherlands', 'Austria', 'Ireland')
CUSTOMER_NAMES = ('Amber Table', 'Birch Dining', 'Copper Fork', 'Dune Kitchens', 'Elm Bistro', 'Flint Restaurants', 'Grove Dining', 'Harbor Plates', 'Indigo Kitchens', 'Juniper Table', 'Kestrel Dining', 'Linden Restaurants', 'Meadow Fork', 'Northstar Bistro', 'Olive Kitchens', 'Pebble Table', 'Quartz Dining', 'Riverstone Restaurants', 'Saffron Plates', 'Thistle Kitchens')
ACTIONS = (('reserve_inventory', 'receive_delivery', 'check_stock', 'record_waste'), ('publish_roster', 'record_shift', 'approve_leave', 'view_employee'), ('submit_health_check', 'record_temperature', 'view_audit', 'update_checklist'), ('enrol_member', 'issue_points', 'redeem_reward', 'view_balance'), ('create_booking', 'update_booking', 'seat_guest', 'check_availability'), ('reconcile_payment', 'post_expense', 'view_ledger', 'export_statement'))
ERRORS = ('WH-409 inventory reservation lock timeout', 'HR-422 roster validation rejected', 'CP-503 compliance checklist service unavailable', 'FC-504 loyalty balance request timeout', 'RB-409 reservation capacity conflict', 'FN-502 finance reconciliation gateway failure')

def shift_months(value, months):
    month_index = value.year * 12 + value.month - 1 + months
    year, month = divmod(month_index, 12)
    return value.replace(year=year, month=month + 1, day=min(value.day, calendar.monthrange(year, month + 1)[1]))

def at(value, hour=0):
    return datetime.combine(value, time(hour), tzinfo=timezone.utc)

def snapshot(reference_date, seed=42, telemetry_events=100000):
    rng = random.Random(seed)
    reference = at(reference_date)
    history_start = shift_months(reference_date, -12)
    month_start = reference_date.replace(day=1)
    first_full_month = history_start.replace(day=1)
    if first_full_month < history_start:
        first_full_month = shift_months(first_full_month, 1)
    data = {name: [] for name in SCHEMAS}

    def add(table, **values):
        data[table].append(values)
        return values

    def employee(employee_id, name, role):
        return add('employees', employee_id=employee_id, name=name, role=role)
    for i, (name, description) in enumerate(MODULES, 1):
        add('products', product_id=f'P{i:02}', name=name, description=description)
    for i, name in enumerate(('Avery Vale', 'Blair Reed', 'Casey Brook', 'Drew Moss'), 1):
        employee(f'AE{i:02}', name, 'AE')
    for i, name in enumerate(('Ellis Finch', 'Frankie Lake', 'Gray Rowan', 'Harper Field', 'Indy Oak', 'Jules Pine'), 1):
        employee(f'SE{i:02}', name, 'SE')
        for offset in (0, 1, 2):
            add('employee_certifications', employee_id=f'SE{i:02}', product_id=f'P{(i - 1 + offset) % 6 + 1:02}', expiry_date=reference_date + timedelta(days=180 if offset < 2 else -7))
    for i, name in enumerate(('Kai Stone', 'Lane Birch', 'Morgan Ash', 'Noel Elm', 'Parker Fern', 'Quinn Willow', 'Riley Cedar', 'Sky Alder'), 1):
        employee(f'TE{i:02}', name, 'TE')
        for product in range(1, 7) if i >= 7 else (i,):
            add('employee_certifications', employee_id=f'TE{i:02}', product_id=f'P{product:02}', expiry_date=reference_date + timedelta(days=-7 if i == 8 else 240))
    for i, name in enumerate(CUSTOMER_NAMES, 1):
        add('customers', customer_id=f'C{i:03}', name=f'{name} Group', country=COUNTRIES[(i - 1) % len(COUNTRIES)])
        add('customer_account_executives', customer_id=f'C{i:03}', account_executive_id=f'AE{(i - 1) % 4 + 1:02}')
    for i in range(1, 21):
        product_numbers = (1, (i - 1) % 5 + 2, i % 5 + 2)
        for product in product_numbers:
            start = shift_months(first_full_month, 0 if product == 1 else rng.randrange(4))
            if product != 1 and i % 7 == 0:
                end = shift_months(month_start, -4) - timedelta(days=1)
            else:
                end = shift_months(month_start, 6 + i % 6) - timedelta(days=1)
            fee = Decimal(150 + product * 95 + i % 5 * 35).quantize(Decimal('0.01'))
            subscription = add('subscriptions', subscription_id=f'SUB{i:03}-{product:02}', customer_id=f'C{i:03}', product_id=f'P{product:02}', start_date=start, end_date=end, monthly_fee=fee, currency='EUR')
            billing = start
            while billing <= min(end, reference_date):
                paid_on = billing + timedelta(days=2)
                if paid_on < reference_date:
                    add('subscription_payments', payment_id=f"PAY-{subscription['subscription_id']}-{billing:%Y%m}", subscription_id=subscription['subscription_id'], billing_month=billing, paid_on=paid_on, amount=fee, currency='EUR')
                billing = shift_months(billing, 1)

    def milestone(opportunity, days_ago, negative=False):
        return add('sales_milestones', milestone_id=f"MS{len(data['sales_milestones']) + 1:04}", opportunity_id=opportunity['opportunity_id'], activity_at=at(reference_date - timedelta(days=days_ago), 10), objective='Review customer concerns about the proposed module expansion' if negative else 'Demonstrate the module and agree purchase next steps', outcome='Customer is unhappy; unresolved integration concerns blocked approval.' if negative else 'Customer approved the demonstration and confirmed purchase next steps.', sales_engineer_id=f"SE{int(opportunity['product_id'][1:]):02}")
    close_date_rng = random.Random(seed)
    close_date_counts = {'open': 0, 'actual': 0}

    def opportunity(customer, product=1, state='open', inactive=False, negative=False, no_milestones=False, age=50, end_age=35):
        start = reference_date - timedelta(days=age)
        if state == 'open':
            band = close_date_counts['open'] % 4
            lower = 10 + band * 111 // 4
            upper = 9 + (band + 1) * 111 // 4
            end = reference_date + timedelta(days=close_date_rng.randint(lower, upper))
            close_date_counts['open'] += 1
        else:
            band = close_date_counts['actual'] % 3
            lower = shift_months(reference_date, band - 3)
            upper = shift_months(reference_date, band - 2)
            end = lower + timedelta(days=close_date_rng.randrange((upper - lower).days))
            close_date_counts['actual'] += 1
            start = min(start, end - timedelta(days=40))
        row = add('sales_opportunities', opportunity_id=f"OPP{len(data['sales_opportunities']) + 1:04}", customer_id=f'C{customer:03}', product_id=f'P{product:02}', account_executive_id=f'AE{(customer - 1) % 4 + 1:02}', start_date=start, end_date=end, state=state, monthly_recurring_revenue=Decimal(200 + product * 100 + customer * 10).quantize(Decimal('0.01')), currency='EUR')
        if not no_milestones:
            if state == 'open':
                milestone(row, min(age - 1, 40), negative=not negative)
                milestone(row, 21 if inactive else 3, negative=negative)
            else:
                milestone(row, (reference_date - end).days + 5, negative=state in ('closed', 'lost'))
        return row
    for customer, inactive, negative in ((1, True, False), (2, False, True), (3, False, False), (4, True, True), (5, True, False), (6, False, True), (7, True, True), (8, False, False)):
        opportunity(customer, inactive=inactive, negative=negative)
    opportunity(9, no_milestones=True, age=5)
    opportunity(10, no_milestones=True, age=20)
    opportunity(12, state='lost', age=80)
    opportunity(13, state='closed', age=80)
    opportunity(14)
    opportunity(14, state='lost', age=80)
    opportunity(15)
    opportunity(16)
    for customer in range(17, 21):
        end_age = 50 + (customer - 17) * 65
        opportunity(customer, state='win', age=end_age + 40, end_age=end_age)
    for subscription in data['subscriptions']:
        product = int(subscription['product_id'][1:])
        if product == 1:
            continue
        customer = int(subscription['customer_id'][1:])
        state = rng.choice(('open', 'closed', 'win', 'lost'))
        end_age = rng.randrange(20, 260)
        opportunity(customer, product, state, age=rng.randrange(45, 160) if state == 'open' else end_age + 40, end_age=end_age)

    def ticket(customer, product=1, state='open', severity='high', days_ago=45, description=None):
        return add('support_tickets', ticket_id=f"TKT{len(data['support_tickets']) + 1:04}", customer_id=f'C{customer:03}', product_id=f'P{product:02}', opened_at=at(reference_date - timedelta(days=days_ago), 8), state=state, closed_at=at(reference_date - timedelta(days=days_ago - 3), 18) if state == 'close' else None, severity=severity, description=description or ERRORS[product - 1])

    def activity(parent, engineer, started, ended, outcome):
        return add('support_activities', activity_id=f"ACT{len(data['support_activities']) + 1:04}", ticket_id=parent['ticket_id'], technical_engineer_id=engineer, started_at=started, ended_at=ended, objective=f"Investigate {parent['description']}", outcome=outcome)
    for customer in (3, 5, 6, 7):
        row = ticket(customer, severity='medium' if customer % 2 else 'high')
        activity(row, 'TE01', row['opened_at'] + timedelta(hours=2), row['opened_at'] + timedelta(hours=3), 'Collected diagnostic traces; customer supplied reproduction details.')
    activity(row, 'TE01', reference - timedelta(hours=3), None, None)
    for customer, product, severity in ((8, 1, 'low'), (1, 2, 'high')):
        row = ticket(customer, product, severity=severity, days_ago=10)
        activity(row, f'TE{product:02}', row['opened_at'] + timedelta(hours=1), row['opened_at'] + timedelta(hours=2), 'Reviewed the report and requested customer confirmation.')
    for subscription in data['subscriptions']:
        customer = int(subscription['customer_id'][1:])
        product = int(subscription['product_id'][1:])
        max_age = (reference_date - subscription['start_date']).days - 2
        row = ticket(customer, product, state='close', severity=rng.choice(('low', 'medium', 'high')), days_ago=rng.randrange(20, max_age))
        for day in (0, 2):
            start = row['opened_at'] + timedelta(days=day, hours=2)
            activity(row, f'TE{product:02}', start, start + timedelta(hours=1), f'Resolved {ERRORS[product - 1]}; customer verified restored operation.' if day else 'Reproduced the reported issue and isolated the affected operation.')
    for engineer, customer in (('TE07', 18), ('TE08', 19)):
        row = ticket(customer, state='close', days_ago=60)
        start = row['opened_at'] + timedelta(hours=1)
        activity(row, engineer, start, start + timedelta(hours=2), 'Resolved WH-409 inventory reservation lock timeout by rebuilding the reservation index; customer verified restored operation.')
    complete_months = [shift_months(month_start, -offset) for offset in (3, 2, 1)]
    buckets = []
    declining_customers = {7, 11, 12, 13, 14, 15}
    for subscription in data['subscriptions']:
        customer = int(subscription['customer_id'][1:])
        product = int(subscription['product_id'][1:])
        cursor = subscription['start_date']
        while cursor < min(subscription['end_date'] + timedelta(days=1), reference_date):
            end = min(shift_months(cursor, 1), subscription['end_date'] + timedelta(days=1), reference_date)
            count = None
            if product == 1 and cursor in complete_months:
                index = complete_months.index(cursor)
                if customer in declining_customers:
                    count = (240, 150, 60)[index]
                elif customer in (8, 16):
                    count = (80, 140, 200)[index]
            weight = (end - cursor).days * rng.uniform(0.8, 1.2) * (1 + ((cursor.year - first_full_month.year) * 12 + cursor.month - first_full_month.month) * 0.06)
            buckets.append({'subscription': subscription, 'start': at(cursor), 'end': at(end), 'count': count, 'weight': weight})
            cursor = shift_months(cursor, 1)
    fixed = sum((bucket['count'] for bucket in buckets if bucket['count'] is not None))
    variable = [bucket for bucket in buckets if bucket['count'] is None]
    for bucket in variable:
        subscription = bucket['subscription']
        forced = []
        if subscription['product_id'] == 'P01' and subscription['customer_id'] in ('C001', 'C002'):
            for days_ago, duration, count in ((3, 3, 24), (6, 3, 12)):
                start = max(bucket['start'], reference - timedelta(days=days_ago))
                end = min(bucket['end'], reference - timedelta(days=days_ago - duration))
                if start < end:
                    forced.extend(((start, end) for _ in range(count)))
        bucket['forced_times'] = forced
        bucket['minimum'] = max(1, len(forced))
    minimum = sum((bucket['minimum'] for bucket in variable))
    budget = telemetry_events - fixed - minimum
    if budget < 0:
        raise ValueError(f'At least {fixed + minimum} telemetry events are required for scenario coverage')
    total_weight = sum((bucket['weight'] for bucket in variable))
    assigned = 0
    remainders = []
    for index, bucket in enumerate(variable):
        share = budget * bucket['weight'] / total_weight
        bucket['count'] = bucket['minimum'] + int(share)
        assigned += int(share)
        remainders.append((share - int(share), index))
    for _, index in sorted(remainders, reverse=True)[:budget - assigned]:
        variable[index]['count'] += 1
    for bucket in buckets:
        subscription = bucket['subscription']
        product = int(subscription['product_id'][1:])
        span = int((bucket['end'] - bucket['start']).total_seconds())
        for event_index in range(bucket['count']):
            forced = bucket.get('forced_times', [])
            if event_index < len(forced):
                start, end = forced[event_index]
                event_at = start + timedelta(seconds=rng.randrange(int((end - start).total_seconds())))
            else:
                event_at = bucket['start'] + timedelta(seconds=rng.randrange(span))
            failure_probability = 0.4 if product == 1 and event_at >= reference - timedelta(days=3) else 0.012
            failed = rng.random() < failure_probability
            if product == 1 and reference - timedelta(days=6) <= event_at < reference - timedelta(days=3):
                failed = False
            if event_index < len(forced) and event_at >= reference - timedelta(days=3):
                failed = True
            add('product_telemetry', event_id=f"EVT{len(data['product_telemetry']) + 1:07}", customer_id=subscription['customer_id'], product_id=subscription['product_id'], event_at=event_at, action=ACTIONS[product - 1][0] if failed else rng.choice(ACTIONS[product - 1]), state='failed' if failed else 'success', error_message=ERRORS[product - 1] if failed else None)
    data['product_telemetry'].sort(key=lambda row: (row['event_at'], row['event_id']))
    return data


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""Verify CSV integrity and derive business-scenario evidence from source facts."""
import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

def require(condition, message):
    if not condition:
        raise ValueError(message)

def negative_activity(milestone):
    if milestone is None:
        return False
    text = f"{milestone['objective']} {milestone['outcome']}".lower()
    return any((word in text for word in ('unhappy', 'blocked', 'rejected', 'budget freeze', 'unresolved')))

def opportunity_risks(data, reference_date):
    milestones = defaultdict(list)
    for row in data['sales_milestones']:
        milestones[row['opportunity_id']].append(row)
    support_pairs = {(row['customer_id'], row['product_id']) for row in data['support_tickets'] if row['state'] == 'open' and row['severity'] in ('medium', 'high')}
    results = []
    for opportunity in data['sales_opportunities']:
        if opportunity['state'] != 'open':
            continue
        latest = max(milestones[opportunity['opportunity_id']], key=lambda row: (row['activity_at'], row['milestone_id']), default=None)
        last_at = latest['activity_at'] if latest else at(opportunity['start_date'])
        conditions = []
        if at(reference_date) - last_at >= timedelta(days=15):
            conditions.append('sales inactivity')
        if negative_activity(latest):
            conditions.append('negative latest sales activity')
        if (opportunity['customer_id'], opportunity['product_id']) in support_pairs:
            conditions.append('matching open medium/high support')
        results.append({'opportunity_id': opportunity['opportunity_id'], 'customer_id': opportunity['customer_id'], 'product_id': opportunity['product_id'], 'risk_level': f'Level-{len(conditions)}' if conditions else 'None', 'conditions': conditions, 'latest_activity_at': latest['activity_at'].isoformat() if latest else None})
    return results

def sales_initiative(data, reference_date, risks):
    month = reference_date.replace(day=1)
    months = [shift_months(month, -offset) for offset in (3, 2, 1)]
    counts = Counter(((row['customer_id'], row['product_id'], row['event_at'].date().replace(day=1)) for row in data['product_telemetry'] if months[0] <= row['event_at'].date() < month))
    opportunities = defaultdict(list)
    for row in data['sales_opportunities']:
        opportunities[row['customer_id'], row['product_id']].append(row)
    risky = {row['opportunity_id'] for row in risks if row['conditions']}
    fees = defaultdict(Decimal)
    for row in data['subscriptions']:
        if row['start_date'] <= reference_date <= row['end_date']:
            fees[row['customer_id'], row['product_id']] += row['monthly_fee']
    results = []
    for pair, mrr in sorted(fees.items()):
        monthly_counts = [counts[*pair, value] for value in months]
        if not monthly_counts[0] > monthly_counts[1] > monthly_counts[2]:
            continue
        history = opportunities[pair]
        active = [row for row in history if row['state'] == 'open']
        reasons = []
        if not active:
            reasons.append('no open opportunity')
        if any((row['opportunity_id'] in risky for row in active)):
            reasons.append('open opportunity at risk')
        if any((row['state'] == 'lost' and shift_months(reference_date, -3) <= row['end_date'] < reference_date for row in history)):
            reasons.append('recent lost opportunity')
        if reasons:
            results.append({'customer_id': pair[0], 'product_id': pair[1], 'month_counts': dict(zip((value.isoformat() for value in months), monthly_counts)), 'subscription_mrr': format(mrr, '.2f'), 'currency': 'EUR', 'opportunity_evidence': reasons})
    customer_mrr = defaultdict(Decimal)
    for row in results:
        customer_mrr[row['customer_id']] += Decimal(row['subscription_mrr'])
    return {'customer_modules': results, 'customers': [{'customer_id': customer, 'eligible_module_mrr': format(mrr, '.2f'), 'currency': 'EUR'} for customer, mrr in sorted(customer_mrr.items())]}

def operational_evidence(data, reference_date):
    reference = at(reference_date)
    busy = {row['technical_engineer_id'] for row in data['support_activities'] if row['ended_at'] is None}
    certifications = {(row['employee_id'], row['product_id']): row['expiry_date'] for row in data['employee_certifications']}
    employees = {row['employee_id']: row for row in data['employees']}
    tickets = {row['ticket_id']: row for row in data['support_tickets']}
    recent = Counter()
    preceding = Counter()
    failures = defaultdict(list)
    for row in data['product_telemetry']:
        key = (row['product_id'], row['state'])
        if reference - timedelta(days=3) <= row['event_at'] < reference:
            recent[key] += 1
            if row['state'] == 'failed':
                failures[row['product_id'], row['error_message']].append(row)
        elif reference - timedelta(days=6) <= row['event_at'] < reference - timedelta(days=3):
            preceding[key] += 1
    results = []
    for (product, message), events in sorted(failures.items()):
        error_code_match = re.search('\\b[A-Z]{2}-\\d{3}\\b', message)
        if error_code_match is None:
            continue
        code = error_code_match.group()
        successes = defaultdict(list)
        for activity in data['support_activities']:
            parent = tickets[activity['ticket_id']]
            if parent['product_id'] == product and parent['state'] == 'close' and (code in parent['description']) and (activity['ended_at'] is not None) and re.search('\\bResolved\\b', activity['outcome'] or '', re.IGNORECASE):
                successes[activity['technical_engineer_id']].append(parent['ticket_id'])
        candidates = [{'employee_id': engineer, 'name': employees[engineer]['name'], 'similar_resolved_tickets': sorted(set(ticket_ids))} for engineer, ticket_ids in sorted(successes.items()) if engineer not in busy and employees[engineer]['role'] == 'TE' and (certifications.get((engineer, product), date.min) >= reference_date)]
        total = recent[product, 'success'] + recent[product, 'failed']
        previous_total = preceding[product, 'success'] + preceding[product, 'failed']
        results.append({'product_id': product, 'error_message': message, 'recent_failure_count': len(events), 'affected_customers': sorted({row['customer_id'] for row in events}), 'module_failure_rate_last_3_days': round(recent[product, 'failed'] / total, 4), 'module_failure_rate_preceding_3_days': round(preceding[product, 'failed'] / previous_total, 4) if previous_total else None, 'available_experienced_certified_engineers': candidates})
    return results

def validate(data, reference_date):
    require(set(data) == set(SCHEMAS), 'Dataset must contain exactly the twelve approved tables')
    reference = at(reference_date)
    history_start = shift_months(reference_date, -12)
    expected_types = {'string': str, 'date': date, 'timestamp': datetime, 'money': Decimal}
    for table, schema in SCHEMAS.items():
        require(bool(data[table]), f'{table} must not be empty')
        keys = []
        for row in data[table]:
            require(set(row) == {column.name for column in schema}, f'{table}: unexpected columns')
            for column in schema:
                value = row[column.name]
                if value is None:
                    require(column.nullable, f'{table}.{column.name} must not be null')
                    continue
                require(type(value) is expected_types[column.kind], f'{table}.{column.name}: invalid type')
                if column.kind == 'string':
                    require(bool(value) and value != '\\N', f'{table}.{column.name}: invalid empty/null-marker text')
                elif column.kind == 'money':
                    require(value.is_finite() and value > 0 and (value == value.quantize(Decimal('0.01'))), f'{table}.{column.name}: invalid monetary amount')
                elif column.kind == 'timestamp':
                    require(value.tzinfo == timezone.utc and at(history_start) <= value < reference, f'{table}.{column.name}: timestamp outside history')
            keys.append(tuple((row[column] for column in PRIMARY_KEYS[table])))
        require(len(keys) == len(set(keys)), f'{table}: duplicate primary key')
    for child, source, parent, target in FOREIGN_KEYS:
        parent_values = {row[target] for row in data[parent]}
        require(all((row[source] in parent_values for row in data[child])), f'{child}.{source}: broken reference to {parent}.{target}')
    employees = {row['employee_id']: row for row in data['employees']}
    certifications = {(row['employee_id'], row['product_id']): row['expiry_date'] for row in data['employee_certifications']}
    mapping = {row['customer_id']: row['account_executive_id'] for row in data['customer_account_executives']}
    require(len(mapping) == len(data['customers']), 'Every customer must have exactly one AE')
    require(all((employees[ae]['role'] == 'AE' for ae in mapping.values())), 'Customer AE role mismatch')
    require(all((row['role'] in ('AE', 'SE', 'TE') for row in employees.values())), 'Invalid employee role')
    require(all((employees[row['employee_id']]['role'] in ('SE', 'TE') for row in data['employee_certifications'])), 'Only SE/TE employees have module certifications in this model')

    def certified(engineer, product, role, activity_date):
        require(employees[engineer]['role'] == role, f'{engineer}: incorrect role')
        require(certifications.get((engineer, product), date.min) >= activity_date, f'{engineer}: certification missing or expired for {product} on {activity_date}')
    opportunities = {row['opportunity_id']: row for row in data['sales_opportunities']}
    for row in opportunities.values():
        require(row['state'] in ('open', 'closed', 'win', 'lost'), 'Invalid opportunity state')
        require(mapping[row['customer_id']] == row['account_executive_id'], 'Opportunity AE pairing mismatch')
        require(history_start <= row['start_date'] < reference_date, 'Opportunity start outside history')
        require(row['start_date'] <= row['end_date'], 'Opportunity ends before start')
        require(row['end_date'] >= reference_date if row['state'] == 'open' else row['end_date'] < reference_date, 'Opportunity end inconsistent with state')
        require(row['currency'] == 'EUR', 'Opportunity currency must be EUR')
        if row['state'] == 'open':
            require(reference_date + timedelta(days=10) <= row['end_date'] <= reference_date + timedelta(days=120), 'Expected close must be 10-120 days after the reference date')
        else:
            require(shift_months(reference_date, -3) <= row['end_date'] < reference_date, 'Actual close must be within the preceding three calendar months')
    future_offsets = [(row['end_date'] - reference_date).days for row in opportunities.values() if row['state'] == 'open']
    require(all((any((10 + band * 111 // 4 <= offset < 10 + (band + 1) * 111 // 4 for offset in future_offsets)) for band in range(4))), 'Expected closes must span all four parts of the 10-120 day horizon')
    actual_dates = [row['end_date'] for row in opportunities.values() if row['state'] != 'open']
    require(all((any((shift_months(reference_date, band - 3) <= value < shift_months(reference_date, band - 2) for value in actual_dates)) for band in range(3))), 'Actual closes must span all three preceding calendar-month intervals')
    for row in data['sales_milestones']:
        parent = opportunities[row['opportunity_id']]
        require(parent['start_date'] <= row['activity_at'].date() <= min(parent['end_date'], reference_date), 'Milestone outside opportunity timeline')
        certified(row['sales_engineer_id'], parent['product_id'], 'SE', row['activity_at'].date())
    tickets = {row['ticket_id']: row for row in data['support_tickets']}
    activities = defaultdict(list)
    for row in data['support_activities']:
        activities[row['ticket_id']].append(row)
        parent = tickets[row['ticket_id']]
        require(row['started_at'] >= parent['opened_at'], 'Activity starts before ticket')
        certified(row['technical_engineer_id'], parent['product_id'], 'TE', row['started_at'].date())
        if row['ended_at'] is None:
            require(row['outcome'] is None, 'Ongoing activity must not have a completed outcome')
            require(parent['state'] == 'open', 'Closed ticket has ongoing activity')
            certified(row['technical_engineer_id'], parent['product_id'], 'TE', reference_date)
        else:
            require(row['ended_at'] >= row['started_at'], 'Activity ends before start')
            require(row['outcome'] is not None, 'Completed activity requires an outcome')
            certified(row['technical_engineer_id'], parent['product_id'], 'TE', row['ended_at'].date())
            if parent['closed_at']:
                require(row['ended_at'] <= parent['closed_at'], 'Activity ends after ticket close')
    busy_counts = Counter((row['technical_engineer_id'] for row in data['support_activities'] if row['ended_at'] is None))
    require(all((count == 1 for count in busy_counts.values())), 'Engineer has concurrent ongoing activities')
    for row in tickets.values():
        require(row['state'] in ('open', 'close'), 'Invalid ticket state')
        require(row['severity'] in ('low', 'medium', 'high'), 'Invalid ticket severity')
        require(bool(activities[row['ticket_id']]), 'Every ticket requires support activities')
        if row['state'] == 'open':
            require(row['closed_at'] is None, 'Open ticket has close timestamp')
        else:
            require(row['closed_at'] is not None and row['closed_at'] >= row['opened_at'], 'Invalid ticket close timestamp')
    subscriptions = {row['subscription_id']: row for row in data['subscriptions']}
    subscription_pairs = defaultdict(list)
    for row in subscriptions.values():
        require(history_start <= row['start_date'] < reference_date, 'Subscription start outside history')
        require(row['end_date'] >= row['start_date'], 'Subscription ends before start')
        require(row['currency'] == 'EUR', 'Subscription currency must be EUR')
        subscription_pairs[row['customer_id'], row['product_id']].append(row)
    periods = set()
    for row in data['subscription_payments']:
        parent = subscriptions[row['subscription_id']]
        period = (row['subscription_id'], row['billing_month'])
        require(period not in periods, 'Duplicate subscription billing month')
        periods.add(period)
        require(row['billing_month'].day == 1, 'Billing month must be first day of month')
        require(parent['start_date'] <= row['billing_month'] <= parent['end_date'], 'Payment outside subscription')
        require(row['billing_month'] <= row['paid_on'] < shift_months(row['billing_month'], 1) and row['paid_on'] < reference_date, 'Payment outside billing month or in future')
        require(row['amount'] == parent['monthly_fee'] and row['currency'] == parent['currency'], 'Payment fee/currency mismatch')
    for row in data['product_telemetry']:
        require(row['state'] in ('success', 'failed'), 'Invalid telemetry state')
        require((row['error_message'] is not None) == (row['state'] == 'failed'), 'Telemetry error message inconsistent with state')
        require(any((parent['start_date'] <= row['event_at'].date() <= parent['end_date'] for parent in subscription_pairs[row['customer_id'], row['product_id']])), 'Telemetry outside a subscribed period')
    risks = opportunity_risks(data, reference_date)
    combinations = {tuple(row['conditions']) for row in risks}
    require(len(combinations) == 8, 'Missing one or more of the eight opportunity risk combinations')
    initiative = sales_initiative(data, reference_date, risks)
    reasons = {reason for row in initiative['customer_modules'] for reason in row['opportunity_evidence']}
    require({'no open opportunity', 'open opportunity at risk', 'recent lost opportunity'} <= reasons, 'Sales initiative does not cover every opportunity branch')
    operations = operational_evidence(data, reference_date)
    require(any((row['recent_failure_count'] >= 10 and len(row['affected_customers']) >= 2 and (row['module_failure_rate_last_3_days'] > 0.15) and (row['module_failure_rate_preceding_3_days'] is not None) and (row['module_failure_rate_last_3_days'] > row['module_failure_rate_preceding_3_days']) and row['available_experienced_certified_engineers'] for row in operations)), 'Missing a multi-customer telemetry incident with an eligible TE')
    return {'reference_date': reference_date.isoformat(), 'row_counts': {name: len(rows) for name, rows in data.items()}, 'risk_combination_count': len(combinations), 'opportunity_risks': risks, 'sales_initiative': initiative, 'operational_excellence': operations}


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


import csv
import tempfile
from pathlib import Path


def upload_verified_file(spark, notebookutils, local_path, target_path):
    payload = local_path.read_bytes()
    if notebookutils.fs.cp(local_path.resolve().as_uri(), target_path) is False:
        raise IOError(f"Could not upload {target_path}")
    if hashlib.sha256(file_bytes(spark, target_path)).digest() != hashlib.sha256(payload).digest():
        raise IOError(f"Uploaded file verification failed: {target_path}")
    return hashlib.sha256(payload).hexdigest()


def generate_snapshot(spark, notebookutils, base_path, reference_date="",
                      seed=42, telemetry_events=100_000, run_id=""):
    base_path = files_root(base_path)
    identity = lakehouse_context(notebookutils)
    if not isinstance(seed, int) or isinstance(seed, bool):
        raise ValueError("Seed must be an integer")
    if not isinstance(telemetry_events, int) or isinstance(telemetry_events, bool):
        raise ValueError("TelemetryEvents must be an integer")
    reference = date.fromisoformat(reference_date) if reference_date else datetime.now(timezone.utc).date()
    if not run_id:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("RunId must contain only letters, digits, underscores or hyphens")
    root = f"{base_path}/runs/{run_id}"
    if notebookutils.fs.exists(root):
        raise FileExistsError(f"Run already exists and will not be overwritten: {root}")
    data = snapshot(reference, seed, telemetry_events)
    report = validate(data, reference)
    if notebookutils.fs.mkdirs(f"{root}/csv") is False:
        raise IOError(f"Could not create {root}/csv")
    manifest = {
        "format_version": 2, "run_id": run_id, "source_root": root,
        **identity, "reference_date": reference.isoformat(),
        "reference_at_utc": encode(at(reference)),
        "history_start": shift_months(reference, -12).isoformat(),
        "seed": seed, "null_marker": NULL, "tables": {},
    }
    LOG.warning("Writing immutable CSV snapshot: %s", root)
    with tempfile.TemporaryDirectory(prefix="theta360_v2_") as temporary:
        temporary = Path(temporary)
        for table, schema in SCHEMAS.items():
            path = temporary / f"{table}.csv"
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle, lineterminator="\n")
                writer.writerow(column.name for column in schema)
                for row in data[table]:
                    writer.writerow(encode(row[column.name]) for column in schema)
            sha256 = upload_verified_file(spark, notebookutils, path, f"{root}/csv/{table}.csv")
            manifest["tables"][table] = {
                "row_count": len(data[table]), "sha256": sha256,
                "columns": [
                    {"name": column.name, "type": column.kind, "nullable": column.nullable}
                    for column in schema
                ],
            }
        metadata_hashes = {}
        for filename, content in (
            ("validation_report.json", report), ("manifest.json", manifest),
        ):
            path = temporary / filename
            path.write_text(json.dumps(content, indent=2) + "\n", encoding="utf-8")
            metadata_hashes[filename] = upload_verified_file(
                spark, notebookutils, path, f"{root}/{filename}"
            )
    pointer = {
        "format_version": 2, "source_root": root,
        "reference_date": reference.isoformat(), **identity,
        "manifest_sha256": metadata_hashes["manifest.json"],
    }
    pointer_path = f"{base_path}/latest.json"
    if notebookutils.fs.put(pointer_path, json.dumps(pointer, indent=2) + "\n", True) is False:
        raise IOError(f"Snapshot is complete but latest pointer could not be updated: {pointer_path}")
    if json.loads(notebookutils.fs.head(pointer_path, 65_536)) != pointer:
        raise IOError("Latest pointer readback failed; use the logged snapshot path explicitly")
    print(json.dumps({
        "source_root": root, "reference_date": manifest["reference_date"],
        "row_counts": {table: item["row_count"] for table, item in manifest["tables"].items()},
        "next_step": "Run notebook 02 to refresh Delta and Kusto tables",
    }, indent=2))
    return manifest


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils

generated_manifest = generate_snapshot(
    spark, notebookutils, BasePath, ReferenceDate, Seed, TelemetryEvents, RunId
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Next: load the generated snapshot
# 
# After the generation cell succeeds, note the printed snapshot path and open
# **02_Refresh_Theta360_v2**. Keep the same default Lakehouse and `BasePath`.
# Configure your KQL database's query URI and database name there.
# Run with `DryRun = True` first, review the plan, then follow that notebook's
# instructions to create or refresh the tables.
# 
# To generate current data again later, leave `ReferenceDate = ""` and
# `RunId = ""` in this notebook's parameters cell and select **Run all**.
# Each run creates a new snapshot; it does not refresh the destination tables.
# Run notebook **02** afterward. Previous CSV snapshots and v1 are preserved.
# 
# These are import-ready notebooks, not an already executed Fabric deployment.

