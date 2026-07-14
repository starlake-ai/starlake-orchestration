#
# Copyright © 2025 Starlake AI (https://starlake.ai)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Canonical cross-orchestrator scenario contract (NFR1).

Single source of truth for what the sample-project scenario must produce,
regardless of orchestrator. Every orchestrator leg asserts against THESE
constants; equivalence between orchestrators is transitive through them.
NFR13: no orchestrator imports allowed in this module.
"""

from __future__ import annotations

from typing import List, Tuple

# --- structural contract -------------------------------------------------

EXPECTED_TASK_IDS = {
    "load_customers": "load_starbake_customers",
    "load_orders": "load_starbake_orders",
    "load_products": "load_starbake_products",
    "pre_load_imported": "check_starbake_incoming_files",
    "import": "import_starbake",
    # NOTE: Dagster op names forbid dots — the shared convention (established
    # in tests/shared/base_test_sl_transform.py) is an underscored task_id
    # with the dotted transform_name passed separately.
    "transform_order_summary": "kpi_order_summary",
    "transform_top_customers": "kpi_top_customers",
}

# action verb (args[0]) + flag/value pairs that MUST be present, per action.
CLI_CONTRACT = {
    "load": {"verb": "load", "flags": {"--domains": "starbake", "--tables": "customers"}},
    "transform": {"verb": "transform", "flags": {"--name": "kpi.order_summary"}},
    "import": {"verb": "stage", "flags": {"--domains": "starbake"}},
    "pre_load": {"verb": "preload", "flags": {"--domain": "starbake", "--strategy": "imported"}},
}

# --- runtime data contract ----------------------------------------------

EXPECTED_ROW_COUNTS = {
    "starbake.customers": 7,
    "starbake.orders": 10,
    "starbake.products": 5,
}

EXPECTED_KPI_ROW_COUNTS = {
    "kpi.order_summary": 7,
    "kpi.top_customers": 5,
}

TOP_CUSTOMERS_MAX_ROWS = 5

# Business columns only (Starlake may add audit columns depending on config);
# ORDER BY the natural key so snapshots are deterministic.
SNAPSHOT_QUERIES = {
    "starbake.customers": (
        "SELECT id, first_name, last_name, email, CAST(join_date AS VARCHAR) "
        "FROM starbake.customers ORDER BY id"
    ),
    "starbake.orders": (
        "SELECT order_id, customer_id, product_id, quantity, CAST(order_date AS VARCHAR) "
        "FROM starbake.orders ORDER BY order_id"
    ),
    "starbake.products": (
        "SELECT product_id, name, category, ROUND(price, 2), ROUND(cost, 2) "
        "FROM starbake.products ORDER BY product_id"
    ),
    "kpi.order_summary": (
        "SELECT customer_id, customer_name, email, total_orders, total_items, "
        "ROUND(total_spent, 2), CAST(first_order_date AS VARCHAR), "
        "CAST(last_order_date AS VARCHAR) "
        "FROM kpi.order_summary ORDER BY customer_id"
    ),
    "kpi.top_customers": (
        "SELECT customer_id, customer_name, email, total_orders, total_items, "
        "ROUND(total_spent, 2) "
        "FROM kpi.top_customers ORDER BY customer_id"
    ),
}


def table_snapshot(conn, table: str) -> List[Tuple]:
    """Deterministic, normalized snapshot of *table* via its SNAPSHOT_QUERY."""
    return [tuple(row) for row in conn.execute(SNAPSHOT_QUERIES[table]).fetchall()]


# Literal expected rows — generated once from a reference run of the sample
# project (Starlake CLI load + transforms against DuckDB, dumped through
# SNAPSHOT_QUERIES), reviewed against the sample-project CSVs and committed.
# Both the Airflow and the Dagster runtime suites must reproduce these EXACT
# rows.
EXPECTED_TABLE_SNAPSHOTS = {
    "starbake.customers": [
        # (id, first_name, last_name, email, join_date)
        (1, "John", "Doe", "john.doe@email.com", "2023-01-15"),
        (2, "Jane", "Smith", "jane.smith@email.com", "2023-02-03"),
        (3, "Michael", "Johnson", "michael.johnson@email.com", "2023-02-28"),
        (4, "Emily", "Brown", "emily.brown@email.com", "2023-03-10"),
        (5, "David", "Wilson", "david.wilson@email.com", "2023-04-05"),
        (6, "Sarah", "Taylor", "sarah.taylor@email.com", "2023-04-22"),
        (7, "Robert", "Anderson", "robert.anderson@email.com", "2023-05-18"),
    ],
    "starbake.orders": [
        # (order_id, customer_id, product_id, quantity, order_date)
        (1, 1, 1, 2, "2023-01-20"),
        (2, 1, 3, 1, "2023-01-25"),
        (3, 2, 2, 3, "2023-02-10"),
        (4, 3, 1, 1, "2023-03-15"),
        (5, 3, 4, 2, "2023-03-15"),
        (6, 4, 2, 2, "2023-03-25"),
        (7, 5, 3, 1, "2023-04-08"),
        (8, 5, 5, 3, "2023-04-08"),
        (9, 6, 4, 2, "2023-04-30"),
        (10, 7, 5, 1, "2023-05-20"),
    ],
    "starbake.products": [
        # (product_id, name, category, price, cost)
        (1, "Baguette", "bread", 14.33, 8.12),
        (2, "Chocolate Croissant", "pastry", 3.99, 1.75),
        (3, "Sourdough Loaf", "bread", 6.5, 3.25),
        (4, "Blueberry Muffin", "pastry", 2.75, 1.2),
        (5, "Cinnamon Roll", "pastry", 3.5, 1.8),
    ],
}

EXPECTED_KPI_SNAPSHOTS = {
    "kpi.order_summary": [
        # (customer_id, customer_name, email, total_orders, total_items,
        #  total_spent, first_order_date, last_order_date)
        (1, "John Doe", "john.doe@email.com", 2, 3, 35.16, "2023-01-20", "2023-01-25"),
        (2, "Jane Smith", "jane.smith@email.com", 1, 3, 11.97, "2023-02-10", "2023-02-10"),
        (3, "Michael Johnson", "michael.johnson@email.com", 2, 3, 19.83, "2023-03-15", "2023-03-15"),
        (4, "Emily Brown", "emily.brown@email.com", 1, 2, 7.98, "2023-03-25", "2023-03-25"),
        (5, "David Wilson", "david.wilson@email.com", 2, 4, 17.0, "2023-04-08", "2023-04-08"),
        (6, "Sarah Taylor", "sarah.taylor@email.com", 1, 2, 5.5, "2023-04-30", "2023-04-30"),
        (7, "Robert Anderson", "robert.anderson@email.com", 1, 1, 3.5, "2023-05-20", "2023-05-20"),
    ],
    "kpi.top_customers": [
        # Top 5 by total_spent (no tie at the cut boundary — deterministic):
        # 35.16 > 19.83 > 17.0 > 11.97 > 7.98 | excluded: 5.5, 3.5
        (1, "John Doe", "john.doe@email.com", 2, 3, 35.16),
        (2, "Jane Smith", "jane.smith@email.com", 1, 3, 11.97),
        (3, "Michael Johnson", "michael.johnson@email.com", 2, 3, 19.83),
        (4, "Emily Brown", "emily.brown@email.com", 1, 2, 7.98),
        (5, "David Wilson", "david.wilson@email.com", 2, 4, 17.0),
    ],
}

# Guard against drift between counts and literals (fails at import time).
for _t, _rows in EXPECTED_TABLE_SNAPSHOTS.items():
    assert len(_rows) == EXPECTED_ROW_COUNTS[_t], (
        f"expected_results drift: {_t} snapshot has {len(_rows)} rows, "
        f"EXPECTED_ROW_COUNTS says {EXPECTED_ROW_COUNTS[_t]}"
    )
for _t, _rows in EXPECTED_KPI_SNAPSHOTS.items():
    assert len(_rows) == EXPECTED_KPI_ROW_COUNTS[_t], (
        f"expected_results drift: {_t} snapshot has {len(_rows)} rows, "
        f"EXPECTED_KPI_ROW_COUNTS says {EXPECTED_KPI_ROW_COUNTS[_t]}"
    )
assert len(EXPECTED_KPI_SNAPSHOTS["kpi.top_customers"]) <= TOP_CUSTOMERS_MAX_ROWS

del _t, _rows  # keep the module namespace clean (loop guard variables)
