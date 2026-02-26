# Databricks notebook source

# MAGIC %md
# MAGIC # ✨ 8. (BONUS) AI/BI Dashboard & Genie Space via API
# MAGIC **Panda Restaurant Group — Synapse to Databricks Evaluation**
# MAGIC
# MAGIC This notebook demonstrates how to **programmatically create** both an AI/BI Dashboard and a Genie Space using the Databricks REST API. This showcases the platform's extensibility — everything you can do in the UI, you can also automate via API.
# MAGIC
# MAGIC **What we'll build:**
# MAGIC 1. 📊 **AI/BI Lakeview Dashboard** — Revenue by region, store performance KPIs, menu analysis
# MAGIC 2. 🤖 **Genie Space** — Natural language SQL exploration of Panda's operational data
# MAGIC
# MAGIC **Prerequisites:** Requires `01_data_management_quality` to have been run (creates the `v_daily_store_revenue` view).

# COMMAND ----------

# MAGIC %run ./00_setup_data_generation

# COMMAND ----------

# Initialize Databricks SDK and find a SQL warehouse
from databricks.sdk import WorkspaceClient
import json, uuid, traceback

w = WorkspaceClient()

# Auto-detect a SQL warehouse (prefer running, fall back to any)
try:
    warehouse_list = list(w.warehouses.list())
    warehouse_id = None
    for wh in warehouse_list:
        if "RUNNING" in str(wh.state):
            warehouse_id = wh.id
            print(f"Found running warehouse: {wh.name} ({wh.id})")
            break
    if not warehouse_id and warehouse_list:
        warehouse_id = warehouse_list[0].id
        print(f"Using first available warehouse: {warehouse_list[0].name} ({warehouse_id})")
    if not warehouse_id:
        print("WARNING: No SQL warehouse found. Dashboard and Genie creation will be skipped.")
except Exception as e:
    warehouse_id = None
    print(f"WARNING: Could not list warehouses: {e}")
    traceback.print_exc()

current_user = spark.sql("SELECT current_user()").first()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📊 Part A: Create AI/BI Lakeview Dashboard via API
# MAGIC
# MAGIC We'll build a dashboard with:
# MAGIC - **3 KPI counters**: Total Revenue, Total Stores, Average Customer Rating
# MAGIC - **Bar chart**: Revenue by Region
# MAGIC - **Pie chart**: Revenue by Menu Category
# MAGIC - **Table**: Top 10 Stores by Revenue

# COMMAND ----------

# Build the dashboard JSON spec
dashboard_json = {
    "datasets": [
        {
            "name": "summary_kpis",
            "displayName": "Summary KPIs",
            "queryLines": [
                f"SELECT ",
                f"  ROUND(SUM(total_revenue), 2) AS total_revenue, ",
                f"  COUNT(DISTINCT store_id) AS total_stores, ",
                f"  ROUND(AVG(total_revenue), 2) AS avg_daily_revenue ",
                f"FROM {catalog}.{schema}.v_daily_store_revenue ",
                f"WHERE sale_date >= '2024-01-01' "
            ]
        },
        {
            "name": "avg_rating",
            "displayName": "Average Rating",
            "queryLines": [
                f"SELECT ROUND(AVG(rating), 1) AS avg_rating ",
                f"FROM {catalog}.{schema}.customer_feedback "
            ]
        },
        {
            "name": "revenue_by_region",
            "displayName": "Revenue by Region",
            "queryLines": [
                f"SELECT region, ROUND(SUM(total_revenue), 2) AS revenue ",
                f"FROM {catalog}.{schema}.v_daily_store_revenue ",
                f"WHERE sale_date >= '2024-01-01' ",
                f"GROUP BY region ",
                f"ORDER BY revenue DESC "
            ]
        },
        {
            "name": "revenue_by_category",
            "displayName": "Revenue by Menu Category",
            "queryLines": [
                f"SELECT m.category, ",
                f"  ROUND(SUM(s.quantity * m.unit_price), 2) AS revenue ",
                f"FROM {catalog}.{schema}.daily_sales s ",
                f"JOIN {catalog}.{schema}.menu_items m ON s.item_id = m.item_id ",
                f"WHERE s.sale_date >= '2024-01-01' ",
                f"GROUP BY m.category ",
                f"ORDER BY revenue DESC "
            ]
        },
        {
            "name": "top_stores",
            "displayName": "Top Stores by Revenue",
            "queryLines": [
                f"SELECT store_name, region, state, store_type, ",
                f"  ROUND(SUM(total_revenue), 2) AS revenue, ",
                f"  ROUND(SUM(total_profit), 2) AS profit ",
                f"FROM {catalog}.{schema}.v_daily_store_revenue ",
                f"WHERE sale_date >= '2024-01-01' ",
                f"GROUP BY store_name, region, state, store_type ",
                f"ORDER BY revenue DESC ",
                f"LIMIT 10 "
            ]
        }
    ],
    "pages": [
        {
            "name": "overview",
            "displayName": "Panda Restaurant Operations",
            "pageType": "PAGE_TYPE_CANVAS",
            "layout": [
                {
                    "widget": {
                        "name": "title",
                        "multilineTextboxSpec": {
                            "lines": ["## Panda Restaurant Operations Dashboard"]
                        }
                    },
                    "position": {"x": 0, "y": 0, "width": 6, "height": 1}
                },
                {
                    "widget": {
                        "name": "subtitle",
                        "multilineTextboxSpec": {
                            "lines": ["2024 performance metrics across all regions and store types"]
                        }
                    },
                    "position": {"x": 0, "y": 1, "width": 6, "height": 1}
                },
                {
                    "widget": {
                        "name": "kpi-total-revenue",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "summary_kpis",
                                "fields": [{"name": "total_revenue", "expression": "`total_revenue`"}],
                                "disaggregated": True
                            }
                        }],
                        "spec": {
                            "version": 2,
                            "widgetType": "counter",
                            "encodings": {
                                "value": {"fieldName": "total_revenue", "displayName": "Total Revenue ($)"}
                            },
                            "frame": {"showTitle": True, "title": "Total Revenue (2024)"}
                        }
                    },
                    "position": {"x": 0, "y": 2, "width": 2, "height": 3}
                },
                {
                    "widget": {
                        "name": "kpi-total-stores",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "summary_kpis",
                                "fields": [{"name": "total_stores", "expression": "`total_stores`"}],
                                "disaggregated": True
                            }
                        }],
                        "spec": {
                            "version": 2,
                            "widgetType": "counter",
                            "encodings": {
                                "value": {"fieldName": "total_stores", "displayName": "Stores"}
                            },
                            "frame": {"showTitle": True, "title": "Total Stores"}
                        }
                    },
                    "position": {"x": 2, "y": 2, "width": 2, "height": 3}
                },
                {
                    "widget": {
                        "name": "kpi-avg-rating",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "avg_rating",
                                "fields": [{"name": "avg_rating", "expression": "`avg_rating`"}],
                                "disaggregated": True
                            }
                        }],
                        "spec": {
                            "version": 2,
                            "widgetType": "counter",
                            "encodings": {
                                "value": {"fieldName": "avg_rating", "displayName": "Avg Rating"}
                            },
                            "frame": {"showTitle": True, "title": "Avg Customer Rating"}
                        }
                    },
                    "position": {"x": 4, "y": 2, "width": 2, "height": 3}
                },
                {
                    "widget": {
                        "name": "section-charts",
                        "multilineTextboxSpec": {
                            "lines": ["### Revenue Breakdown"]
                        }
                    },
                    "position": {"x": 0, "y": 5, "width": 6, "height": 1}
                },
                {
                    "widget": {
                        "name": "chart-revenue-by-region",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "revenue_by_region",
                                "fields": [
                                    {"name": "region", "expression": "`region`"},
                                    {"name": "revenue", "expression": "`revenue`"}
                                ],
                                "disaggregated": True
                            }
                        }],
                        "spec": {
                            "version": 3,
                            "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "region", "scale": {"type": "categorical"}, "displayName": "Region"},
                                "y": {"fieldName": "revenue", "scale": {"type": "quantitative"}, "displayName": "Revenue ($)"}
                            },
                            "frame": {"showTitle": True, "title": "Revenue by Region"}
                        }
                    },
                    "position": {"x": 0, "y": 6, "width": 3, "height": 6}
                },
                {
                    "widget": {
                        "name": "chart-revenue-by-category",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "revenue_by_category",
                                "fields": [
                                    {"name": "category", "expression": "`category`"},
                                    {"name": "revenue", "expression": "`revenue`"}
                                ],
                                "disaggregated": True
                            }
                        }],
                        "spec": {
                            "version": 3,
                            "widgetType": "pie",
                            "encodings": {
                                "angle": {"fieldName": "revenue", "displayName": "Revenue ($)"},
                                "color": {"fieldName": "category", "scale": {"type": "categorical"}, "displayName": "Category"}
                            },
                            "frame": {"showTitle": True, "title": "Revenue by Menu Category"}
                        }
                    },
                    "position": {"x": 3, "y": 6, "width": 3, "height": 6}
                },
                {
                    "widget": {
                        "name": "section-details",
                        "multilineTextboxSpec": {
                            "lines": ["### Top Performing Stores"]
                        }
                    },
                    "position": {"x": 0, "y": 12, "width": 6, "height": 1}
                },
                {
                    "widget": {
                        "name": "table-top-stores",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "top_stores",
                                "fields": [
                                    {"name": "store_name", "expression": "`store_name`"},
                                    {"name": "region", "expression": "`region`"},
                                    {"name": "state", "expression": "`state`"},
                                    {"name": "store_type", "expression": "`store_type`"},
                                    {"name": "revenue", "expression": "`revenue`"},
                                    {"name": "profit", "expression": "`profit`"}
                                ],
                                "disaggregated": True
                            }
                        }],
                        "spec": {
                            "version": 2,
                            "widgetType": "table",
                            "encodings": {
                                "columns": [
                                    {"fieldName": "store_name", "displayName": "Store"},
                                    {"fieldName": "region", "displayName": "Region"},
                                    {"fieldName": "state", "displayName": "State"},
                                    {"fieldName": "store_type", "displayName": "Type"},
                                    {"fieldName": "revenue", "displayName": "Revenue ($)"},
                                    {"fieldName": "profit", "displayName": "Profit ($)"}
                                ]
                            },
                            "frame": {"showTitle": True, "title": "Top 10 Stores by Revenue"}
                        }
                    },
                    "position": {"x": 0, "y": 13, "width": 6, "height": 6}
                }
            ]
        }
    ]
}

print("Dashboard JSON built successfully")
print(f"  Datasets: {len(dashboard_json['datasets'])}")
print(f"  Widgets: {len(dashboard_json['pages'][0]['layout'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚀 Deploy the Dashboard via REST API

# COMMAND ----------

# Create the dashboard via Lakeview API
dashboard_id = None
if warehouse_id:
    try:
        response = w.api_client.do(
            "POST",
            "/api/2.0/lakeview/dashboards",
            body={
                "display_name": f"Panda Restaurant Operations ({catalog}.{schema})",
                "serialized_dashboard": json.dumps(dashboard_json),
                "warehouse_id": warehouse_id,
                "parent_path": f"/Workspace/Users/{current_user}"
            }
        )
        dashboard_id = response.get("dashboard_id")
        dashboard_path = response.get("path", "")
        print(f"Dashboard created successfully!")
        print(f"  Dashboard ID: {dashboard_id}")
        print(f"  Path: {dashboard_path}")
    except Exception as e:
        print(f"ERROR creating dashboard: {e}")
        traceback.print_exc()
else:
    print("Skipped dashboard creation (no warehouse_id)")

# COMMAND ----------

# Publish the dashboard so it's accessible to viewers
if dashboard_id:
    try:
        pub_response = w.api_client.do(
            "POST",
            f"/api/2.0/lakeview/dashboards/{dashboard_id}/published",
            body={
                "embed_credentials": True,
                "warehouse_id": warehouse_id
            }
        )
        print(f"Dashboard published successfully!")
    except Exception as e:
        print(f"Note: Could not publish dashboard (may need additional permissions): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🤖 Part B: Create Genie Space via API
# MAGIC
# MAGIC Genie Spaces allow users to ask **natural language questions** about structured data. The system translates questions into SQL, executes them, and presents results conversationally.
# MAGIC
# MAGIC > 🆚 **Contrast with Synapse:** Synapse has no equivalent natural language query interface. Users must write SQL manually or use external tools.

# COMMAND ----------

def gen_id():
    """Generate 32-char lowercase hex ID (required by Genie API)."""
    return uuid.uuid4().hex

space_title = f"Panda Restaurant Analytics - {catalog}"
space_description = "Natural language analytics for Panda Restaurant Group operations and sales data."

# Tables with descriptions
tables = sorted([
    {"identifier": f"{catalog}.{schema}.stores",
     "description": ["~200 store locations across 5 US regions with type, state, and open date"]},
    {"identifier": f"{catalog}.{schema}.menu_items",
     "description": ["29 menu items with unit cost, unit price, and category"]},
    {"identifier": f"{catalog}.{schema}.daily_sales",
     "description": ["~500K daily sales transactions over 2 years with store_id, item_id, quantity"]},
    {"identifier": f"{catalog}.{schema}.customer_feedback",
     "description": ["10K customer reviews with ratings, review text, and customer email"]},
    {"identifier": f"{catalog}.{schema}.v_daily_store_revenue",
     "description": ["Pre-aggregated view: daily revenue and profit by store with region and type"]}
], key=lambda x: x["identifier"])

# Sample questions
sample_questions = sorted([
    {"id": gen_id(), "question": ["What were total sales by region in 2024?"]},
    {"id": gen_id(), "question": ["Which menu items generate the most profit?"]},
    {"id": gen_id(), "question": ["What is the average customer rating by store type?"]},
    {"id": gen_id(), "question": ["Show me the top 10 stores by revenue"]},
    {"id": gen_id(), "question": ["How does weekend sales volume compare to weekdays?"]},
    {"id": gen_id(), "question": ["Which regions have the most negative customer feedback?"]}
], key=lambda x: x["id"])

# Example question SQLs to guide Genie's query generation
example_question_sqls = sorted([
    {"id": gen_id(),
     "question": ["What were total sales by region in 2024?"],
     "sql": [f"SELECT region, ROUND(SUM(total_revenue), 2) AS total_revenue FROM {catalog}.{schema}.v_daily_store_revenue WHERE sale_date >= '2024-01-01' GROUP BY region ORDER BY total_revenue DESC"]},
    {"id": gen_id(),
     "question": ["Which menu items generate the most profit?"],
     "sql": [f"SELECT m.item_name, m.category, ROUND(SUM(s.quantity * (m.unit_price - m.unit_cost)), 2) AS total_profit FROM {catalog}.{schema}.daily_sales s JOIN {catalog}.{schema}.menu_items m ON s.item_id = m.item_id GROUP BY m.item_name, m.category ORDER BY total_profit DESC LIMIT 10"]},
    {"id": gen_id(),
     "question": ["Show me the top 10 stores by revenue"],
     "sql": [f"SELECT store_name, region, state, ROUND(SUM(total_revenue), 2) AS revenue FROM {catalog}.{schema}.v_daily_store_revenue GROUP BY store_name, region, state ORDER BY revenue DESC LIMIT 10"]}
], key=lambda x: x["id"])

# Text instructions for Genie
text_instructions = sorted([
    {"id": gen_id(), "content": [
        "v_daily_store_revenue is the pre-aggregated view - use it for store/region level queries. "
        "daily_sales has raw transactions - join with menu_items on item_id and stores on store_id. "
        "Revenue = quantity * unit_price. Profit = quantity * (unit_price - unit_cost). "
        "Regions: West, Southwest, Midwest, Southeast, Northeast. "
        "Store types: Dine-In, Express, Mall, Drive-Thru. "
        "IMPORTANT: Always use LIMIT 10 for top-N queries. Use ROUND for monetary values."
    ]}
], key=lambda x: x["id"])

serialized_space = {
    "version": 2,
    "config": {"sample_questions": sample_questions},
    "data_sources": {"tables": tables},
    "instructions": {
        "text_instructions": text_instructions,
        "example_question_sqls": example_question_sqls
    }
}

print("Genie Space configuration built successfully")
print(f"  Tables: {len(tables)}")
print(f"  Sample questions: {len(sample_questions)}")
print(f"  Example SQLs: {len(example_question_sqls)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚀 Deploy the Genie Space via REST API

# COMMAND ----------

# Create or update the Genie Space
genie_space_id = None
if warehouse_id:
    try:
        # Check for existing space with same title
        existing_id = None
        try:
            resp = w.api_client.do("GET", "/api/2.0/genie/spaces")
            for space in resp.get("spaces", []):
                if space.get("title") == space_title:
                    existing_id = space.get("space_id") or space.get("id")
                    break
        except Exception as e:
            print(f"Note: Could not list existing spaces: {e}")

        if existing_id:
            # Update existing space
            resp = w.api_client.do(
                "PATCH",
                f"/api/2.0/genie/spaces/{existing_id}",
                body={
                    "title": space_title,
                    "description": space_description,
                    "serialized_space": json.dumps(serialized_space)
                }
            )
            genie_space_id = existing_id
            print(f"Genie Space updated: {genie_space_id}")
        else:
            # Create new space
            resp = w.api_client.do(
                "POST",
                "/api/2.0/genie/spaces",
                body={
                    "warehouse_id": warehouse_id,
                    "title": space_title,
                    "description": space_description,
                    "serialized_space": json.dumps(serialized_space)
                }
            )
            genie_space_id = resp.get("space_id") or resp.get("id")
            print(f"Genie Space created: {genie_space_id}")

        print(f"  Access: SQL Editor -> Genie -> '{space_title}'")
    except Exception as e:
        print(f"ERROR creating Genie Space: {e}")
        traceback.print_exc()
else:
    print("Skipped Genie Space creation (no warehouse_id)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Summary
# MAGIC
# MAGIC | Resource | Status | Access |
# MAGIC |----------|--------|--------|
# MAGIC | **AI/BI Dashboard** | Created via API | [Open Dashboards →](/sql/dashboards) |
# MAGIC | **Genie Space** | Created via API | [Open Genie →](/sql/genie) |
# MAGIC
# MAGIC ### 🔑 What This Demonstrates
# MAGIC
# MAGIC - **Full API coverage** — Everything in the Databricks UI is also available programmatically
# MAGIC - **Infrastructure as Code** — Dashboards and Genie Spaces can be version-controlled and deployed via CI/CD
# MAGIC - **AI-powered analytics** — Genie lets non-technical users explore data using natural language
# MAGIC - **Governed by Unity Catalog** — Both dashboard queries and Genie queries respect column masks, row filters, and access controls
# MAGIC
# MAGIC > 🆚 **Contrast with Synapse:** Synapse has no equivalent to Genie (natural language SQL). Power BI dashboards require a separate service and cannot be created programmatically from within a notebook.
