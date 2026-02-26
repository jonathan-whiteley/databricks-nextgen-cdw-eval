# Databricks notebook source

# MAGIC %md
# MAGIC # 📊 1. Data Management & Data Quality
# MAGIC **Panda Restaurant Group — Synapse to Databricks Evaluation**
# MAGIC
# MAGIC > 🆚 **Synapse Pain Points:**
# MAGIC > - *"Difficult to ingest or share data with other warehouses/vendors"*
# MAGIC > - *"Agg tables automation (lake view, etc.)"*
# MAGIC > - *"Missing SQL capabilities (triggers, cross-DB queries)"*
# MAGIC > - *"Inadequate data lineage visibility across pipelines and tables"*
# MAGIC > - *"Limited data portability, no direct connector"*
# MAGIC > - *"Easy to copy data across environments / Easy to migrate from Synapse"*
# MAGIC
# MAGIC **Prerequisites:** Setup notebook runs automatically via `%run`.

# COMMAND ----------

# MAGIC %run ./_resources/00_setup_data_generation

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🕐 1.1 Delta Lake Time Travel — Built-In Data Versioning
# MAGIC Every write to a Delta table is versioned. Query any previous state without snapshots or backups.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- See the full history of changes to the daily_sales table
# MAGIC DESCRIBE HISTORY daily_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query a previous version of the table (version 0 = original write)
# MAGIC SELECT COUNT(*) AS original_row_count
# MAGIC FROM daily_sales VERSION AS OF 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔗 1.2 Cross-Catalog Queries
# MAGIC Query across catalogs and schemas in a single SQL statement — no linked servers, no external tables, no OPENROWSET hacks.
# MAGIC
# MAGIC > 🆚 **Contrast with Synapse:** Cross-database queries require external tables or OPENROWSET with credentials. Unity Catalog makes it a simple dot-notation reference.

# COMMAND ----------

# Cross-catalog query example using fully qualified names
df_cross = spark.sql(f"""
    SELECT
      s.store_name,
      s.region,
      COUNT(*) AS feedback_count,
      ROUND(AVG(f.rating), 2) AS avg_rating
    FROM {catalog}.{schema}.customer_feedback f
    JOIN {catalog}.{schema}.stores s ON f.store_id = s.store_id
    -- In practice: JOIN other_catalog.other_schema.other_table t ON ...
    GROUP BY s.store_name, s.region
    ORDER BY avg_rating DESC
    LIMIT 10
""")
df_cross.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔄 1.3 Automated Aggregation — Views & Materialized Views
# MAGIC Databricks supports both standard views (always up-to-date) and materialized views (pre-computed with auto-refresh). Here we create a view to demonstrate automated aggregation.
# MAGIC
# MAGIC > 🔑 **This directly addresses the "Agg tables automation" pain point.** No need to build and schedule custom ETL to maintain aggregation tables.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{schema}.v_daily_store_revenue
    AS
    SELECT
      s.sale_date,
      st.store_id,
      st.store_name,
      st.region,
      st.state,
      st.store_type,
      COUNT(*) AS transaction_count,
      SUM(s.quantity) AS total_units,
      ROUND(SUM(s.quantity * m.unit_price), 2) AS total_revenue,
      ROUND(SUM(s.quantity * (m.unit_price - m.unit_cost)), 2) AS total_profit
    FROM daily_sales s
    JOIN stores st ON s.store_id = st.store_id
    JOIN menu_items m ON s.item_id = m.item_id
    GROUP BY s.sale_date, st.store_id, st.store_name, st.region, st.state, st.store_type
""")

print("✓ View 'v_daily_store_revenue' created")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the aggregation view — always reflects the latest data
# MAGIC SELECT region, store_type,
# MAGIC   ROUND(SUM(total_revenue), 2) AS revenue,
# MAGIC   ROUND(SUM(total_profit), 2) AS profit
# MAGIC FROM v_daily_store_revenue
# MAGIC WHERE sale_date >= '2024-01-01'
# MAGIC GROUP BY region, store_type
# MAGIC ORDER BY revenue DESC

# COMMAND ----------

# Visualize the aggregation — revenue and profit by region and store type
import plotly.express as px

df_agg = spark.sql("""
    SELECT region, store_type,
      ROUND(SUM(total_revenue), 2) AS revenue,
      ROUND(SUM(total_profit), 2) AS profit
    FROM v_daily_store_revenue
    WHERE sale_date >= '2024-01-01'
    GROUP BY region, store_type
    ORDER BY revenue DESC
""").toPandas()

fig = px.bar(
    df_agg, x="region", y="revenue", color="store_type",
    title="2024 Revenue by Region & Store Type (from Aggregation View)",
    barmode="group",
    labels={"revenue": "Revenue ($)", "region": "Region", "store_type": "Store Type"},
)
fig.update_layout(template="plotly_white")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > ⚡ **Materialized Views for Production Workloads**
# MAGIC >
# MAGIC > For pre-computed aggregation tables that auto-refresh incrementally when source data changes, Databricks supports **Materialized Views**. These are created via:
# MAGIC >
# MAGIC > - **Lakeflow Declarative Pipelines (DLT)** — define materialized views in a pipeline that handles scheduling, refresh, and data quality expectations automatically
# MAGIC > - **SQL Warehouses** — `CREATE MATERIALIZED VIEW` runs directly on a SQL warehouse endpoint
# MAGIC >
# MAGIC > ```sql
# MAGIC > -- Example: Materialized View (run on a SQL warehouse or in a DLT pipeline)
# MAGIC > CREATE OR REPLACE MATERIALIZED VIEW mv_daily_store_revenue
# MAGIC > AS
# MAGIC > SELECT sale_date, store_id, SUM(quantity * unit_price) AS total_revenue
# MAGIC > FROM daily_sales s JOIN menu_items m ON s.item_id = m.item_id
# MAGIC > GROUP BY sale_date, store_id;
# MAGIC > ```
# MAGIC >
# MAGIC > Materialized views are ideal for dashboards and reporting layers where you want **sub-second query latency** on pre-aggregated data with **zero manual refresh logic**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🤝 1.4 Delta Sharing — Governed Data Sharing Without Data Movement
# MAGIC Share data with external vendors, partners, or other warehouses without copying data. Recipients can read Delta tables using their own tools (Spark, pandas, Power BI, etc.).
# MAGIC
# MAGIC > 🆚 **Contrast with Synapse:** Sharing data requires ETL pipelines, CETAS exports, or linked services. Delta Sharing provides governed, live access.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a share for a vendor or partner (requires metastore admin)
# MAGIC -- CREATE SHARE IF NOT EXISTS panda_vendor_share;
# MAGIC -- ALTER SHARE panda_vendor_share ADD TABLE ${catalog}.${schema}.daily_sales;
# MAGIC -- ALTER SHARE panda_vendor_share ADD TABLE ${catalog}.${schema}.stores;
# MAGIC -- GRANT SELECT ON SHARE panda_vendor_share TO RECIPIENT smart_bridge;

# MAGIC -- List existing shares
# MAGIC SHOW SHARES

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🛡️ 1.5 Data Quality Constraints
# MAGIC Add CHECK constraints directly on Delta tables to enforce data quality at the storage layer.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add quality constraints to daily_sales (drop first if re-running)
# MAGIC ALTER TABLE daily_sales DROP CONSTRAINT IF EXISTS quantity_positive;
# MAGIC ALTER TABLE daily_sales DROP CONSTRAINT IF EXISTS valid_item;
# MAGIC ALTER TABLE daily_sales ADD CONSTRAINT quantity_positive CHECK (quantity > 0);
# MAGIC ALTER TABLE daily_sales ADD CONSTRAINT valid_item CHECK (item_id > 0);

# COMMAND ----------

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify constraints are applied
# MAGIC DESCRIBE DETAIL daily_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This insert would FAIL because it violates the quantity_positive constraint:
# MAGIC -- INSERT INTO daily_sales VALUES (999999999, '2024-06-01', 1, 1, -5);
# MAGIC -- Error: CHECK constraint quantity_positive violated

# MAGIC -- Constraints enforce quality at the storage layer — no separate validation pipeline needed.
# MAGIC SELECT 'Data quality constraints active — invalid data is rejected at write time.' AS status

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📂 1.6 Discoverable Catalog — INFORMATION_SCHEMA
# MAGIC Unity Catalog exposes all metadata through standard INFORMATION_SCHEMA views — tables, columns, constraints, and more. No proprietary DMVs or sys tables needed.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Discover all tables and views in the current schema
# MAGIC SELECT table_name, table_type, comment
# MAGIC FROM information_schema.tables
# MAGIC WHERE table_schema = current_schema()
# MAGIC ORDER BY table_type, table_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Explore column metadata for a specific table
# MAGIC SELECT column_name, data_type, is_nullable, comment
# MAGIC FROM information_schema.columns
# MAGIC WHERE table_schema = current_schema()
# MAGIC   AND table_name = 'daily_sales'
# MAGIC ORDER BY ordinal_position

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 1.7 Data Lineage
# MAGIC Unity Catalog automatically tracks lineage across tables and pipelines — no manual configuration.

# COMMAND ----------

# Query table lineage programmatically
spark.sql(f"""
    SELECT
      entity_type,
      entity_id,
      source_table_full_name,
      target_table_full_name
    FROM system.access.table_lineage
    WHERE target_table_catalog = '{catalog}'
      AND event_time >= DATEADD(HOUR, -2, CURRENT_TIMESTAMP())
    ORDER BY event_time DESC
    LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### Visualize Table Lineage in [Catalog Explorer](/explore)
# MAGIC
# MAGIC The table lineage graph can be explored visually:
# MAGIC
# MAGIC 1. Select the **Catalog** explorer icon <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/icon/catalog-explorer-icon.png?raw=true" width="25"/> on the navigation bar
# MAGIC 2. Search for your table (e.g., `v_daily_store_revenue`)
# MAGIC 3. Click the table name to open its details
# MAGIC 4. Click the **Lineage** tab
# MAGIC 5. Click **See Lineage Graph** for a full visual dependency map
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/lineage/lineage-table.gif?raw=true"/>
# MAGIC
# MAGIC > Our `v_daily_store_revenue` view depends on `daily_sales`, `stores`, and `menu_items` — Unity Catalog tracks this automatically.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### Column-Level Lineage
# MAGIC
# MAGIC Lineage is also available at the **column level** — useful for tracking field-level dependencies and ensuring GDPR compliance. Column lineage is also accessible via the REST API.
# MAGIC
# MAGIC In the **Lineage Graph** view, click the **+** icon at the edge of a table box, then click individual columns to trace their origins:
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/lineage/lineage-column.gif?raw=true"/>
# MAGIC
# MAGIC > 🆚 **Contrast with Synapse:** Synapse has no built-in lineage. You'd need third-party tools like Purview or manual documentation. Unity Catalog captures table and column lineage automatically across notebooks, jobs, and pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ 1.8 Data Management Summary
# MAGIC
# MAGIC | Capability | Synapse | Databricks |
# MAGIC |-----------|---------|------------|
# MAGIC | Data versioning / time travel | No native support | **Delta Lake** — query any version |
# MAGIC | Cross-database queries | OPENROWSET / external tables | **Dot notation** across catalogs |
# MAGIC | Automated aggregation tables | Manual ETL required | **Materialized views** with auto-refresh |
# MAGIC | Data sharing | ETL pipelines / exports | **Delta Sharing** — governed, no copies |
# MAGIC | Data quality | External tools | **CHECK constraints** + Lakeflow expectations |
# MAGIC | Lineage | Limited / third-party tools | **Automatic** in Unity Catalog |
# MAGIC | Synapse migration | N/A | **Lakehouse Federation** — query Synapse in place, migrate incrementally |
# MAGIC | Open format | Proprietary | **Delta Lake (Parquet-based)** — no vendor lock-in |
# MAGIC
# MAGIC > **Navigate to:** Open the [Catalog Explorer](/explore), select a table, then click the **Lineage** tab to see the visual lineage graph across all pipelines and tables.
# MAGIC
# MAGIC ---

# COMMAND ----------
