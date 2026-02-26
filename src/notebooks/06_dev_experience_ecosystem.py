# Databricks notebook source

# MAGIC %md
# MAGIC # 🔗 6. Developer Experience, Ecosystem & Integration
# MAGIC **Panda Restaurant Group — Synapse to Databricks Evaluation**
# MAGIC
# MAGIC > 🆚 **Synapse Pain Points:**
# MAGIC > - *"No AI integration for code (SQL, Python) development"*
# MAGIC > - *"Need familiar SQL Server-like experience"*
# MAGIC > - *"Need deep Azure ecosystem integration (ADLS, Power BI, Azure ML, Key Vault)"*
# MAGIC > - *"Direct connectors to vendor platforms (Jams, Informatica Cloud)"*
# MAGIC > - *"Complex environment management (dev/staging/prod)"*
# MAGIC > - *"Weak multi-cloud support"*
# MAGIC
# MAGIC This notebook covers the **full developer experience** — SQL capabilities, Python + SQL interop, the AI assistant, CI/CD with Asset Bundles, ecosystem connectors, and open formats.

# COMMAND ----------

# MAGIC %run ./_resources/00_setup_data_generation

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 💻 SQL Development
# MAGIC ### 6.1 Familiar SQL — T-SQL Compatible Syntax
# MAGIC Databricks SQL supports CTEs, window functions, MERGE, PIVOT, and other constructs that SQL Server / Synapse users expect. Try these in the [SQL Editor](/sql/editor) or [Queries](/sql/queries) menu.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CTE + Window Functions — familiar SQL Server patterns
# MAGIC WITH ranked_stores AS (
# MAGIC   SELECT
# MAGIC     st.store_name,
# MAGIC     st.region,
# MAGIC     st.state,
# MAGIC     ROUND(SUM(s.quantity * m.unit_price), 2) AS revenue,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY st.region ORDER BY SUM(s.quantity * m.unit_price) DESC) AS rank_in_region,
# MAGIC     ROUND(SUM(s.quantity * m.unit_price) / SUM(SUM(s.quantity * m.unit_price)) OVER (PARTITION BY st.region) * 100, 1) AS pct_of_region
# MAGIC   FROM daily_sales s
# MAGIC   JOIN stores st ON s.store_id = st.store_id
# MAGIC   JOIN menu_items m ON s.item_id = m.item_id
# MAGIC   WHERE s.sale_date >= '2024-01-01'
# MAGIC   GROUP BY st.store_name, st.region, st.state
# MAGIC )
# MAGIC SELECT * FROM ranked_stores
# MAGIC WHERE rank_in_region <= 3
# MAGIC ORDER BY region, rank_in_region

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE (upsert) — familiar pattern for data engineers coming from SQL Server
# MAGIC -- Create a staging table with updated menu prices
# MAGIC CREATE OR REPLACE TEMP VIEW menu_updates AS
# MAGIC SELECT 1 AS item_id, 'Orange Chicken' AS item_name, 'Entree' AS category, 2.10 AS unit_cost, 5.99 AS unit_price
# MAGIC UNION ALL
# MAGIC SELECT 5, 'Honey Walnut Shrimp', 'Entree', 3.20, 6.99;
# MAGIC
# MAGIC -- MERGE into the target table
# MAGIC MERGE INTO menu_items AS target
# MAGIC USING menu_updates AS source
# MAGIC ON target.item_id = source.item_id
# MAGIC WHEN MATCHED THEN UPDATE SET target.unit_price = source.unit_price
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC SELECT item_id, item_name, unit_price FROM menu_items WHERE item_id IN (1, 5)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PIVOT — another SQL Server favorite, natively supported
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     st.region,
# MAGIC     m.category,
# MAGIC     ROUND(SUM(s.quantity * m.unit_price), 2) AS revenue
# MAGIC   FROM daily_sales s
# MAGIC   JOIN stores st ON s.store_id = st.store_id
# MAGIC   JOIN menu_items m ON s.item_id = m.item_id
# MAGIC   WHERE s.sale_date >= '2024-01-01'
# MAGIC   GROUP BY st.region, m.category
# MAGIC )
# MAGIC PIVOT (
# MAGIC   SUM(revenue)
# MAGIC   FOR category IN ('Entree', 'Side', 'Combo', 'Appetizer', 'Beverage', 'Dessert', 'Kids')
# MAGIC )
# MAGIC ORDER BY region

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🐍 6.2 Python + SQL in One Notebook
# MAGIC Switch seamlessly between SQL and Python in the same notebook. No separate Spark pool required.

# COMMAND ----------

# Use SQL results in Python for visualization
import plotly.express as px

df_region = spark.sql("""
    SELECT st.region, m.category,
      ROUND(SUM(s.quantity * m.unit_price), 2) AS revenue
    FROM daily_sales s
    JOIN stores st ON s.store_id = st.store_id
    JOIN menu_items m ON s.item_id = m.item_id
    WHERE sale_date >= '2024-01-01'
    GROUP BY st.region, m.category
""").toPandas()

fig = px.bar(
    df_region,
    x="region",
    y="revenue",
    color="category",
    title="2024 Revenue by Region and Menu Category",
    barmode="stack",
    labels={"revenue": "Revenue ($)", "region": "Region"},
)
fig.update_layout(template="plotly_white")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🤖 6.3 Databricks Assistant — AI-Powered Development
# MAGIC
# MAGIC The **Databricks Assistant** is an AI coding companion built into every notebook, SQL editor, and file editor. It understands your data, your schema, and the Databricks platform.
# MAGIC
# MAGIC | Capability | How It Works |
# MAGIC |-----------|-------------|
# MAGIC | **Natural language to SQL/Python** | Type "Show me top stores by revenue this quarter" and get working code |
# MAGIC | **Code explanation** | Select any code block and "Explain this" |
# MAGIC | **Debug errors** | When a cell fails, click "Diagnose error" for AI-powered fix suggestions |
# MAGIC | **Auto-complete** | Context-aware code completion using your schema and table metadata |
# MAGIC | **Documentation** | Ask "How do I set up Delta Sharing?" and get platform-specific answers |
# MAGIC | **Generate tests** | "Write unit tests for this function" |
# MAGIC
# MAGIC > **Try it now:** Click the Assistant icon (sparkle) in the sidebar or press `Cmd+I` in any cell.
# MAGIC >
# MAGIC > 🆚 **Contrast with Synapse:** Synapse has no built-in AI assistant. Developers rely on external tools like GitHub Copilot for code help, with no awareness of Synapse-specific patterns or your data catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🔄 Environment Management & CI/CD
# MAGIC ### 6.4 Databricks Asset Bundles (DABs)
# MAGIC
# MAGIC **DABs** let you define your entire workspace — jobs, pipelines, permissions, and configurations — as code and promote across environments.
# MAGIC
# MAGIC ```yaml
# MAGIC # databricks.yml — define your project as code
# MAGIC bundle:
# MAGIC   name: panda-restaurant-analytics
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     workspace:
# MAGIC       host: https://adb-dev.azuredatabricks.net
# MAGIC   staging:
# MAGIC     workspace:
# MAGIC       host: https://adb-staging.azuredatabricks.net
# MAGIC   prod:
# MAGIC     workspace:
# MAGIC       host: https://adb-prod.azuredatabricks.net
# MAGIC
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     daily_sales_pipeline:
# MAGIC       name: "Daily Sales ETL"
# MAGIC       tasks:
# MAGIC         - task_key: ingest
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/ingest_sales.py
# MAGIC ```
# MAGIC
# MAGIC ```bash
# MAGIC # Deploy to any environment with one command
# MAGIC databricks bundle deploy --target prod
# MAGIC ```
# MAGIC
# MAGIC > 🆚 **Contrast with Synapse:** Managing dev/staging/prod in Synapse requires separate resource groups, ARM templates, and complex CI/CD pipelines. DABs make it a single config file.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🔌 Ecosystem & Integration
# MAGIC ### 6.5 Azure Ecosystem Integration
# MAGIC Databricks on Azure integrates natively with the Azure services Panda already uses.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Access data in ADLS Gen2 via Unity Catalog external locations
# MAGIC -- No storage keys in notebooks — credentials managed by Unity Catalog
# MAGIC -- SHOW EXTERNAL LOCATIONS;

# MAGIC -- Example: Create an external location (one-time admin setup)
# MAGIC -- CREATE EXTERNAL LOCATION adls_panda
# MAGIC --   URL 'abfss://container@storageaccount.dfs.core.windows.net/path'
# MAGIC --   WITH (STORAGE CREDENTIAL azure_credential);

# MAGIC -- After setup, users query external data like any other table:
# MAGIC -- SELECT * FROM delta.`abfss://container@account.dfs.core.windows.net/sales/`;

# MAGIC SELECT 'External locations let Unity Catalog govern ADLS access — no keys in code.' AS info

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📂 6.6 Open Formats — No Vendor Lock-In
# MAGIC All data in Databricks is stored as **Delta Lake (Parquet + transaction log)**. Any tool that reads Parquet can read your data.

# COMMAND ----------

# Read the daily_sales Delta table directly as files to demonstrate open format
table_location = spark.sql("DESCRIBE DETAIL daily_sales").select("location").first()[0]
print(f"Table storage location: {table_location}")
print(f"\nDelta tables are Parquet files + a transaction log.")
print(f"Any Parquet-compatible tool (Spark, pandas, DuckDB, Snowflake, Trino) can read this data.")
print(f"No proprietary format. No vendor lock-in.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔗 6.7 Partner Connect & Connectors
# MAGIC
# MAGIC | Integration | Connection Method | Notes |
# MAGIC |------------|------------------|-------|
# MAGIC | **Power BI** | Native connector (DirectQuery + Import) | One-click via Partner Connect |
# MAGIC | **Informatica Cloud** | Partner Connect + JDBC/ODBC | Direct integration with Unity Catalog |
# MAGIC | **Jams Scheduler** | REST API + JDBC | Trigger Databricks jobs via REST API, query results via JDBC |
# MAGIC | **Azure Data Factory** | Native linked service | Orchestrate Databricks notebooks and jobs |
# MAGIC | **Azure Key Vault** | Secret scope backing | `dbutils.secrets.get(scope, key)` — no plaintext credentials |
# MAGIC | **Azure ML** | MLflow integration | Track experiments, deploy models across Azure ML and Databricks |
# MAGIC | **dbt** | dbt-databricks adapter | First-class support via Partner Connect |
# MAGIC | **Fivetran / Airbyte** | Partner Connect | Managed ingestion into Delta Lake |
# MAGIC
# MAGIC > **Navigate to:** Open [Partner Connect](/partnerconnect) to see one-click integrations with 50+ tools.
# MAGIC
# MAGIC ### 🌐 6.8 Multi-Cloud — Same Platform Everywhere
# MAGIC
# MAGIC | | Synapse | Databricks |
# MAGIC |---|--------|------------|
# MAGIC | Azure | Yes | Yes |
# MAGIC | AWS | No | Yes |
# MAGIC | GCP | No | Yes |
# MAGIC | Same SQL, same APIs | N/A | Yes — identical experience across clouds |
# MAGIC
# MAGIC > 🔑 **Key Takeaway:** If Panda ever expands to multi-cloud or needs to work with partners on different clouds, Databricks runs identically on Azure, AWS, and GCP. Synapse locks you into Azure.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### ✅ 6.9 Full Summary: Synapse Pain Points to Databricks Solutions
# MAGIC
# MAGIC | Pain Point | Synapse Challenge | Databricks Solution |
# MAGIC |-----------|------------------|-------------------|
# MAGIC | **Performance tuning** | Manual distributions, partitions, resource classes | Photon + Predictive Optimization — zero tuning |
# MAGIC | **Performance diagnostics** | Limited DMVs | Query Profile UI + system tables |
# MAGIC | **Idle costs** | Dedicated pools charge 24/7 | Serverless auto-suspend in seconds |
# MAGIC | **Cost visibility** | Limited native tooling | System tables + tagging + budget policies |
# MAGIC | **Security UI** | No dedicated interface | Catalog Explorer — visual governance |
# MAGIC | **Data loss prevention** | Gaps in monitoring | Column masks + row filters + audit logs |
# MAGIC | **Entra ID** | Partial integration | Full SCIM sync with groups |
# MAGIC | **Data sharing** | ETL required | Delta Sharing — governed, zero-copy |
# MAGIC | **Agg table automation** | Manual ETL | Materialized views with auto-refresh |
# MAGIC | **Cross-DB queries** | External tables / OPENROWSET | Dot notation across catalogs |
# MAGIC | **Data lineage** | Third-party tools | Built-in Unity Catalog lineage |
# MAGIC | **AI code assistant** | None | Databricks Assistant in every editor |
# MAGIC | **SQL experience** | T-SQL | Full SQL support — CTEs, MERGE, PIVOT, windows |
# MAGIC | **ML integration** | Separate Azure ML | Built-in MLflow + Model Serving |
# MAGIC | **AI from SQL** | Not available | `ai_query()` — call LLMs directly from SQL |
# MAGIC | **Vendor connectors** | Limited | Partner Connect — 50+ one-click integrations |
# MAGIC | **Multi-cloud** | Azure only | Azure + AWS + GCP |
# MAGIC | **DevOps overhead** | Multiple pools, pipelines, networking | Serverless + Asset Bundles + single admin |
# MAGIC | **Environment management** | ARM templates + complex CI/CD | Databricks Asset Bundles (YAML) |
# MAGIC | **Open formats** | Proprietary | Delta Lake (Parquet-based) |
# MAGIC
# MAGIC ---
