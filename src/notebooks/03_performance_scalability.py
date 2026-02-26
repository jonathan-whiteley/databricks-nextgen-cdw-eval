# Databricks notebook source

# MAGIC %md
# MAGIC # ⚡ 3. Performance & Scalability
# MAGIC **Panda Restaurant Group — Synapse to Databricks Evaluation**
# MAGIC
# MAGIC > 🆚 **Synapse Pain Points:**
# MAGIC > - *"Requires heavy tuning across distributions, partitions, and Spark clusters"*
# MAGIC > - *"Hard to diagnose performance regressions and inconsistent behavior"*
# MAGIC > - *"No self-corrected performance tuning"*
# MAGIC
# MAGIC Databricks eliminates manual tuning through **Photon-accelerated serverless compute**, **predictive optimization**, and **built-in query profiling**. No distribution keys. No partition schemes. No resource class juggling.
# MAGIC
# MAGIC **Prerequisites:** Setup notebook runs automatically via `%run`.

# COMMAND ----------

# MAGIC %run ./_resources/00_setup_data_generation

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚀 3.1 Zero-Config Analytical Query
# MAGIC In Synapse, this query would require choosing the right distribution (HASH vs ROUND_ROBIN), partition columns, and resource class. In Databricks, you just write SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 menu items by revenue across all stores (2024)
# MAGIC -- No distribution keys, no partition tuning, no resource classes needed.
# MAGIC SELECT
# MAGIC   m.item_name,
# MAGIC   m.category,
# MAGIC   SUM(s.quantity) AS total_units_sold,
# MAGIC   ROUND(SUM(s.quantity * m.unit_price), 2) AS total_revenue,
# MAGIC   ROUND(SUM(s.quantity * (m.unit_price - m.unit_cost)), 2) AS total_profit
# MAGIC FROM daily_sales s
# MAGIC JOIN menu_items m ON s.item_id = m.item_id
# MAGIC WHERE s.sale_date >= '2024-01-01'
# MAGIC GROUP BY m.item_name, m.category
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚡ 3.2 Complex Multi-Table Aggregation with Photon
# MAGIC This join across 3 tables with window functions runs on the **Photon engine** — a C++ vectorized execution engine that accelerates SQL and Spark workloads without any code changes.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Regional performance with month-over-month growth — Photon accelerated automatically
# MAGIC WITH monthly_revenue AS (
# MAGIC   SELECT
# MAGIC     st.region,
# MAGIC     DATE_TRUNC('month', s.sale_date) AS month,
# MAGIC     SUM(s.quantity * m.unit_price) AS revenue
# MAGIC   FROM daily_sales s
# MAGIC   JOIN stores st ON s.store_id = st.store_id
# MAGIC   JOIN menu_items m ON s.item_id = m.item_id
# MAGIC   GROUP BY st.region, DATE_TRUNC('month', s.sale_date)
# MAGIC )
# MAGIC SELECT
# MAGIC   region,
# MAGIC   month,
# MAGIC   ROUND(revenue, 2) AS revenue,
# MAGIC   ROUND(LAG(revenue) OVER (PARTITION BY region ORDER BY month), 2) AS prev_month_revenue,
# MAGIC   ROUND(((revenue - LAG(revenue) OVER (PARTITION BY region ORDER BY month))
# MAGIC     / LAG(revenue) OVER (PARTITION BY region ORDER BY month)) * 100, 1) AS mom_growth_pct
# MAGIC FROM monthly_revenue
# MAGIC ORDER BY region, month

# COMMAND ----------

# Visualize month-over-month growth by region
import plotly.express as px

df_mom = spark.sql("""
    WITH monthly_revenue AS (
      SELECT st.region, DATE_TRUNC('month', s.sale_date) AS month,
        SUM(s.quantity * m.unit_price) AS revenue
      FROM daily_sales s
      JOIN stores st ON s.store_id = st.store_id
      JOIN menu_items m ON s.item_id = m.item_id
      GROUP BY st.region, DATE_TRUNC('month', s.sale_date)
    )
    SELECT region, month, ROUND(revenue, 2) AS revenue
    FROM monthly_revenue ORDER BY region, month
""").toPandas()

fig = px.line(
    df_mom, x="month", y="revenue", color="region",
    title="Monthly Revenue by Region — Photon-Accelerated Query",
    labels={"revenue": "Revenue ($)", "month": "Month", "region": "Region"},
    markers=True,
)
fig.update_layout(template="plotly_white", width=900, height=450)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔧 3.3 Auto-Optimization (OPTIMIZE + ZORDER)
# MAGIC Databricks automatically manages file compaction and data layout. Below we trigger it manually to show the commands, but with **Predictive Optimization** enabled, this happens automatically in the background.
# MAGIC
# MAGIC > 🆚 **Contrast with Synapse:** You must manually choose and maintain distribution keys (HASH, REPLICATE, ROUND_ROBIN) and partition columns. Change your query patterns? Rebuild the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize the daily_sales table and Z-ORDER by common query columns
# MAGIC OPTIMIZE daily_sales ZORDER BY (sale_date, store_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if Predictive Optimization is available (auto-OPTIMIZE, auto-ZORDER)
# MAGIC -- When enabled, Databricks automatically optimizes tables based on query patterns
# MAGIC DESCRIBE DETAIL daily_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 3.4 Query Profiling for Performance Diagnostics
# MAGIC Use `EXPLAIN` to inspect execution plans. In the Databricks UI, the **Query Profile** provides a visual, interactive breakdown of every stage — far beyond Synapse's limited DMV-based diagnostics. Review past query performance in the [Query History](/sql/history).

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT st.region, m.category, SUM(s.quantity * m.unit_price) AS revenue
# MAGIC FROM daily_sales s
# MAGIC JOIN stores st ON s.store_id = st.store_id
# MAGIC JOIN menu_items m ON s.item_id = m.item_id
# MAGIC WHERE s.sale_date BETWEEN '2024-06-01' AND '2024-12-31'
# MAGIC GROUP BY st.region, m.category

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📈 3.5 Auto-Scaling — Zero-Config Elasticity
# MAGIC
# MAGIC | Capability | Synapse | Databricks |
# MAGIC |-----------|---------|------------|
# MAGIC | Scale up/down | Manual DWU scaling, minutes of downtime | Serverless — instant, automatic |
# MAGIC | Scale out (concurrency) | Workload groups, resource classes | Serverless auto-scales query concurrency |
# MAGIC | Idle costs | Dedicated pools charge while paused resume takes minutes | Serverless suspends in seconds, zero idle cost |
# MAGIC | Mixed workloads | Separate SQL Pool + Spark Pool | Single serverless endpoint handles SQL + Spark + ML |
# MAGIC
# MAGIC > 🔑 **Key Takeaway:** Databricks serverless compute automatically scales to match workload demands. No capacity planning, no DWU levels, no pool management.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 🏆 3.6 Databricks SQL Warehouses: Best-in-Class BI Engine &nbsp; <a href="/sql/warehouses">Open SQL Warehouses →</a>
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://www.databricks.com/wp-content/uploads/2022/06/how-does-it-work-image-5.svg" />
# MAGIC
# MAGIC Databricks SQL is a warehouse engine packed with thousands of optimizations to provide you with the best performance for all your tools, query types and real-world applications. <a href='https://www.databricks.com/blog/2021/11/02/databricks-sets-official-data-warehousing-performance-record.html'>It set the official Data Warehousing Performance Record.</a>
# MAGIC
# MAGIC This includes the next-generation vectorized query engine **Photon**, which together with SQL warehouses, provides up to **12x better price/performance** than other cloud data warehouses.
# MAGIC
# MAGIC **Serverless warehouses** provide instant, elastic SQL compute — decoupled from storage — and will automatically scale to provide unlimited concurrency without disruption, for high concurrency use cases.
# MAGIC
# MAGIC Databricks SQL offers a full warehouse experience with comprehensive capabilities:
# MAGIC
# MAGIC <img style="float: right" width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-dbsql-pk-fk.png" />
# MAGIC
# MAGIC **Data Modeling**
# MAGIC
# MAGIC Comprehensive data modeling support. Organize your data based on your requirements: Data Vault, Star Schema, Inmon, etc. Databricks lets you create PK/FK constraints, identity columns (auto-increment), and more.
# MAGIC
# MAGIC **Query Federation**
# MAGIC
# MAGIC Need to access cross-system data? Databricks SQL query federation lets you define data sources outside of Databricks (e.g., SQL Server, Synapse, PostgreSQL) and query them seamlessly alongside your lakehouse tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🤖 3.7 AI/BI Genie — Let Business Users Talk to Your Data &nbsp; [Open Genie →](/sql/genie)
# MAGIC
# MAGIC Databricks **AI/BI Genie** lets anyone ask questions in plain English against your data. No SQL required — Genie translates natural language into optimized queries, executes them on a SQL warehouse, and presents results conversationally.
# MAGIC
# MAGIC > See the **Bonus notebook** (`08_bonus_dashboard_genie`) for a live demo that programmatically creates a Genie Space over our Panda restaurant data.
# MAGIC
# MAGIC ---
