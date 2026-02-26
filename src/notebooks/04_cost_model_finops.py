# Databricks notebook source

# MAGIC %md
# MAGIC # 💰 4. Cost Model & FinOps
# MAGIC **Panda Restaurant Group — Synapse to Databricks Evaluation**
# MAGIC
# MAGIC > 🆚 **Synapse Pain Points:**
# MAGIC > - *"Limited visibility and controls hinder proactive cost optimization"*
# MAGIC > - *"Dedicated pools incur charges while idle"*
# MAGIC > - *"Serverless query patterns can produce large unanticipated scan costs"*
# MAGIC
# MAGIC Databricks provides **system tables for full cost visibility**, **custom tagging for chargeback**, **budget policies**, and **serverless auto-suspend** that eliminates idle costs.
# MAGIC
# MAGIC **Prerequisites:** Setup notebook runs automatically via `%run`.

# COMMAND ----------

# MAGIC %run ./_resources/00_setup_data_generation

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 4.1 Cost Visibility via System Tables
# MAGIC Databricks exposes billing and usage data directly as queryable Delta tables. No third-party tools needed — just SQL. Try it yourself in the [SQL Editor](/sql/editor).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query billing usage to see cost by SKU and workspace
# MAGIC -- This is LIVE billing data from your workspace
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   sku_name,
# MAGIC   usage_unit,
# MAGIC   ROUND(SUM(usage_quantity), 2) AS total_dbus
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
# MAGIC GROUP BY usage_date, sku_name, usage_unit
# MAGIC ORDER BY usage_date DESC, total_dbus DESC
# MAGIC LIMIT 20

# COMMAND ----------

# Visualize billing usage trend over the last 30 days
import plotly.express as px

df_billing = spark.sql("""
    SELECT
      usage_date,
      sku_name,
      ROUND(SUM(usage_quantity), 2) AS total_dbus
    FROM system.billing.usage
    WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
    GROUP BY usage_date, sku_name
    ORDER BY usage_date
""").toPandas()

if not df_billing.empty:
    fig = px.area(
        df_billing, x="usage_date", y="total_dbus", color="sku_name",
        title="DBU Consumption by SKU — Last 30 Days",
        labels={"total_dbus": "DBUs", "usage_date": "Date", "sku_name": "SKU"},
    )
    fig.update_layout(template="plotly_white", width=900, height=400)
    fig.show()
else:
    print("No billing data available in the last 30 days.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🏷️ 4.2 Cost Attribution with Custom Tags
# MAGIC Tag warehouses, clusters, and jobs with team, project, or cost-center labels for precise chargeback/showback reporting.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Query usage attributed by custom tags
# MAGIC -- In practice, you tag warehouses/clusters when creating them:
# MAGIC --   CREATE WAREHOUSE ... WITH TAGS ('team' = 'data-engineering', 'cost_center' = 'restaurant-ops')
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   custom_tags,
# MAGIC   sku_name,
# MAGIC   ROUND(SUM(usage_quantity), 2) AS total_dbus
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= DATEADD(DAY, -7, CURRENT_DATE())
# MAGIC   AND custom_tags IS NOT NULL
# MAGIC GROUP BY usage_date, custom_tags, sku_name
# MAGIC ORDER BY usage_date DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### 💡 4.3 Serverless Pricing — Pay Only for What You Use
# MAGIC
# MAGIC Databricks serverless compute uses a **consumption-based pricing model** — you pay per DBU (Databricks Unit) only while queries are running. Compare this to Synapse's capacity-based model:
# MAGIC
# MAGIC | Pricing Dimension | Synapse Dedicated Pool | Databricks Serverless |
# MAGIC |------------------|----------------------|----------------------|
# MAGIC | **Billing unit** | DWU-hours (fixed capacity) | DBUs (per-query consumption) |
# MAGIC | **Minimum commitment** | 100 DWU always running | **None** — $0 when idle |
# MAGIC | **Scale granularity** | DWU tiers (100, 200, 500...) | **Continuous** — scales per-query |
# MAGIC | **Idle cost** | Full DWU rate even with 0 queries | **$0** — auto-suspends in seconds |
# MAGIC | **Concurrency scaling** | Extra cost for workload isolation | **Included** — auto-scales transparently |
# MAGIC | **Startup time** | 5-10 min to resume paused pool | **Sub-second** — instant cold start |
# MAGIC
# MAGIC > 🔑 **For Panda's workload profile:** If queries run 8 hours/day on weekdays, Synapse charges for 24/7 or requires a pause/resume workflow. Databricks charges only for those 8 hours — roughly **60-70% less** in idle cost savings alone, before factoring in Photon's performance advantage that reduces per-query DBU consumption.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🛡️ 4.4 Budget Policies & Guardrails
# MAGIC Set spending limits and alerts to prevent runaway costs — something Synapse lacks natively.
# MAGIC
# MAGIC ```sql
# MAGIC -- Example: Create a budget policy (requires account admin)
# MAGIC -- This is shown as reference — uncomment to run if you have permissions
# MAGIC --
# MAGIC -- CREATE BUDGET POLICY panda_budget
# MAGIC --   WITH LIMIT 5000
# MAGIC --   PERIOD MONTHLY
# MAGIC --   FILTER ON (catalog = 'jdub_demo')
# MAGIC --   ALERT AT 50, 75, 90, 100 PERCENT;
# MAGIC ```
# MAGIC
# MAGIC ### ⚡ 4.5 No Idle Costs — Serverless Auto-Suspend
# MAGIC
# MAGIC | Scenario | Synapse Dedicated Pool | Databricks Serverless |
# MAGIC |----------|----------------------|----------------------|
# MAGIC | Overnight (no queries) | **Still charging** unless manually paused. Resume takes 5-10 min. | **$0** — auto-suspends in seconds |
# MAGIC | Weekend batch window | Charge 24/7 or pause/resume dance | Pay only for the batch runtime |
# MAGIC | Ad-hoc analyst query | Full pool cost even for 1 query | Sub-second startup, pay per query |
# MAGIC
# MAGIC > 🔑 **Key Takeaway:** Databricks serverless eliminates idle costs entirely. System tables + tagging + budget policies give you the visibility and guardrails Synapse lacks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ 4.6 Cost Model Summary
# MAGIC
# MAGIC | Cost Concern | Synapse Challenge | Databricks Solution |
# MAGIC |-------------|------------------|-------------------|
# MAGIC | **Idle compute** | Dedicated pools charge 24/7 | **Serverless** auto-suspends in seconds — $0 idle |
# MAGIC | **Cost visibility** | Limited Azure Cost Management | **System tables** — query billing with SQL |
# MAGIC | **Chargeback** | Manual tagging in Azure | **Custom tags** on warehouses, clusters, and jobs |
# MAGIC | **Budget controls** | Azure Budgets (coarse-grained) | **Budget policies** with per-catalog granularity |
# MAGIC | **Pricing model** | Fixed DWU capacity tiers | **Per-query DBU** consumption — pay for what you use |
# MAGIC | **Concurrency cost** | Extra workload isolation config | **Included** in serverless auto-scaling |
# MAGIC | **Admin overhead** | Capacity planning, pause/resume scripts | **Zero** — serverless manages everything |
# MAGIC
# MAGIC ---
