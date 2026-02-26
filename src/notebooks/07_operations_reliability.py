# Databricks notebook source

# MAGIC %md
# MAGIC # 🔄 7. Operations & Reliability
# MAGIC **Panda Restaurant Group — Synapse to Databricks Evaluation**
# MAGIC
# MAGIC > 🆚 **Synapse Pain Points:**
# MAGIC > - *"High DevOps and maintenance overhead (data sync, environments)"*
# MAGIC > - *"Complex to manage SQL, Spark, pipelines, networking, and environments together"*
# MAGIC > - *"Admin overhead for capacity management, tuning, and upgrades"*
# MAGIC > - *"Need a clear migration path from Synapse without a big-bang cutover"*
# MAGIC
# MAGIC This notebook covers **platform operations, monitoring, pipeline orchestration, automated maintenance, migration strategy, and reliability** — everything an ops team needs to evaluate.

# COMMAND ----------

# MAGIC %run ./_resources/00_setup_data_generation

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ⚡ 7.1 Serverless — Zero Infrastructure Management
# MAGIC
# MAGIC Every cell in every notebook in this demo ran on **serverless compute** — no clusters to provision, no pools to configure, no DWU levels to choose. Compare this to the Synapse operational burden:
# MAGIC
# MAGIC | Task | Synapse | Databricks Serverless |
# MAGIC |------|---------|----------------------|
# MAGIC | Provision compute | Create Dedicated SQL Pool, choose DWU level | **Nothing** — just run your query |
# MAGIC | Scale up for heavy workloads | Manually increase DWUs (causes brief downtime) | **Automatic** — transparent scaling |
# MAGIC | Scale down after peak | Manually decrease DWUs or pause pool | **Automatic** — suspends in seconds |
# MAGIC | Spark workloads | Create separate Spark Pool, configure libraries | **Same serverless compute** handles SQL + Spark + ML |
# MAGIC | Patching & upgrades | Schedule maintenance windows | **Zero-downtime** — Databricks manages all updates |
# MAGIC | Concurrency scaling | Configure workload groups, resource classes | **Automatic** — serverless handles concurrency |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: this query is running on serverless compute right now
# MAGIC SELECT
# MAGIC   current_user() AS user,
# MAGIC   current_catalog() AS catalog,
# MAGIC   current_schema() AS schema,
# MAGIC   'Serverless — no pools, no clusters, no config' AS compute_type

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📊 7.2 Operational Monitoring via System Tables
# MAGIC
# MAGIC Databricks exposes **live operational data as queryable Delta tables** in the `system` catalog. No Azure Monitor setup, no Log Analytics workspace, no Kusto queries — just SQL. View live status in the [SQL Warehouses](/sql/warehouses) console.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 📦 Warehouse Inventory
# MAGIC See all SQL warehouses, their size, and current state — one query replaces the Azure portal.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All SQL warehouses in the workspace
# MAGIC SELECT
# MAGIC   warehouse_name,
# MAGIC   warehouse_type,
# MAGIC   warehouse_size,
# MAGIC   auto_stop_minutes,
# MAGIC   created_by
# MAGIC FROM system.compute.warehouses
# MAGIC WHERE delete_time IS NULL
# MAGIC ORDER BY warehouse_name

# COMMAND ----------

# MAGIC %md
# MAGIC #### 📈 Query History — Who Ran What, When, and How Long?
# MAGIC Identify slow queries, heavy users, and scan patterns — all from a single system table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recent query execution stats — see duration, rows scanned, errors
# MAGIC SELECT
# MAGIC   executed_by AS user,
# MAGIC   execution_status AS status,
# MAGIC   ROUND(total_duration_ms / 1000.0, 2) AS duration_sec,
# MAGIC   produced_rows,
# MAGIC   start_time
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= DATEADD(HOUR, -2, CURRENT_TIMESTAMP())
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC #### 🔄 Job Run History — Pipeline Health at a Glance
# MAGIC Track every scheduled job and pipeline run. Use this to build automated alerting or SLA dashboards.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recent job runs with duration and status
# MAGIC SELECT
# MAGIC   run_name,
# MAGIC   result_state,
# MAGIC   ROUND(run_duration_seconds / 60.0, 1) AS duration_min,
# MAGIC   period_start_time,
# MAGIC   period_end_time
# MAGIC FROM system.lakeflow.job_run_timeline
# MAGIC WHERE period_start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
# MAGIC ORDER BY period_start_time DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC > 🔑 **System tables available for monitoring:**
# MAGIC >
# MAGIC > | System Table | What It Tracks |
# MAGIC > |-------------|---------------|
# MAGIC > | `system.compute.warehouses` | Warehouse inventory and state |
# MAGIC > | `system.query.history` | Every query: duration, user, rows, status |
# MAGIC > | `system.lakeflow.job_run_timeline` | Job and pipeline run history |
# MAGIC > | `system.billing.usage` | DBU consumption by SKU (see notebook 04) |
# MAGIC > | `system.access.audit` | Security audit trail (see notebook 02) |
# MAGIC > | `system.access.table_lineage` | Data lineage (see notebook 01) |
# MAGIC >
# MAGIC > 🆚 **Contrast with Synapse:** Monitoring requires configuring Azure Monitor + Log Analytics + Diagnostic Settings for each resource. Databricks gives you everything as SQL tables — build dashboards, set alerts, analyze trends with the same SQL you already know.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🔧 7.3 Predictive Optimization — Automatic Table Maintenance
# MAGIC
# MAGIC In Synapse, DBAs must manually manage table distributions, statistics updates, index rebuilds, and partition maintenance. Databricks **Predictive Optimization** handles all of this automatically:
# MAGIC
# MAGIC | Maintenance Task | Synapse (Manual) | Databricks (Automatic) |
# MAGIC |-----------------|-----------------|----------------------|
# MAGIC | Compaction (small files) | Rebuild/reorganize indexes | `OPTIMIZE` — runs automatically |
# MAGIC | Data layout tuning | Choose distribution keys upfront | `ZORDER` — auto-applied based on query patterns |
# MAGIC | Statistics updates | `UPDATE STATISTICS` manually | **Automatic** — stats always current |
# MAGIC | Old file cleanup | Manual cleanup scripts | `VACUUM` — auto-removes stale files |
# MAGIC | Partition management | Manual partition scheme design | **Liquid clustering** — adapts to query patterns |
# MAGIC
# MAGIC > When Predictive Optimization is enabled, Databricks continuously monitors query patterns and automatically runs OPTIMIZE, ZORDER, and VACUUM — no DBA intervention needed.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check Predictive Optimization status for our tables
# MAGIC -- When enabled, the platform auto-tunes file layout and compaction
# MAGIC DESCRIBE DETAIL daily_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View recent auto-optimization events on your tables
# MAGIC -- These run in the background when Predictive Optimization is active
# MAGIC SELECT
# MAGIC   table_name,
# MAGIC   operation_type,
# MAGIC   operation_status,
# MAGIC   operation_metrics,
# MAGIC   start_time
# MAGIC FROM system.storage.predictive_optimization_operations_history
# MAGIC WHERE start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🔀 7.4 Lakeflow Declarative Pipelines — Managed ETL
# MAGIC
# MAGIC Synapse Pipelines require managing triggers, linked services, integration runtimes, and copy activities. Databricks **Lakeflow Declarative Pipelines** (formerly Delta Live Tables / DLT) let you define your ETL as simple SQL or Python — the platform handles orchestration, error handling, retries, and data quality.
# MAGIC
# MAGIC ```sql
# MAGIC -- Example: Define a complete ETL pipeline in pure SQL
# MAGIC
# MAGIC -- Bronze: raw ingestion (streaming or batch)
# MAGIC CREATE OR REFRESH STREAMING TABLE raw_sales
# MAGIC AS SELECT * FROM cloud_files('/data/sales/', 'json');
# MAGIC
# MAGIC -- Silver: cleaned and validated
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW clean_sales (
# MAGIC   CONSTRAINT valid_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC AS SELECT store_id, item_id, quantity, sale_date
# MAGIC    FROM STREAM(LIVE.raw_sales)
# MAGIC    WHERE store_id IS NOT NULL;
# MAGIC
# MAGIC -- Gold: business-ready aggregation
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW daily_revenue
# MAGIC AS SELECT sale_date, store_id, SUM(quantity * unit_price) AS revenue
# MAGIC    FROM LIVE.clean_sales c JOIN LIVE.menu_items m ON c.item_id = m.item_id
# MAGIC    GROUP BY sale_date, store_id;
# MAGIC ```
# MAGIC
# MAGIC | Capability | Synapse Pipelines | Lakeflow Declarative Pipelines |
# MAGIC |-----------|------------------|-------------------------------|
# MAGIC | Pipeline definition | JSON config + GUI drag-and-drop | **SQL or Python** — version-controlled |
# MAGIC | Data quality checks | Custom validation activities | **Built-in expectations** — EXPECT constraints inline |
# MAGIC | Error handling | Manual retry/failure activities | **Automatic retries** with dead-letter handling |
# MAGIC | Schema evolution | Manual schema management | **Auto-detected** — handles schema changes gracefully |
# MAGIC | Incremental processing | Change data capture (complex) | **Streaming tables** — incremental by default |
# MAGIC | Monitoring | Pipeline run history in Synapse Studio | [Pipeline UI](/pipelines) — visual DAG with data quality metrics |
# MAGIC
# MAGIC > 🔑 **Key Takeaway:** Lakeflow Declarative Pipelines replace both Synapse Pipelines *and* custom ETL scripts with declarative SQL. Define *what* you want — the platform handles *how*.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📅 7.5 Workflow Orchestration
# MAGIC
# MAGIC Databricks [Workflows](/workflows) replaces Synapse triggers, ADF pipelines, and external schedulers with a single orchestration layer. Jobs can chain notebooks, SQL queries, Python scripts, and pipelines with full dependency management.

# COMMAND ----------

# Show the workflow that orchestrates this entire demo
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# List recent jobs in the workspace
try:
    jobs = list(w.jobs.list(limit=10))
    print(f"{'Job Name':<55} {'Schedule':<30} {'Creator'}")
    print("-" * 110)
    for job in jobs[:10]:
        schedule = "Manual" if not job.settings.schedule else job.settings.schedule.quartz_cron_expression
        print(f"{(job.settings.name or 'Unnamed')[:54]:<55} {str(schedule)[:29]:<30} {job.creator_user_name or 'N/A'}")
except Exception as e:
    print(f"Could not list jobs: {e}")
    print("Navigate to Workflows in the sidebar to view jobs.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Workflow capabilities that replace Synapse operational overhead:**
# MAGIC
# MAGIC | Feature | How It Helps |
# MAGIC |---------|-------------|
# MAGIC | **Task dependencies** | Chain notebooks, SQL, Python, JARs, and pipelines with DAG-style dependencies |
# MAGIC | **Parameterized runs** | Pass catalog, schema, date ranges as parameters — one job definition, multiple environments |
# MAGIC | **Cron scheduling** | Built-in scheduler — no external ADF or Jams required |
# MAGIC | **Retry policies** | Automatic retries with configurable backoff per task |
# MAGIC | **Alerts & notifications** | Email/Slack/webhook on failure, success, or duration threshold |
# MAGIC | **Run-as service principal** | Jobs run with service principal identity — no shared credentials |
# MAGIC | **Multi-task orchestration** | Parallel and sequential task execution in a single job |
# MAGIC
# MAGIC > View and manage all scheduled jobs in the [Workflows](/workflows) console.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🌉 7.6 Migration Path — Lakehouse Federation
# MAGIC
# MAGIC Databricks **Lakehouse Federation** lets you query external databases (including Synapse, SQL Server, PostgreSQL) directly from Databricks SQL — no data movement required. This enables a **gradual migration** instead of a risky big-bang cutover.
# MAGIC
# MAGIC ```sql
# MAGIC -- Step 1: Create a connection to your existing Synapse instance (one-time admin setup)
# MAGIC CREATE CONNECTION synapse_prod TYPE sqlserver
# MAGIC   OPTIONS (
# MAGIC     host 'panda-synapse.sql.azuresynapse.net',
# MAGIC     port '1433',
# MAGIC     user secret('keyvault-scope', 'synapse-username'),
# MAGIC     password secret('keyvault-scope', 'synapse-password')
# MAGIC   );
# MAGIC
# MAGIC -- Step 2: Create a foreign catalog that mirrors the Synapse schema
# MAGIC CREATE FOREIGN CATALOG synapse_legacy USING CONNECTION synapse_prod;
# MAGIC
# MAGIC -- Step 3: Query Synapse data alongside Databricks tables — same SQL, dot notation
# MAGIC SELECT d.sale_date, d.revenue, s.legacy_metric
# MAGIC FROM panda_catalog.analytics.daily_revenue d
# MAGIC JOIN synapse_legacy.dbo.legacy_metrics s ON d.store_id = s.store_id;
# MAGIC ```
# MAGIC
# MAGIC **Migration strategy:**
# MAGIC
# MAGIC | Phase | What Happens | Synapse Status |
# MAGIC |-------|-------------|---------------|
# MAGIC | **1. Connect** | Create federation connection to Synapse | Running (source of truth) |
# MAGIC | **2. Mirror** | Run parallel pipelines — write to both Synapse and Databricks | Running (primary) |
# MAGIC | **3. Validate** | Compare results between Synapse and Databricks tables | Running (validation) |
# MAGIC | **4. Cutover** | Point BI tools and applications to Databricks | Paused (standby) |
# MAGIC | **5. Decommission** | Shut down Synapse after confidence period | Decommissioned |
# MAGIC
# MAGIC > 🔑 **Key Takeaway:** No big-bang migration. Lakehouse Federation lets Panda query Synapse data from Databricks during the transition — migrate table-by-table at your own pace while maintaining full operational continuity.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🏗️ 7.7 Unified Platform — One Tool for Everything
# MAGIC
# MAGIC In Synapse, operations teams manage multiple disconnected services. Databricks consolidates everything into a single platform with a single admin console.
# MAGIC
# MAGIC | In Synapse, you manage... | In Databricks, it's... |
# MAGIC |--------------------------|----------------------|
# MAGIC | Dedicated SQL Pool + DWU config | [Serverless SQL Warehouse](/sql/warehouses) (managed) |
# MAGIC | Spark Pool + libraries + versions | Serverless Compute (managed) |
# MAGIC | Synapse Pipelines + triggers + linked services | [Lakeflow Declarative Pipelines](/pipelines) (SQL-defined) |
# MAGIC | ADF / Synapse triggers for scheduling | [Databricks Workflows](/workflows) (built-in scheduler) |
# MAGIC | Azure Monitor + Log Analytics + Diagnostic Settings | [System Tables](#) — query with SQL |
# MAGIC | Networking + private endpoints per service | Private Link + Unity Catalog (simplified) |
# MAGIC | Separate dev / staging / prod resource groups | Databricks Asset Bundles (CI/CD as code) |
# MAGIC | Multiple admin consoles across services | Single [Admin Console](/settings/workspace) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🛡️ 7.8 Reliability & Disaster Recovery
# MAGIC
# MAGIC | Capability | Synapse | Databricks |
# MAGIC |-----------|---------|------------|
# MAGIC | **SLA** | 99.9% (dedicated pool) | **99.95%** uptime SLA |
# MAGIC | **Backup / Restore** | Automatic restore points (8-hour RPO) | **Delta Lake time travel** — restore to any point up to 30 days |
# MAGIC | **Disaster Recovery** | Geo-redundant backup (manual restore) | **Cross-region replication** with Unity Catalog |
# MAGIC | **Failover** | Manual failover to secondary | **Built into serverless** — automatic |
# MAGIC | **Upgrade management** | Scheduled maintenance windows | **Zero-downtime** — Databricks manages runtime upgrades |
# MAGIC | **Data durability** | Azure Storage redundancy | **Delta Lake** on cloud storage + transaction log |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Demo: Point-in-time recovery with Delta Lake time travel
# MAGIC -- See the full version history of our sales table
# MAGIC DESCRIBE HISTORY daily_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restore a table to any previous version — one command
# MAGIC -- (Commented out to avoid modifying demo data)
# MAGIC -- RESTORE TABLE daily_sales TO VERSION AS OF 0;
# MAGIC
# MAGIC -- Or restore to a specific timestamp:
# MAGIC -- RESTORE TABLE daily_sales TO TIMESTAMP AS OF '2024-06-01 00:00:00';
# MAGIC
# MAGIC SELECT 'Delta time travel provides point-in-time recovery with a single SQL command — no restore points, no support tickets.' AS recovery_info

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ 7.9 Operations & Reliability Summary
# MAGIC
# MAGIC | Operations Concern | Synapse Challenge | Databricks Solution |
# MAGIC |-------------------|------------------|-------------------|
# MAGIC | **Compute management** | Provision & manage SQL Pools, Spark Pools, DWU levels | **Serverless** — zero provisioning |
# MAGIC | **Monitoring** | Azure Monitor + Log Analytics + Kusto | **System tables** — query with SQL |
# MAGIC | **ETL pipelines** | Synapse Pipelines + linked services + copy activities | **Lakeflow Declarative Pipelines** — SQL-defined ETL |
# MAGIC | **Scheduling** | ADF triggers, Synapse triggers | **[Workflows](/workflows)** — built-in, DAG-based |
# MAGIC | **Table maintenance** | Manual stats, distributions, index rebuilds | **Predictive Optimization** — automatic |
# MAGIC | **Migration** | Big-bang cutover or parallel run with ETL | **Lakehouse Federation** — query Synapse in-place |
# MAGIC | **Platform consolidation** | 5+ Azure services to manage | **One platform**, one admin console |
# MAGIC | **Uptime SLA** | 99.9% | **99.95%** |
# MAGIC | **Disaster recovery** | Geo-backup + manual restore (hours) | **Time travel + cross-region replication** (seconds) |
# MAGIC | **Upgrades** | Scheduled maintenance windows | **Zero-downtime** — managed by Databricks |
# MAGIC
# MAGIC > 🔑 **Key Takeaway:** Databricks eliminates the operational overhead that makes Synapse expensive beyond just compute costs. Serverless compute, automatic maintenance, SQL-based monitoring, and a clear migration path mean Panda's ops team can focus on business value instead of infrastructure management.
# MAGIC
# MAGIC ---
