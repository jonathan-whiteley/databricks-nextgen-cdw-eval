# Databricks notebook source

# MAGIC %md
# MAGIC # Cleanup
# MAGIC **Panda Restaurant Group Demo**
# MAGIC
# MAGIC Run this notebook to remove all demo objects created by the setup notebook.
# MAGIC
# MAGIC **WARNING:** This will drop the entire schema and all its contents including tables, views, and functions.

# COMMAND ----------

dbutils.widgets.text("catalog", "jdub_demo", "Catalog Name")
dbutils.widgets.text("schema", "panda", "Schema Name")
dbutils.widgets.dropdown("confirm_delete", "NO", ["NO", "YES"], "Confirm Deletion")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
confirm = dbutils.widgets.get("confirm_delete")

print(f"Target: {catalog}.{schema}")
print(f"Confirm delete: {confirm}")

# COMMAND ----------

if confirm == "YES":
    print(f"Cleaning up {catalog}.{schema}...")
    spark.sql(f"DROP VIEW IF EXISTS {catalog}.{schema}.v_daily_store_revenue")
    print("  Dropped view: v_daily_store_revenue")
    spark.sql(f"DROP FUNCTION IF EXISTS {catalog}.{schema}.mask_email")
    print("  Dropped function: mask_email")
    spark.sql(f"DROP FUNCTION IF EXISTS {catalog}.{schema}.region_filter")
    print("  Dropped function: region_filter")
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")
    print(f"  Dropped schema: {catalog}.{schema}")
    print()
    print("Cleanup complete - all demo objects removed.")
else:
    print("Cleanup skipped. Set 'confirm_delete' widget to YES to proceed.")
