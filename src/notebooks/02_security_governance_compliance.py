# Databricks notebook source

# MAGIC %md
# MAGIC # 🔒 2. Security, Governance & Compliance
# MAGIC **Panda Restaurant Group — Synapse to Databricks Evaluation**
# MAGIC
# MAGIC > 🆚 **Synapse Pain Points:**
# MAGIC > - *"No user interface to integrate security with database objects"*
# MAGIC > - *"Data loss prevention and monitoring gaps"*
# MAGIC > - *"Need strong Entra ID integration for centralized governance"*
# MAGIC
# MAGIC Unity Catalog provides a **unified governance layer** with a visual UI for managing access, column masking, row-level security, and comprehensive audit logging — all integrated with Entra ID.
# MAGIC
# MAGIC **Prerequisites:** Setup notebook runs automatically via `%run`.

# COMMAND ----------

# MAGIC %run ./_resources/00_setup_data_generation

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🛡️ 2.1 Declarative Access Control with Unity Catalog
# MAGIC Grant and revoke permissions at the catalog, schema, table, or column level using standard SQL. Manage permissions visually in the [Catalog Explorer](/explore).

# COMMAND ----------

# Show current grants on a table
spark.sql(f"SHOW GRANTS ON TABLE {catalog}.{schema}.daily_sales").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Granting Permissions to Groups
# MAGIC Unity Catalog uses **Entra ID groups** (synced via SCIM) for access control. Permissions are declarative SQL — no ARM templates or portal clicks required.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant analysts read-only access to operational tables
# MAGIC -- Note: groups must exist in your account console (synced from Entra ID via SCIM)
# MAGIC -- GRANT SELECT ON TABLE daily_sales TO `analysts`;
# MAGIC -- GRANT SELECT ON TABLE stores TO `analysts`;
# MAGIC -- GRANT SELECT ON TABLE menu_items TO `analysts`;
# MAGIC -- GRANT SELECT ON TABLE customer_feedback TO `analysts`;
# MAGIC
# MAGIC -- Data engineers get broader access — SELECT + MODIFY on the entire schema
# MAGIC -- GRANT SELECT, MODIFY ON SCHEMA ${catalog}.${schema} TO `dataengineers`;
# MAGIC
# MAGIC -- Verify what's currently granted
# MAGIC SHOW GRANTS ON SCHEMA ${catalog}.${schema}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔐 2.2 Column Masking — Protect PII Automatically
# MAGIC Customer emails in the feedback table contain PII. We can create a masking function so non-privileged users see redacted values while admins see the original.
# MAGIC
# MAGIC > 🆚 **Contrast with Synapse:** Synapse has no built-in column masking UI. You'd need to implement dynamic data masking with T-SQL or build custom views.

# COMMAND ----------

# MAGIC %md
# MAGIC The pattern is simple: create a UDF that returns the original or masked value based on group membership, then attach it to columns. The `panda_admin` group sees everything; all other users see redacted PII.

# COMMAND ----------

# Create a general-purpose masking function — admins see real data, everyone else sees ****
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {catalog}.{schema}.mask_pii(column_value STRING)
    RETURNS STRING
    RETURN IF(is_member('admins'), column_value, '****')
""")

# Create a more targeted email mask — preserves domain for analysts
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {catalog}.{schema}.mask_email(email STRING)
    RETURNS STRING
    RETURN CASE
      WHEN is_member('admins') THEN email
      ELSE CONCAT(LEFT(email, 2), '****@', SPLIT(email, '@')[1])
    END
""")

# Apply the email mask to the customer_email column
spark.sql(f"""
    ALTER TABLE {catalog}.{schema}.customer_feedback
    ALTER COLUMN customer_email SET MASK {catalog}.{schema}.mask_email
""")

print("Column masks created and applied to customer_feedback.customer_email")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now when non-admin users query this table, emails are automatically masked
# MAGIC -- Admins see: customer_12345@email.com
# MAGIC -- Others see: cu****@email.com
# MAGIC SELECT feedback_id, store_id, rating, review_text, customer_email
# MAGIC FROM customer_feedback
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔑 2.3 Row-Level Security
# MAGIC Restrict data access by region so regional managers only see their own stores' data.
# MAGIC
# MAGIC The pattern: create a UDF that returns `TRUE`/`FALSE` based on group membership, then attach it to a table column. The filter runs transparently on every query — users don't even know it's there.

# COMMAND ----------

# Create a row filter — admins see all regions, others see only their assigned region
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {catalog}.{schema}.region_filter(region_val STRING)
    RETURNS BOOLEAN
    RETURN CASE
      WHEN is_member('admins') THEN TRUE                                      -- admins see everything
      WHEN is_member('west_region') AND region_val = 'West' THEN TRUE         -- west team sees West
      WHEN is_member('southeast_region') AND region_val = 'Southeast' THEN TRUE
      ELSE TRUE  -- For demo: allow all. In production, default to FALSE.
    END
""")

# Apply row filter to the stores table on the 'region' column
spark.sql(f"""
    ALTER TABLE {catalog}.{schema}.stores
    SET ROW FILTER {catalog}.{schema}.region_filter ON (region)
""")

print("Row filter applied to stores table on 'region' column")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📋 2.4 Audit Logging — Who Accessed What, When
# MAGIC Unity Catalog logs every data access event to queryable system tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query audit logs to see recent data access events
# MAGIC SELECT
# MAGIC   event_time,
# MAGIC   user_identity.email AS user_email,
# MAGIC   action_name,
# MAGIC   request_params.full_name_arg AS object_accessed
# MAGIC FROM system.access.audit
# MAGIC WHERE action_name IN ('getTable', 'commandSubmit', 'generateTemporaryTableCredential')
# MAGIC   AND event_time >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🏷️ 2.5 Data Classification with Tags
# MAGIC Classify tables and columns with custom tags (e.g., `pii`, `confidential`, `public`) to enforce governance policies and enable automated compliance scanning.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tag a table with its classification level (requires tag policy permissions)
# MAGIC -- ALTER TABLE customer_feedback SET TAGS ('data_classification' = 'confidential', 'contains_pii' = 'true');
# MAGIC -- ALTER TABLE stores SET TAGS ('data_classification' = 'internal');
# MAGIC -- ALTER TABLE daily_sales SET TAGS ('data_classification' = 'internal');
# MAGIC
# MAGIC -- Tag individual columns for fine-grained governance
# MAGIC -- ALTER TABLE customer_feedback ALTER COLUMN customer_email SET TAGS ('pii_type' = 'email');
# MAGIC -- ALTER TABLE customer_feedback ALTER COLUMN review_text SET TAGS ('pii_type' = 'free_text');
# MAGIC
# MAGIC -- Query existing tags across the schema
# MAGIC SELECT table_name, tag_name, tag_value
# MAGIC FROM information_schema.table_tags
# MAGIC WHERE schema_name = current_schema()
# MAGIC ORDER BY table_name

# COMMAND ----------

# MAGIC %md
# MAGIC > 🔑 **Why this matters:** Tags drive automated policy enforcement — e.g., automatically apply column masks to any column tagged `pii_type = 'email'`, or restrict access to tables tagged `data_classification = 'confidential'`. This replaces manual governance spreadsheets with queryable, programmatic classification.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ 2.6 Security & Governance Summary
# MAGIC
# MAGIC | Capability | Synapse | Databricks |
# MAGIC |-----------|---------|------------|
# MAGIC | Security management UI | No dedicated UI | **Catalog Explorer** — visual grant/revoke, masking, filters |
# MAGIC | Column masking | T-SQL dynamic data masking (limited) | **UDF-based column masks** — fully programmable |
# MAGIC | Row-level security | Row-level security via predicates | **Row filters** — UDF-based, group-aware |
# MAGIC | Audit logging | Azure Monitor integration required | **Built-in system tables** — query with SQL |
# MAGIC | Entra ID integration | Partial | **Full SCIM sync** — groups, users, service principals |
# MAGIC | Data classification | Manual spreadsheets | **Tags on tables & columns** — queryable, automatable |
# MAGIC | Certifications | SOC, ISO, GDPR | SOC 1/2, ISO 27001, GDPR, HIPAA, FedRAMP |
# MAGIC
# MAGIC > **Navigate to:** Open the [Catalog Explorer](/explore), select a table, then click the **Permissions** tab to see the visual security management UI that Synapse lacks.
# MAGIC
# MAGIC ---
