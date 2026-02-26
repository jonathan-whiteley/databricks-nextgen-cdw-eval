# Databricks Next Gen Cloud Data Warehouse - Demo

Databricks evaluation demo comparing Azure Synapse capabilities against the Databricks Lakehouse platform across 7 key pillars.

Deployed as a [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/) with a multi-task job that generates sample data and runs all notebooks end-to-end.

## Notebooks

| # | Notebook | Pillar |
|---|----------|--------|
| 00 | `_resources/00_setup_data_generation` | Setup — creates catalog, schema, and synthetic Panda restaurant data |
| 01 | `01_data_management_quality` | Data management, quality enforcement, schema evolution, lineage |
| 02 | `02_security_governance_compliance` | Unity Catalog access control, column masking, row-level security, audit logs |
| 03 | `03_performance_scalability` | Serverless SQL, caching, adaptive query execution, Photon |
| 04 | `04_cost_model_finops` | Billing system tables, serverless pricing, FinOps visibility |
| 05 | `05_development_ai_ml` | MLflow experiment tracking, feature engineering, model serving |
| 06 | `06_dev_experience_ecosystem` | Multi-language support, SQL editor, Partner Connect, Git integration |
| 07 | `07_operations_reliability` | System tables monitoring, Predictive Optimization, Lakeflow, DR |
| 08 | `_resources/08_bonus_dashboard_genie` | AI/BI Dashboard and Genie Space creation (bonus) |

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) v0.230+
- A Databricks workspace with Unity Catalog enabled
- A configured CLI profile (see [authentication docs](https://docs.databricks.com/dev-tools/cli/authentication.html))
- A serverless SQL warehouse

## Quick Start

### 1. Configure

Open `databricks.yml` and update the values marked with `# ⚠️`:

| Setting | What to change |
|---------|---------------|
| `profile` | Your Databricks CLI profile name (in both targets) |
| `catalog` | Unity Catalog catalog to use |
| `schema` | Schema name (default: `panda`) |
| `deployer_email` | Your Databricks login email |
| `warehouse` | Name of your SQL warehouse |

### 2. Validate

```bash
databricks bundle validate
```

### 3. Deploy

```bash
databricks bundle deploy
```

### 4. Run

```bash
databricks bundle run panda_demo_job
```

This launches a workflow that generates sample data and runs all 8 notebooks. Tasks 01-07 run in parallel after setup completes.

## Project Structure

```
panda-cdw-demo/
├── databricks.yml                          # Bundle config — variables, targets
├── resources/
│   ├── panda_demo_job.yml                  # Job definition with task dependencies
│   └── panda_dashboard.yml                 # AI/BI dashboard resource
└── src/notebooks/
    ├── 01_data_management_quality.py
    ├── 02_security_governance_compliance.py
    ├── 03_performance_scalability.py
    ├── 04_cost_model_finops.py
    ├── 05_development_ai_ml.py
    ├── 06_dev_experience_ecosystem.py
    ├── 07_operations_reliability.py
    └── _resources/
        ├── 00_setup_data_generation.py     # Data generation (runs first)
        ├── 08_bonus_dashboard_genie.py     # Dashboard + Genie setup
        └── 99_cleanup.py                   # Tear-down (manual)
```

## Cleanup

To drop the generated data and remove deployed resources:

```bash
# Run the cleanup notebook manually in the workspace, then:
databricks bundle destroy --auto-approve
```
