# Databricks notebook source

# MAGIC %md
# MAGIC # 🤖 5. Development — AI/ML
# MAGIC **Panda Restaurant Group — Synapse to Databricks Evaluation**
# MAGIC
# MAGIC > 🆚 *"Limited ML integration" | "No AI integration" | "Future needs: ML and AI integration"*
# MAGIC
# MAGIC Databricks provides a **fully integrated ML and AI platform** — from training models with MLflow experiment tracking, to deploying them as serverless endpoints, to calling LLMs directly from SQL with `ai_query()`. No separate Azure ML subscription or external tooling required.
# MAGIC
# MAGIC **Prerequisites:** Requires `01_data_management_quality` to have been run (creates the `v_daily_store_revenue` view used for ML training).

# COMMAND ----------

# MAGIC %run ./_resources/00_setup_data_generation

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧪 5.1 ML with MLflow — Train and Track a Demand Forecasting Model
# MAGIC Train a simple model to predict daily store revenue using our sales data. MLflow automatically tracks the experiment, parameters, metrics, and model artifacts — all governed by Unity Catalog.

# COMMAND ----------

# MAGIC %pip install mlflow scikit-learn --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Re-establish parameters after Python restart
# (%run state is lost after restartPython)
dbutils.widgets.text("catalog", "jdub_demo", "Catalog Name")
dbutils.widgets.text("schema", "panda", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

import mlflow
import mlflow.sklearn
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
import pandas as pd

mlflow.set_registry_uri("databricks-uc")

# Prepare training data from our aggregation view
train_df = spark.sql(f"""
    SELECT
      store_id,
      MONTH(sale_date) AS month,
      DAYOFWEEK(sale_date) AS day_of_week,
      total_units,
      transaction_count,
      total_revenue
    FROM {catalog}.{schema}.v_daily_store_revenue
""").toPandas()

X = train_df[["store_id", "month", "day_of_week", "total_units", "transaction_count"]]
y = train_df["total_revenue"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train with MLflow tracking
mlflow.set_experiment(f"/Users/{spark.sql('SELECT current_user()').first()[0]}/panda_demand_forecast")

with mlflow.start_run(run_name="demand_forecast_gbr") as run:
    model = GradientBoostingRegressor(n_estimators=100, max_depth=5, learning_rate=0.1, random_state=42)
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    mae = mean_absolute_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)

    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 5)
    mlflow.log_param("learning_rate", 0.1)
    mlflow.log_metric("mae", mae)
    mlflow.log_metric("r2_score", r2)

    mlflow.sklearn.log_model(
        model,
        artifact_path="demand_model",
        input_example=X_train.head(5),
    )

    print(f"Model trained and logged to MLflow")
    print(f"  Run ID:   {run.info.run_id}")
    print(f"  MAE:      ${mae:,.2f}")
    print(f"  R-squared: {r2:.4f}")

# COMMAND ----------

# Visualize model performance — Predicted vs Actual Revenue
import plotly.express as px

results_df = pd.DataFrame({"Actual Revenue": y_test.values, "Predicted Revenue": predictions})
fig = px.scatter(
    results_df, x="Actual Revenue", y="Predicted Revenue",
    title=f"Demand Forecast: Predicted vs Actual Daily Revenue (R2 = {r2:.3f})",
    labels={"Actual Revenue": "Actual Revenue ($)", "Predicted Revenue": "Predicted Revenue ($)"},
    opacity=0.5,
)
# Add perfect-prediction reference line
min_val = min(y_test.min(), predictions.min())
max_val = max(y_test.max(), predictions.max())
fig.add_shape(type="line", x0=min_val, y0=min_val, x1=max_val, y1=max_val,
              line=dict(color="red", dash="dash"))
fig.update_layout(width=800, height=500)
fig.show()

# COMMAND ----------

# Feature importance — what drives daily store revenue?
import plotly.express as px
import pandas as pd

feature_names = X_train.columns.tolist()
importances = model.feature_importances_
fi_df = pd.DataFrame({"Feature": feature_names, "Importance": importances}).sort_values("Importance", ascending=True)

fig = px.bar(
    fi_df, x="Importance", y="Feature", orientation="h",
    title="Feature Importance — What Drives Daily Store Revenue?",
    labels={"Importance": "Relative Importance", "Feature": ""},
)
fig.update_layout(template="plotly_white", width=700, height=350)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 **From Notebook to Production in One Click:**
# MAGIC > This model is already tracked in MLflow. To deploy it as a real-time API endpoint:
# MAGIC > 1. Open the [Experiments](/ml/experiments) page and find this run
# MAGIC > 2. Click **Register Model** to add it to the Unity Catalog model registry
# MAGIC > 3. Deploy to a [Model Serving endpoint](/ml/endpoints) — serverless, auto-scaling, governed
# MAGIC >
# MAGIC > The entire lifecycle — training, tracking, registration, deployment, monitoring — stays within the Databricks platform. No separate Azure ML subscription required.

# COMMAND ----------

# MAGIC %md
# MAGIC > 🔑 **What just happened:**
# MAGIC > - We trained a scikit-learn model on our restaurant sales data
# MAGIC > - MLflow automatically logged parameters, metrics, and the model artifact
# MAGIC > - The model is versioned and traceable in the **MLflow Experiment UI**
# MAGIC > - From here, we can register it to Unity Catalog and deploy to a serving endpoint with one click
# MAGIC >
# MAGIC > **Navigate to:** Open the [Experiments](/ml/experiments) page to see the tracked run, compare models, and register the best one.
# MAGIC >
# MAGIC > 🆚 **Contrast with Synapse:** ML requires a separate Azure ML workspace, separate compute, separate credentials, and manual model management. Databricks keeps everything in one platform.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🤖 5.2 AI Functions — Call LLMs Directly from SQL with `ai_query()`
# MAGIC Use Foundation Model APIs and `ai_query()` to run AI tasks (sentiment analysis, summarization, classification) directly in SQL — no Python, no API keys, no external services.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sentiment analysis on customer reviews using a Foundation Model endpoint
# MAGIC -- ai_query() calls a model serving endpoint directly from SQL
# MAGIC SELECT
# MAGIC   feedback_id,
# MAGIC   rating,
# MAGIC   review_text,
# MAGIC   ai_query(
# MAGIC     'databricks-claude-sonnet-4-5',
# MAGIC     CONCAT(
# MAGIC       'Classify the sentiment of this restaurant review as POSITIVE, NEGATIVE, or NEUTRAL. ',
# MAGIC       'Respond with only one word. Review: ', review_text
# MAGIC     )
# MAGIC   ) AS ai_sentiment
# MAGIC FROM customer_feedback
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Generate actionable insights from negative reviews
# MAGIC SELECT
# MAGIC   ai_query(
# MAGIC     'databricks-claude-sonnet-4-5',
# MAGIC     CONCAT(
# MAGIC       'You are a restaurant operations analyst. Based on these negative customer reviews, ',
# MAGIC       'provide 3 specific actionable recommendations for the restaurant manager. ',
# MAGIC       'Be concise. Reviews: ',
# MAGIC       reviews
# MAGIC     )
# MAGIC   ) AS ai_recommendations
# MAGIC FROM (
# MAGIC   SELECT CONCAT_WS(' | ', COLLECT_LIST(review_text)) AS reviews
# MAGIC   FROM customer_feedback
# MAGIC   WHERE rating < 2.5
# MAGIC   LIMIT 20
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ 5.3 AI/ML Summary
# MAGIC
# MAGIC | Capability | Synapse | Databricks |
# MAGIC |-----------|---------|------------|
# MAGIC | ML experiment tracking | Requires Azure ML workspace | **MLflow** — built-in, governed by Unity Catalog |
# MAGIC | Model registry | Azure ML Model Registry (separate) | **Unity Catalog Model Registry** — same governance as data |
# MAGIC | Model serving | Azure ML Endpoints (separate service) | **[Serverless Model Serving](/ml/endpoints)** — one-click deploy |
# MAGIC | LLM access from SQL | Not available | **`ai_query()`** — call any model from SQL |
# MAGIC | Foundation Models | Requires Azure OpenAI (separate) | **Foundation Model APIs** — built into the platform |
# MAGIC | Feature engineering | Manual | **Feature Store** in Unity Catalog |
# MAGIC | GPU compute | Separate config | Seamless GPU clusters alongside CPU |
# MAGIC
# MAGIC > 🔑 **Key Takeaway:** Databricks unifies data, ML, and AI on one platform. No separate Azure ML subscription, no credential juggling, no data movement between services. Train a model on your data, deploy it, and call it from SQL — all governed by Unity Catalog.
# MAGIC
# MAGIC ---
