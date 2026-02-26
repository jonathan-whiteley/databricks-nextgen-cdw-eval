# Databricks notebook source

# MAGIC %md
# MAGIC # Panda Restaurant Group — Setup & Data Generation
# MAGIC **Synapse to Databricks Evaluation**
# MAGIC
# MAGIC This notebook creates the catalog, schema, and synthetic data tables used across all demo notebooks.
# MAGIC It is **idempotent** — if tables already exist, data generation is skipped.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 Demo Notebooks
# MAGIC 1. 📊 Data Management & Data Quality — `01_data_management_quality`
# MAGIC 2. 🔒 Security, Governance & Compliance — `02_security_governance_compliance`
# MAGIC 3. ⚡ Performance & Scalability — `03_performance_scalability`
# MAGIC 4. 💰 Cost Model & FinOps — `04_cost_model_finops`
# MAGIC 5. 🤖 Development (AI/ML) — `05_development_ai_ml`
# MAGIC 6. 🔗 Development Experience, Ecosystem & Integration — `06_dev_experience_ecosystem`
# MAGIC 7. 🔄 Operations & Reliability — `07_operations_reliability`
# MAGIC 8. ✨ (BONUS) AI/BI Dashboard & Genie Space — `08_bonus_dashboard_genie`
# MAGIC
# MAGIC 🧹 **Cleanup:** `_resources/99_cleanup`
# MAGIC
# MAGIC **Prerequisites:** Unity Catalog-enabled workspace, serverless compute or a cluster with Databricks Runtime 15.4+

# COMMAND ----------

# Notebook parameters — customize catalog and schema names
dbutils.widgets.text("catalog", "jdub_demo", "Catalog Name")
dbutils.widgets.text("schema", "panda", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Using catalog: {catalog}")
print(f"Using schema:  {schema}")

# COMMAND ----------

# Create catalog and schema for the demo
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"USE SCHEMA {schema}")

print(f"Catalog '{catalog}' and schema '{schema}' ready.")

# COMMAND ----------

# Idempotency check — skip data generation if all tables already exist
_tables_needed = ["stores", "menu_items", "daily_sales", "customer_feedback"]
_setup_complete = all(spark.catalog.tableExists(t) for t in _tables_needed)

if _setup_complete:
    print(f"Setup already complete — all tables exist in {catalog}.{schema}. Skipping data generation.")
else:
    print(f"Tables missing — generating synthetic data in {catalog}.{schema}...")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import random

if not _setup_complete:
    # --- Stores ---
    regions = ["West", "Southwest", "Midwest", "Southeast", "Northeast"]
    states_by_region = {
        "West": ["CA", "WA", "OR", "NV", "AZ", "CO", "UT"],
        "Southwest": ["TX", "NM", "OK"],
        "Midwest": ["IL", "OH", "MI", "IN", "WI", "MN", "MO"],
        "Southeast": ["FL", "GA", "NC", "SC", "VA", "TN"],
        "Northeast": ["NY", "NJ", "PA", "MA", "CT", "MD"],
    }
    store_types = ["Dine-In", "Express", "Mall", "Drive-Thru"]

    store_rows = []
    store_id = 1
    for region, states in states_by_region.items():
        for state in states:
            num_stores = random.randint(3, 12)
            for _ in range(num_stores):
                store_rows.append((
                    store_id,
                    f"Store #{store_id:04d}",
                    region,
                    state,
                    random.choice(store_types),
                    round(random.uniform(28.0, 47.0), 4),
                    round(random.uniform(-122.0, -71.0), 4),
                    f"{random.randint(2005, 2023)}-{random.randint(1,12):02d}-01",
                ))
                store_id += 1

    stores_schema = StructType([
        StructField("store_id", IntegerType()),
        StructField("store_name", StringType()),
        StructField("region", StringType()),
        StructField("state", StringType()),
        StructField("store_type", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("open_date", StringType()),
    ])

    df_stores = spark.createDataFrame(store_rows, schema=stores_schema) \
        .withColumn("open_date", F.col("open_date").cast("date"))

    df_stores.write.mode("overwrite").saveAsTable("stores")
    print(f"stores table created: {df_stores.count()} rows")

# COMMAND ----------

if not _setup_complete:
    # --- Menu Items ---
    menu_data = [
        ("Orange Chicken", "Entree", 2.10, 5.50),
        ("Beijing Beef", "Entree", 2.30, 5.50),
        ("Broccoli Beef", "Entree", 1.90, 5.50),
        ("Kung Pao Chicken", "Entree", 2.00, 5.50),
        ("Honey Walnut Shrimp", "Entree", 3.20, 6.50),
        ("Grilled Teriyaki Chicken", "Entree", 2.40, 5.75),
        ("Mushroom Chicken", "Entree", 2.10, 5.50),
        ("SweetFire Chicken Breast", "Entree", 2.20, 5.50),
        ("String Bean Chicken Breast", "Entree", 1.95, 5.50),
        ("Black Pepper Angus Steak", "Entree", 3.80, 7.25),
        ("Honey Sesame Chicken", "Entree", 2.15, 5.50),
        ("Firecracker Shrimp", "Entree", 3.10, 6.50),
        ("Chow Mein", "Side", 0.80, 4.00),
        ("Fried Rice", "Side", 0.85, 4.00),
        ("White Steamed Rice", "Side", 0.30, 3.50),
        ("Super Greens", "Side", 1.00, 4.00),
        ("Chicken Egg Roll", "Appetizer", 0.50, 2.00),
        ("Veggie Spring Roll", "Appetizer", 0.45, 2.00),
        ("Cream Cheese Rangoon", "Appetizer", 0.55, 2.50),
        ("Apple Pie Roll", "Dessert", 0.40, 2.00),
        ("Fountain Drink (Sm)", "Beverage", 0.25, 2.10),
        ("Fountain Drink (Md)", "Beverage", 0.30, 2.60),
        ("Fountain Drink (Lg)", "Beverage", 0.35, 3.10),
        ("Bottled Water", "Beverage", 0.20, 2.00),
        ("Bowl", "Combo", 3.50, 8.50),
        ("Plate", "Combo", 4.80, 10.50),
        ("Bigger Plate", "Combo", 6.00, 12.50),
        ("Family Feast", "Combo", 18.00, 43.00),
        ("Panda Cub Meal", "Kids", 3.00, 6.50),
    ]

    menu_schema = StructType([
        StructField("item_name", StringType()),
        StructField("category", StringType()),
        StructField("unit_cost", DoubleType()),
        StructField("unit_price", DoubleType()),
    ])

    df_menu = spark.createDataFrame(
        [(i + 1, *row) for i, row in enumerate(menu_data)],
        schema=StructType([StructField("item_id", IntegerType())] + menu_schema.fields),
    )

    df_menu.write.mode("overwrite").saveAsTable("menu_items")
    print(f"menu_items table created: {df_menu.count()} rows")

# COMMAND ----------

if not _setup_complete:
    # --- Daily Sales (generates ~500K rows spanning 2 years) ---
    from pyspark.sql.functions import explode, sequence, lit, rand, floor, ceil, when, dayofweek

    store_ids = [r.store_id for r in spark.table("stores").select("store_id").collect()]
    item_ids = [r.item_id for r in spark.table("menu_items").select("item_id").collect()]

    # Generate date range
    dates_df = spark.sql("""
        SELECT explode(sequence(DATE'2023-01-01', DATE'2024-12-31', INTERVAL 1 DAY)) AS sale_date
    """)

    # Cross join stores x dates, then sample items
    df_sales = (
        dates_df
        .crossJoin(spark.createDataFrame([(sid,) for sid in store_ids], ["store_id"]))
        .withColumn("rand_val", rand())
        # Each store sells 3-8 items per day
        .withColumn("num_items", floor(rand() * 6 + 3).cast("int"))
    )

    # Explode to individual transactions
    df_sales = (
        df_sales
        .withColumn("item_idx", explode(sequence(lit(1), F.col("num_items"))))
        .withColumn("item_id", (floor(rand() * len(item_ids)) + 1).cast("int"))
        .withColumn("quantity", (floor(rand() * 20) + 1).cast("int"))
        # Weekend boost
        .withColumn("quantity", when(dayofweek("sale_date").isin(1, 7), ceil(F.col("quantity") * 1.3)).otherwise(F.col("quantity")).cast("int"))
        .withColumn("transaction_id", F.monotonically_increasing_id())
        .select("transaction_id", "sale_date", "store_id", "item_id", "quantity")
    )

    df_sales.write.mode("overwrite").saveAsTable("daily_sales")
    row_count = spark.table("daily_sales").count()
    print(f"daily_sales table created: {row_count:,} rows")

# COMMAND ----------

if not _setup_complete:
    # --- Customer Feedback ---
    positive_reviews = [
        "Love the Orange Chicken, always crispy and flavorful!",
        "Quick service even during lunch rush. Great for takeout.",
        "Best value combo meals. Our family favorite on weekends.",
        "Clean restaurant, friendly staff. Will come back again!",
        "The new Honey Sesame Chicken is amazing. Highly recommend.",
        "Drive-thru was fast and order was perfect.",
        "Consistent quality every time we visit this location.",
        "Great portion sizes for the price. Kids love the cub meal.",
    ]
    negative_reviews = [
        "Food was cold when we received it. Disappointing.",
        "Long wait time during peak hours. Needs more staff.",
        "Chow mein was overcooked and soggy this time.",
        "Order was wrong - missing an entree from our plate.",
        "Parking lot was full, had to circle multiple times.",
        "Prices have gone up but portions seem smaller.",
    ]
    neutral_reviews = [
        "Decent food, nothing special. Average experience.",
        "It's okay for fast food. Standard quality.",
        "Convenient location but the dining area was crowded.",
    ]

    store_ids = [r.store_id for r in spark.table("stores").select("store_id").collect()]

    all_reviews = (
        [(r, random.uniform(4.0, 5.0)) for r in positive_reviews] * 600
        + [(r, random.uniform(1.0, 2.5)) for r in negative_reviews] * 250
        + [(r, random.uniform(2.5, 4.0)) for r in neutral_reviews] * 200
    )
    random.shuffle(all_reviews)

    feedback_rows = []
    for i, (review, rating) in enumerate(all_reviews[:10000]):
        feedback_rows.append((
            i + 1,
            random.choice(store_ids),
            f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            round(rating, 1),
            review,
            f"customer_{random.randint(10000, 99999)}@email.com",
        ))

    feedback_schema = StructType([
        StructField("feedback_id", IntegerType()),
        StructField("store_id", IntegerType()),
        StructField("feedback_date", StringType()),
        StructField("rating", DoubleType()),
        StructField("review_text", StringType()),
        StructField("customer_email", StringType()),
    ])

    df_feedback = spark.createDataFrame(feedback_rows, schema=feedback_schema) \
        .withColumn("feedback_date", F.col("feedback_date").cast("date"))

    df_feedback.write.mode("overwrite").saveAsTable("customer_feedback")
    print(f"customer_feedback table created: {df_feedback.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Data Generation Complete
# MAGIC
# MAGIC | Table | Rows | Description |
# MAGIC |-------|------|-------------|
# MAGIC | `stores` | ~200 | Store locations across 5 US regions |
# MAGIC | `menu_items` | 29 | Menu items with cost and price |
# MAGIC | `daily_sales` | ~500K | 2 years of daily transactions |
# MAGIC | `customer_feedback` | 10,000 | Customer reviews with ratings and PII |
# MAGIC
# MAGIC All data lives in the configured catalog and schema — a Unity Catalog-governed namespace.
# MAGIC
# MAGIC ---
