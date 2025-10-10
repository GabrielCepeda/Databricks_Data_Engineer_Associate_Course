# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Module 4 - Data Generation & Catalog Setup
# MAGIC
# MAGIC This setup notebook creates all the required data, tables, and functions needed for the Module 4 production pipeline laboratories. We'll generate realistic GlobalMart retail data directly in the Unity Catalog, avoiding the complexity of DBFS paths.
# MAGIC
# MAGIC ## What we'll create:
# MAGIC - Sample transactional data (sales, inventory, customers)
# MAGIC - Unity Catalog schema and tables
# MAGIC - User-defined functions (UDFs) for data quality
# MAGIC - Monitoring and metrics tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize Environment and Create Catalog/Schema

# COMMAND ----------

def get_user_schema():
    import re

    user_email = spark.sql("SELECT current_user()").collect()[0][0]
    # Extract before '@'
    match = re.search(r'^[^@]+', user_email)
    if match:
        user_name =  match.group(0).replace('.', '_')
        user_schema = "module4_" + user_name
        return user_schema
    
    raise ValueError('User name could not be extracted')

# COMMAND ----------

# Initialize Spark session and imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
import uuid
from datetime import datetime, timedelta
import json

user_schema = get_user_schema()

print("🚀 Initializing environment...")
print("=" * 100)
print(f"🛠️ A new schema: {user_schema} will be created.")

# Create catalog and schema for GlobalMart production data
spark.sql("USE CATALOG sm_training")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {user_schema}")
spark.sql(f"USE SCHEMA {user_schema}")

print(f"✅ Current catalog: {spark.catalog.currentCatalog()}")
print(f"✅ Current schema:  {spark.catalog.currentDatabase()}")
print("=" * 100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Sample Sales Data

# COMMAND ----------

from pyspark.sql.functions import least, greatest, col, lit

# Generate realistic sales transactions
def generate_sales_data(num_records=10000, processing_date=None):
    if processing_date is None:
        processing_date = datetime.now().strftime("%Y-%m-%d")
    
    # Define sample data generators
    stores = [f"STORE_{i:03d}" for i in range(1, 21)]  # 20 stores
    products = [f"PROD_{i:04d}" for i in range(1, 101)]  # 100 products
    customers = [f"CUST_{i:06d}" for i in range(1, 5001)]  # 5000 customers
    payment_methods = ["CREDIT", "DEBIT", "CASH", "MOBILE"]
    
    sales_data = []
    base_time = datetime.strptime(processing_date, "%Y-%m-%d")
    
    for i in range(num_records):
        # Generate transaction with some realistic patterns
        hour = random.gauss(14, 4)  # Peak around 2 PM
        if hour < 0:
            hour = 0
        elif hour > 23:
            hour = 23
        
        transaction_time = base_time + timedelta(
            hours=hour,
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        # Create transaction record
        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "store_id": random.choice(stores),
            "customer_id": random.choice(customers),
            "product_id": random.choice(products),
            "transaction_timestamp": transaction_time,
            "quantity": random.randint(1, 5),
            "unit_price": random.uniform(9.99, 299.99),
            "discount_amount": random.uniform(0, 20),
            "payment_method": random.choice(payment_methods),
            "processing_date": processing_date,
            "is_valid": random.random() > 0.02  # 98% valid records
        }
        
        # Calculate total amount
        transaction["total_amount"] = (transaction["quantity"] * transaction["unit_price"]) - transaction["discount_amount"]
        
        # Add some data quality issues for testing
        if random.random() < 0.01:  # 1% null customer_id
            transaction["customer_id"] = None
        if random.random() < 0.005:  # 0.5% duplicate transactions
            transaction["transaction_id"] = sales_data[-1]["transaction_id"] if sales_data else transaction["transaction_id"]
        
        sales_data.append(transaction)
    
    return sales_data

print("📦 Generating sales data...")

# Generate data for multiple days
all_sales_data = []
for days_ago in range(7):  # Last 7 days of data
    date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    daily_data = generate_sales_data(num_records=5000, processing_date=date)
    all_sales_data.extend(daily_data)
    print(f"  Generated {len(daily_data):,} records for {date}")

# Create DataFrame and write to catalog
sales_df = spark.createDataFrame(all_sales_data)
sales_df.write.mode("overwrite").saveAsTable(f"{user_schema}.raw_sales")

print(f"\n✅ Created {sales_df.count():,} sales records in 'raw_sales' table")

# Show sample data
print("\nSample sales data:")
sales_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate Inventory Data

# COMMAND ----------

# Generate inventory data
def generate_inventory_data():
    stores = [f"STORE_{i:03d}" for i in range(1, 21)]
    products = [f"PROD_{i:04d}" for i in range(1, 101)]
    
    inventory_data = []
    for store in stores:
        for product in products:
            # Generate realistic inventory levels
            current_stock = random.randint(0, 500)
            reorder_point = random.randint(20, 100)
            
            inventory_data.append({
                "store_id": store,
                "product_id": product,
                "current_stock": current_stock,
                "reorder_point": reorder_point,
                "reorder_quantity": random.randint(100, 500),
                "last_restock_date": (datetime.now() - timedelta(days=random.randint(1, 30))).date(),
                "stock_status": "LOW" if current_stock < reorder_point else "ADEQUATE",
                "processing_date": datetime.now().strftime("%Y-%m-%d")
            })
    
    return inventory_data

print("📦 Generating inventory data...")

inventory_df = spark.createDataFrame(generate_inventory_data())
inventory_df.write.mode("overwrite").saveAsTable(f"{user_schema}.raw_inventory")

print(f"✅ Created {inventory_df.count():,} inventory records")

# Show sample data
print("\nSample inventory data:")
inventory_df.show(5)

# Show inventory statistics
print("\nInventory statistics:")
inventory_df.groupBy("stock_status").count().show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Generate Customer Data

# COMMAND ----------

# Generate customer dimension data
def generate_customer_data(num_customers=5000):
    segments = ["Premium", "Regular", "Occasional", "New"]
    regions = ["North", "South", "East", "West", "Central"]
    
    customer_data = []
    for i in range(1, num_customers + 1):
        registration_date = datetime.now() - timedelta(days=random.randint(1, 1095))  # Up to 3 years
        
        customer_data.append({
            "customer_id": f"CUST_{i:06d}",
            "customer_segment": random.choice(segments),
            "customer_region": random.choice(regions),
            "registration_date": registration_date.date(),
            "lifetime_value": random.uniform(100, 10000),
            "last_purchase_date": (datetime.now() - timedelta(days=random.randint(0, 90))).date(),
            "is_active": random.random() > 0.1,  # 90% active
            "processing_date": datetime.now().strftime("%Y-%m-%d")
        })
    
    return customer_data

print("👥 Generating customer data...")

customer_df = spark.createDataFrame(generate_customer_data())
customer_df.write.mode("overwrite").saveAsTable(f"{user_schema}.raw_customers")

print(f"✅ Created {customer_df.count():,} customer records")

# Show sample data
print("\nSample customer data:")
customer_df.show(5)

# Show customer distribution
print("\nCustomer segment distribution:")
customer_df.groupBy("customer_segment").count().orderBy("count", ascending=False).show()

print("\nCustomer region distribution:")
customer_df.groupBy("customer_region").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create User-Defined Functions (UDFs) for Data Quality

# COMMAND ----------

# Register UDFs for data quality checks
from pyspark.sql.functions import udf

print("🔧 Creating User-Defined Functions...")

# Transaction ID validation UDF
def validate_transaction_id(transaction_id):
    if transaction_id is None:
        return False
    try:
        uuid.UUID(transaction_id)
        return True
    except ValueError:
        return False

# Register UDF in catalog
spark.udf.register("validate_transaction_id", validate_transaction_id, BooleanType())
print("✅ Created UDF: validate_transaction_id")

# Amount validation UDF
def validate_amount(amount):
    return amount is not None and amount > 0 and amount < 10000

spark.udf.register("validate_amount", validate_amount, BooleanType())
print("✅ Created UDF: validate_amount")

# Date validation UDF
def validate_date_range(date_value, days_threshold=365):
    if date_value is None:
        return False
    days_diff = (datetime.now().date() - date_value).days
    return 0 <= days_diff <= days_threshold

spark.udf.register("validate_date_range", validate_date_range, BooleanType())
print("✅ Created UDF: validate_date_range")

# Create a permanent function in the catalog (requires appropriate privileges)
try:
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION {user_schema}.calculate_revenue(quantity INT, unit_price DOUBLE, discount DOUBLE)
    RETURNS DOUBLE
    RETURN ROUND((quantity * unit_price) - discount, 2)
    """)
    print("✅ Created SQL function: calculate_revenue")
except:
    print("ℹ️ Note: SQL function creation requires appropriate catalog privileges")

print("\n✅ User-defined functions created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Monitoring and Metrics Tables

# COMMAND ----------

print("📊 Creating monitoring and metrics tables...")

# Create pipeline metrics table for monitoring
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {user_schema}.pipeline_metrics (
    metric_timestamp TIMESTAMP,
    pipeline_name STRING,
    task_name STRING,
    metric_name STRING,
    metric_value DOUBLE,
    metric_unit STRING,
    processing_date DATE,
    run_id STRING
) USING DELTA
PARTITIONED BY (processing_date)
""")
print("✅ Created table: pipeline_metrics")

# Create data quality metrics table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {user_schema}.data_quality_metrics (
    check_timestamp TIMESTAMP,
    table_name STRING,
    check_name STRING,
    check_result STRING,
    failed_records INT,
    total_records INT,
    error_percentage DOUBLE,
    processing_date DATE
) USING DELTA
PARTITIONED BY (processing_date)
""")
print("✅ Created table: data_quality_metrics")

# Create job execution history table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {user_schema}.job_execution_history (
    job_id STRING,
    job_name STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING,
    error_message STRING,
    records_processed BIGINT,
    processing_date DATE
) USING DELTA
PARTITIONED BY (processing_date)
""")
print("✅ Created table: job_execution_history")

# Create alert history table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {user_schema}.alert_history (
    alert_id STRING,
    alert_name STRING,
    severity STRING,
    triggered_at TIMESTAMP,
    resolved_at TIMESTAMP,
    alert_details STRING,
    processing_date DATE
) USING DELTA
PARTITIONED BY (processing_date)
""")
print("✅ Created table: alert_history")

print("\n✅ Monitoring tables created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Empty Bronze, Silver, and Gold Tables

# COMMAND ----------

print("🏗️ Creating Bronze, Silver, and Gold layer tables...")

# Create Bronze layer tables (will be populated by ingestion job)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {user_schema}.bronze_sales (
    transaction_id STRING,
    store_id STRING,
    customer_id STRING,
    product_id STRING,
    transaction_timestamp TIMESTAMP,
    quantity INT,
    unit_price DOUBLE,
    discount_amount DOUBLE,
    total_amount DOUBLE,
    payment_method STRING,
    is_valid BOOLEAN,
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    data_quality_flag STRING,
    bronze_run_id STRING,
    processing_date DATE
) USING DELTA
PARTITIONED BY (processing_date)
""")
print("✅ Created table: bronze_sales")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {user_schema}.bronze_inventory (
    store_id STRING,
    product_id STRING,
    current_stock INT,
    reorder_point INT,
    reorder_quantity INT,
    last_restock_date DATE,
    stock_status STRING,
    stock_alert STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    bronze_run_id STRING,
    processing_date DATE
) USING DELTA
PARTITIONED BY (processing_date)
""")
print("✅ Created table: bronze_inventory")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {user_schema}.bronze_customers (
    customer_id STRING,
    customer_segment STRING,
    customer_region STRING,
    registration_date DATE,
    lifetime_value DOUBLE,
    last_purchase_date DATE,
    is_active BOOLEAN,
    days_since_last_purchase INT,
    customer_status STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    bronze_run_id STRING,
    processing_date DATE
) USING DELTA
PARTITIONED BY (processing_date)
""")
print("✅ Created table: bronze_customers")

# COMMAND ----------

# Create Silver layer tables
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {user_schema}.silver_sales (
    transaction_id STRING,
    store_id STRING,
    customer_id STRING,
    product_id STRING,
    transaction_timestamp TIMESTAMP,
    quantity INT,
    unit_price DOUBLE,
    discount_amount DOUBLE,
    total_amount DOUBLE,
    payment_method STRING,
    customer_segment STRING,
    customer_region STRING,
    lifetime_value DOUBLE,
    customer_status STRING,
    revenue_after_discount DOUBLE,
    discount_percentage DOUBLE,
    is_high_value_transaction BOOLEAN,
    transaction_hour INT,
    transaction_day_of_week INT,
    is_weekend BOOLEAN,
    is_valid BOOLEAN,
    data_quality_score DOUBLE,
    silver_processing_timestamp TIMESTAMP,
    processing_date DATE
) USING DELTA
PARTITIONED BY (processing_date)
""")
print("✅ Created table: silver_sales")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {user_schema}.silver_inventory (
    store_id STRING,
    product_id STRING,
    current_stock INT,
    reorder_point INT,
    reorder_quantity INT,
    last_restock_date DATE,
    stock_status STRING,
    total_stock_by_product BIGINT,
    avg_stock_by_product DOUBLE,
    stock_coverage_days DOUBLE,
    needs_reorder BOOLEAN,
    stock_value DOUBLE,
    silver_processing_timestamp TIMESTAMP,
    processing_date DATE
) USING DELTA
""")
print("✅ Created table: silver_inventory")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {user_schema}.silver_customers (
    customer_id STRING,
    customer_segment STRING,
    customer_region STRING,
    registration_date DATE,
    lifetime_value DOUBLE,
    last_purchase_date DATE,
    is_active BOOLEAN,
    transaction_count BIGINT,
    total_spent DOUBLE,
    avg_transaction_value DOUBLE,
    last_transaction_date TIMESTAMP,
    unique_products_purchased BIGINT,
    unique_stores_visited BIGINT,
    days_since_last_transaction INT,
    churn_risk_score DOUBLE,
    customer_value_segment STRING,
    silver_processing_timestamp TIMESTAMP,
    processing_date DATE
) USING DELTA
""")
print("✅ Created table: silver_customers")

# COMMAND ----------

# Create Gold layer aggregate tables
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {user_schema}.gold_daily_sales_summary (
    summary_date DATE,
    store_id STRING,
    total_transactions BIGINT,
    total_revenue DOUBLE,
    avg_transaction_value DOUBLE,
    unique_customers BIGINT,
    unique_products_sold BIGINT,
    total_units_sold BIGINT,
    total_discounts DOUBLE,
    avg_discount_percentage DOUBLE,
    high_value_transactions BIGINT,
    weekend_revenue DOUBLE,
    weekday_revenue DOUBLE,
    revenue_per_customer DOUBLE,
    items_per_transaction DOUBLE,
    weekend_revenue_ratio DOUBLE,
    top_product_id STRING,
    top_product_revenue DOUBLE,
    processing_timestamp TIMESTAMP,
    processing_date DATE
) USING DELTA
PARTITIONED BY (processing_date)
""")
print("✅ Created table: gold_daily_sales_summary")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {user_schema}.gold_customer_metrics (
    customer_id STRING,
    total_purchases BIGINT,
    total_spent DOUBLE,
    avg_purchase_value DOUBLE,
    first_purchase_date TIMESTAMP,
    last_purchase_date TIMESTAMP,
    days_as_customer INT,
    days_since_last_purchase INT,
    stores_visited BIGINT,
    unique_products BIGINT,
    total_items_purchased BIGINT,
    avg_discount_received DOUBLE,
    purchase_frequency DOUBLE,
    purchase_consistency DOUBLE,
    customer_value_score DOUBLE,
    engagement_score DOUBLE,
    loyalty_score DOUBLE,
    churn_risk_score DOUBLE,
    customer_segment STRING,
    processing_timestamp TIMESTAMP,
    processing_date DATE
) USING DELTA
PARTITIONED BY (processing_date)
""")
print("✅ Created table: gold_customer_metrics")

print("\n✅ Bronze, Silver, and Gold layer tables created!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Summary and Verification

# COMMAND ----------

print("📊 Setup Summary and Verification")
print("=" * 70)

# Verify all tables were created
print(f"\n📋 Tables created in globalmart_prod.{user_schema}:")
print("-" * 50)

tables = spark.sql(f"SHOW TABLES IN {user_schema}").collect()

# Categorize tables
raw_tables = []
bronze_tables = []
silver_tables = []
gold_tables = []
monitoring_tables = []

for table in tables:
    table_name = table.tableName
    
    # Get row count
    try:
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM globalmart_prod.{user_schema}.{table_name}").collect()[0].cnt
    except:
        row_count = 0
    
    table_info = f"  ✓ {table_name:35s} - {row_count:,} records"
    
    if table_name.startswith("raw_"):
        raw_tables.append(table_info)
    elif table_name.startswith("bronze_"):
        bronze_tables.append(table_info)
    elif table_name.startswith("silver_"):
        silver_tables.append(table_info)
    elif table_name.startswith("gold_"):
        gold_tables.append(table_info)
    else:
        monitoring_tables.append(table_info)

# Display organized table list
print("\n🗂️ Raw Data Tables:")
for t in raw_tables:
    print(t)

print("\n🥉 Bronze Layer Tables:")
for t in bronze_tables:
    print(t)

print("\n🥈 Silver Layer Tables:")
for t in silver_tables:
    print(t)

print("\n🥇 Gold Layer Tables:")
for t in gold_tables:
    print(t)

print("\n📊 Monitoring Tables:")
for t in monitoring_tables:
    print(t)

# COMMAND ----------

# Display data statistics
print("\n📈 Data Statistics:")
print("-" * 50)

# Sales data stats
sales_stats = spark.sql(f"""
    SELECT 
        COUNT(DISTINCT processing_date) as days_of_data,
        COUNT(DISTINCT store_id) as stores,
        COUNT(DISTINCT customer_id) as customers,
        COUNT(DISTINCT product_id) as products,
        COUNT(*) as total_transactions,
        ROUND(SUM(total_amount), 2) as total_revenue
    FROM {user_schema}.raw_sales
""").collect()[0]

print(f"  Days of data: {sales_stats['days_of_data']}")
print(f"  Stores: {sales_stats['stores']}")
print(f"  Customers: {sales_stats['customers']:,}")
print(f"  Products: {sales_stats['products']}")
print(f"  Total transactions: {sales_stats['total_transactions']:,}")
print(f"  Total revenue: ${sales_stats['total_revenue']:,.2f}")

print("\n" + "=" * 70)
print("✅ SETUP COMPLETE!")
print("=" * 70)
print("\n📌 Your catalog is ready for the Module 4 production pipeline labs.")
print(f"📌 Database to use: '{user_schema}'")
print("📌 All job notebooks should reference these catalog tables.")
print("\n🎯 Next Steps:")
print("  1. Upload the job notebooks to your workspace")
print("  2. Create the Databricks job using the provided configuration")
print("  3. Run the job and monitor the pipeline execution")
print("  4. Check the monitoring tables for metrics and quality results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Setup Complete!
# MAGIC
# MAGIC Your environment is now ready for Module 4 labs. You have:
# MAGIC
# MAGIC - ✅ **Unity Catalog** configured
# MAGIC - ✅ **35,000+ sales records** across 7 days
# MAGIC - ✅ **2,000 inventory records** for 20 stores
# MAGIC - ✅ **5,000 customer records** with segments and regions
# MAGIC - ✅ **Data quality UDFs** for validation
# MAGIC - ✅ **Empty medallion architecture tables** (Bronze, Silver, Gold)
# MAGIC - ✅ **Monitoring tables** for pipeline metrics
# MAGIC
# MAGIC ### 📚 Important Notes:
# MAGIC
# MAGIC 1. All data is stored in **Unity Catalog** - no DBFS paths needed
# MAGIC 2. The job notebooks will reference `globalmart_prod.{user_schema}` tables
# MAGIC 3. Data includes intentional quality issues for testing (duplicates, nulls)
# MAGIC 4. Monitoring tables will be populated as the pipeline runs
# MAGIC
# MAGIC ### 🚀 Ready to proceed with the production pipeline labs!
