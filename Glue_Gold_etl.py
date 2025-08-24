import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, sum as _sum, avg, when, round
)
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# ----------------------------------------
# Initialize Spark & Glue Context
# ----------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ----------------------------------------
# Input Paths (Change to your bucket paths)
# ----------------------------------------
sales_fact_silver_path = "s3://ecommerce-datalake-prod/silver/sales_fact/"
customers_dim_silver_path = "s3://ecommerce-datalake-prod/silver/customers_dim/"
product_dim_silver_path = "s3://ecommerce-datalake-prod/silver/products_dim/"

# Output Paths (Change as needed)
dim_customers_gold_path = "s3://ecommerce-datalake-prod/gold/customers_dim/"
dim_products_gold_path = "s3://ecommerce-datalake-prod/gold/products_dim/"
sales_fact_gold_path   = "s3://ecommerce-datalake-prod/gold/sales_fact/"

# ----------------------------------------
# Load Source Data
# ----------------------------------------
sales_fact_df = spark.read.parquet(sales_fact_silver_path)
customers_dim_df = spark.read.parquet(customers_dim_silver_path)
products_dim_df = spark.read.parquet(product_dim_silver_path)

# ==================================================================================
# 1. Customer Dimension (Gold Layer)
# ==================================================================================
dim_customers_gold = (
    sales_fact_df
    .join(customers_dim_df, "customer_id", "left")
    .join(products_dim_df, "product_id", "left")
    .groupBy(
        "customer_id", "gender", "age", "occupation", 
        "occupation_name", "city_category", "city_category_name",
        "stay_in_current_city_years", "marital_status"
    )
    .agg(
        countDistinct("product_id").alias("total_products_bought"),
        _sum("purchase").alias("total_spent")
    )
)

# Add Age Group
dim_customers_gold = dim_customers_gold.withColumn(
    "age_group",
    when(col("age").isin("0-17", "18-25"), "Youth")
    .when(col("age").isin("26-35", "36-45"), "Adult")
    .when(col("age").isin("46-50", "51-55", "55+"), "Senior")
    .otherwise("Unknown")
)

# Add Customer Segment based on total spend
mean_spent = dim_customers_gold.select(avg("total_spent")).collect()[0][0]
dim_customers_gold = dim_customers_gold.withColumn(
    "customer_segment",
    when(col("total_spent") >= mean_spent * 2, "VIP")       # Top spenders
    .when(col("total_spent") >= mean_spent, "Loyal")        # Above average
    .otherwise("Regular")                                   # Below average
)

# Add Frequency Segment based on number of products bought
dim_customers_gold = dim_customers_gold.withColumn(
    "frequency_segment",
    when(col("total_products_bought") > 100, "High")
    .when((col("total_products_bought") > 30) & (col("total_products_bought") <= 100), "Medium")
    .otherwise("Low")
)

# ==================================================================================
# 2. Product Dimension (Gold Layer)
# ==================================================================================
dim_products_gold = (
    sales_fact_df
    .join(products_dim_df, "product_id", "left")
    .groupBy("product_id", "product_category", "product_name")
    .agg(
        countDistinct("customer_id").alias("total_customers_bought"),
        _sum("purchase").alias("total_revenue"),
    )
    .withColumn(
        "avg_revenue_per_customer",
        round((col("total_revenue") / col("total_customers_bought")), 2)
    )
)

# Add Product Segment relative to average revenue
avg_revenue_val = dim_products_gold.agg(avg("total_revenue")).collect()[0][0]
dim_products_gold = dim_products_gold.withColumn(
    "product_segment",
    when(col("total_revenue") > avg_revenue_val, "Top Seller").otherwise("Regular")
)

# Add Revenue Contribution %
total_revenue_all = dim_products_gold.agg(_sum("total_revenue")).collect()[0][0]
dim_products_gold = dim_products_gold.withColumn(
    "revenue_contribution_pct",
    round((col("total_revenue") / total_revenue_all) * 100, 2)
)

# ==================================================================================
# 3. Sales Fact (Gold Layer)
# ==================================================================================
sales_fact_gold = (
    sales_fact_df
    .join(dim_customers_gold.select("customer_id", "customer_segment"), "customer_id", "left")
    .join(dim_products_gold.select("product_id", "product_segment"), "product_id", "left")
    .groupBy("customer_id", "product_id", "product_segment", "customer_segment")
    .agg(
        _sum("purchase").alias("total_spent")
    )
)

# ==================================================================================
# Write Outputs to S3 (Parquet)
# ==================================================================================
dim_customers_gold.write.mode("overwrite").parquet(dim_customers_gold_path)
dim_products_gold.write.mode("overwrite").parquet(dim_products_gold_path)
sales_fact_gold.write.mode("overwrite").parquet(sales_fact_gold_path)

print("✅ Gold layer ETL completed successfully!")
