import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, create_map
from itertools import chain
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# ---------------------------
# Initialize Glue & Spark
# ---------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

logger.info("Starting Walmart Data Transformation Job")

# ---------------------------
# Read Raw Data from S3 (Bronze Layer)
# ---------------------------
# Replace with your actual S3 path
input_path = "s3://ecommerce-datalake-prod/bronze/walmart_sales.csv"

raw_sales_df = spark.read.csv(input_path, header=True)

# Standardize column names: lowercase & replace spaces with underscores
for c in raw_sales_df.columns:
    raw_sales_df = raw_sales_df.withColumnRenamed(c, c.lower().replace(" ", "_"))

logger.info("Raw Walmart sales data loaded successfully")

# ---------------------------
# Customers Dimension Table
# ---------------------------
customers_dim_df = raw_sales_df.select(
    col("user_id").alias("customer_id"),
    col("gender"),
    col("age"),
    col("occupation"),
    col("city_category"),
    col("stay_in_current_city_years"),
    col("marital_status")
).dropDuplicates(["customer_id"])

# Occupation mapping dictionary
occupation_mapping = {
    0: "Firefighter", 1: "Doctor", 2: "Lawyer", 3: "Teacher", 4: "Engineer",
    5: "Nurse", 6: "Police Officer", 7: "Farmer", 8: "Scientist", 9: "Pilot",
    10: "Chef", 11: "Artist", 12: "Musician", 13: "Writer", 14: "Actor",
    15: "Athlete", 16: "Businessperson", 17: "Politician", 18: "Soldier",
    19: "Driver", 20: "Student"
}
occupation_expr = create_map([lit(x) for x in chain(*occupation_mapping.items())])
customers_dim_df = customers_dim_df.withColumn("occupation_name", occupation_expr[col("occupation")])

# City category mapping
city_mapping = {"A": "Metro", "B": "Suburban", "C": "Rural"}
city_expr = create_map([lit(x) for x in chain(*city_mapping.items())])
customers_dim_df = customers_dim_df.withColumn("city_category_name", city_expr[col("city_category")])

logger.info("Customers dimension table created")

# ---------------------------
# Products Dimension Table
# ---------------------------
products_dim_df = raw_sales_df.select(
    col("product_id"),
    col("product_category")
).dropDuplicates(["product_id"])

# Product category mapping
product_mapping = {
    1: "Electronics", 2: "Clothing", 3: "Home Appliances", 4: "Furniture", 
    5: "Personal Care", 6: "Sports & Outdoor", 7: "Automotive", 
    8: "Food & Beverages", 9: "Toys", 10: "Health & Wellness", 
    11: "Books & Stationery", 12: "Music & Movies", 13: "Gaming", 
    14: "Jewelry & Accessories", 15: "Footwear", 16: "Office Supplies", 
    17: "Pet Supplies", 18: "Gardening & Tools", 19: "Travel & Luggage", 
    20: "Other / Miscellaneous"
}
product_expr = create_map([lit(x) for x in chain(*product_mapping.items())])
products_dim_df = products_dim_df.withColumn("product_name", product_expr[col("product_category")])

logger.info("Products dimension table created")

# ---------------------------
# Sales Fact Table
# ---------------------------
sales_fact_df = raw_sales_df.select(
    col("user_id").alias("customer_id"),
    col("product_id"),
    col("purchase").cast("double").alias("purchase")
)

logger.info("Sales fact table created")

# ---------------------------
# Write to S3 (Silver Layer)
# ---------------------------
output_path = "s3://ecommerce-datalake-prod/silver/"

customers_dim_df.write.mode("overwrite").parquet(output_path + "customers_dim/")
products_dim_df.write.mode("overwrite").parquet(output_path + "products_dim/")
sales_fact_df.write.mode("overwrite").parquet(output_path + "sales_fact/")

logger.info("All transformed tables written to Silver Layer successfully")
