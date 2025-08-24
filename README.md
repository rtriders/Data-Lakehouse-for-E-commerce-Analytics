# Data-Lakehouse-for-E-commerce-Analytics

### Overview

This project implements a Data Lakehouse for E-commerce Analytics by unifying the scalability of a data lake with the performance of a data warehouse. Raw transactional data is ingested into Amazon S3 (Bronze Layer), cleaned and transformed into curated Silver and Gold layers using AWS Glue and PySpark. The Gold layer organizes data into dimensional fact and dimension tables (Sales, Customers, Products) following a star schema design, enriched with advanced segmentation such as Customer Lifetime Value (CLV), Product Revenue Contribution, and Frequency Segments. The processed datasets are seamlessly integrated with Amazon Redshift Serverless via Spectrum, enabling near real-time analytics while leveraging cost-efficient Parquet storage in S3. The architecture incorporates data quality checks, schema enforcement, and partitioning to improve reliability, performance, and reduce query costs. Finally, curated datasets power business insights such as identifying top-selling products, high-value customers, and market segments, supporting data-driven decision making for marketing and sales teams.

 ### Architecture

 [Architecture Diagram](https://github.com/rtriders/Data-Lakehouse-for-E-commerce-Analytics/blob/main/Architecture.jpeg?plain=1)


### About E-commerce DataSource

The [Walmart E-commerce DataSet](https://www.kaggle.com/datasets/devarajv88/walmart-sales-dataset) is derived from Walmart’s customer transactions, providing detailed insights into both customer demographics and purchasing behavior. It includes:

1. Unique Identifiers: Customer ID, Product ID
2. Customer Demographics: Gender, Age Group, Occupation (masked), Marital Status
3. Geographic Attributes: City Category, Duration of Stay in Current City
4. Product Information: Product Categories (masked)
5. Transactional Details: Purchase Amounts

This rich dataset enables analysis of customer behavior, product performance, and market segmentation for data-driven decision-making.


### Services Used
 
1. [**Amazon S3**](https://docs.aws.amazon.com/s3/index.html): Amazon Simple Storage Service (S3) is used as the **data lake storage layer**, organizing data into **Bronze (raw), Silver (cleaned), and Gold (curated)** layers. It provides durable, scalable, and cost-effective object storage for transactional and analytical datasets.  
2. [**AWS Glue**](https://docs.aws.amazon.com/glue/index.html): AWS Glue is used to build **ETL pipelines** that clean, transform, and enrich raw data into structured fact and dimension tables. It orchestrates the processing flow from **Bronze → Silver → Gold** layers.  
3. [**PySpark**](https://spark.apache.org/docs/latest/api/python/): PySpark, the Python API for Apache Spark, is used within AWS Glue for **distributed data processing and transformations**. It enables large-scale computation, schema enforcement, and advanced segmentation (e.g., Customer Lifetime Value, Product Revenue Contribution).  
4. [**Amazon Redshift Serverless**](https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-whatis.html): Used as the **analytics warehouse layer**, Redshift Serverless integrates with S3 through Spectrum to query Gold datasets. It supports **dimensional modeling (facts & dimensions)** for BI and reporting.  
5. [**Amazon Redshift Spectrum**](https://docs.aws.amazon.com/redshift/latest/dg/c-using-redshift-spectrum.html): Enables Redshift to directly **query Parquet files stored in Amazon S3 (Gold Layer)** without the need for data loading, reducing storage costs while enabling near real-time analytics.  

  
