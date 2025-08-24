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


### ⚙️ Services Used
 
1. [**Amazon S3**](https://docs.aws.amazon.com/s3/index.html): Amazon Simple Storage Service (S3) is used as the **data lake storage layer**, organizing data into **Bronze (raw), Silver (cleaned), and Gold (curated)** layers. It provides durable, scalable, and cost-effective object storage for transactional and analytical datasets.  
2. [**AWS Glue**](https://docs.aws.amazon.com/glue/index.html): AWS Glue is used to build **ETL pipelines** that clean, transform, and enrich raw data into structured fact and dimension tables. It orchestrates the processing flow from **Bronze → Silver → Gold** layers.  
3. [**PySpark**](https://spark.apache.org/docs/latest/api/python/): PySpark, the Python API for Apache Spark, is used within AWS Glue for **distributed data processing and transformations**. It enables large-scale computation, schema enforcement, and advanced segmentation (e.g., Customer Lifetime Value, Product Revenue Contribution).  
4. [**Amazon Redshift Serverless**](https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-whatis.html): Used as the **analytics warehouse layer**, Redshift Serverless integrates with S3 through Spectrum to query Gold datasets. It supports **dimensional modeling (facts & dimensions)** for BI and reporting.  
5. [**Amazon Redshift Spectrum**](https://docs.aws.amazon.com/redshift/latest/dg/c-using-redshift-spectrum.html): Enables Redshift to directly **query Parquet files stored in Amazon S3 (Gold Layer)** without the need for data loading, reducing storage costs while enabling near real-time analytics.

 

### 🚀 Execution Flow  

1. **Data Ingestion (Bronze Layer)**  
   - Raw e-commerce transactional data (customer, product, sales) is ingested and stored in **Amazon S3 (Bronze Layer)** in its original format.  

2. **Data Transformation (Silver Layer)**  
   - **AWS Glue with PySpark** processes raw data by cleaning, validating, and standardizing it.  
   - The transformed data is stored in **Amazon S3 (Silver Layer)** in partitioned Parquet format for efficient querying.  

3. **Data Modeling (Gold Layer)**  
   - Further transformations in Glue organize data into **fact (Sales) and dimension (Customer, Product)** tables following a **Star Schema**.  
   - Advanced business segmentations such as **Customer Lifetime Value (CLV), Product Revenue Contribution, and Frequency Segments** are applied.  
   - Final curated data is stored in **Amazon S3 (Gold Layer)**.  

4. **Analytics Integration**  
   - **Amazon Redshift Spectrum** is used to directly query Gold Layer tables in S3.  
   - **Amazon Redshift Serverless** serves as the **analytics warehouse**, enabling scalable and interactive querying.  

5. **Business Insights & Reporting**  
   - Curated datasets are exposed for **BI tools** (e.g., Amazon QuickSight / Power BI).  
   - Stakeholders can generate insights such as **Top-Selling Products, High-Value Customers, and Market Segments**, driving data-driven decision making.  
  

### 💰 Cost Optimization

1. Partitioned Parquet Storage in S3 reduces query scan costs.
2. Redshift Spectrum queries data directly from S3 → avoids duplicating storage.
3. Serverless Redshift scales automatically → no fixed cluster costs.
4. Schema enforcement in Glue ensures data consistency and avoids costly reprocessing.
5. Eliminated need for Glue Crawler by defining schema directly → cost savings.
