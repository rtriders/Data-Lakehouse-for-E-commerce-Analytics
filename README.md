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
  
