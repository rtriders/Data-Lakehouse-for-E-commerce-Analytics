# Data-Lakehouse-for-E-commerce-Analytics

### Overview

This project implements a Data Lakehouse for E-commerce Analytics by unifying the scalability of a data lake with the performance of a data warehouse. Raw transactional data is ingested into Amazon S3 (Bronze Layer), cleaned and transformed into curated Silver and Gold layers using AWS Glue and PySpark. The Gold layer organizes data into dimensional fact and dimension tables (Sales, Customers, Products) following a star schema design, enriched with advanced segmentation such as Customer Lifetime Value (CLV), Product Revenue Contribution, and Frequency Segments. The processed datasets are seamlessly integrated with Amazon Redshift Serverless via Spectrum, enabling near real-time analytics while leveraging cost-efficient Parquet storage in S3. The architecture incorporates data quality checks, schema enforcement, and partitioning to improve reliability, performance, and reduce query costs. Finally, curated datasets power business insights such as identifying top-selling products, high-value customers, and market segments, supporting data-driven decision making for marketing and sales teams.

 ### Architecture

  ![Architecture Diagram](https://github.com/rtriders/Data-Lakehouse-for-E-commerce-Analytics/blob/main/Architecture.png)


  
