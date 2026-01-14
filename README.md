# Day 6 – Medallion Architecture (Bronze, Silver, Gold)

## Objective
To design and implement a Bronze–Silver–Gold data architecture using Delta Lake for structured data processing.

## Dataset Used
- sale_data
- customer

## Architecture Design
- Bronze Layer: Raw ingestion of sales and customer data with minimal transformation.
- Silver Layer: Cleaned and validated data with duplicates removed and derived columns added.
- Gold Layer: Business-level aggregated data for analytics and reporting.

## Tasks Performed
- Designed a 3-layer medallion architecture
- Built Bronze layer for raw data ingestion
- Built Silver layer for cleaning and validation
- Built Gold layer with business aggregates

## Key Learnings
- Separating raw, cleaned, and aggregated data improves maintainability
- Medallion architecture is a common industry pattern for data pipelines
- Delta Lake works effectively across all three layers

## Tools Used
- Databricks Community Edition
- Apache Spark (PySpark)
- Delta Lake

## Notes
This work is part of the Databricks 14 Days AI Challenge organized by Indian Data Club and Codebasics.
