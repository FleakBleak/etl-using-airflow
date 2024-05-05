# ETL Pipeline with Apache Airflow

This project is an ETL (Extract, Transform, Load) pipeline built using Apache Airflow. It automates the extraction of data from an archive, processes various file formats, and consolidates them into a single output. The final result is a transformed CSV file that can be used for further analysis or reporting.

## Pipeline Overview

The ETL pipeline consists of the following steps:
1. Unzips the archive `tolldata.tgz` to access the raw data files.
2. Extracts key fields from multiple file formats (CSV, TSV, and fixed-width).
3. Consolidates the extracted data into a single CSV file, `extracted_data.csv`.
4. Transforms the data by capitalizing all text, creating the final output, `transformed_data.csv`.
