# Cloud data Engineering Project

## Overview
Overview
This project demonstrates a real-world cloud data engineering pipeline orchestrated with Apache Airflow (Amazon MWAA) and integrated with multiple AWS services including Amazon S3 and Amazon Redshift Serverless.
The system is designed to simulate how modern data teams build, validate, transform, and load data into a cloud data warehouse for analytics and reporting. Streaming music data is ingested from Amazon S3, processed using Python and Pandas within Airflow tasks, and then stored in Redshift where Key Performance Indicators (KPIs) are calculated and maintained.

## Steps involved
Create 2 S3 buckets 
Set up Redshifts
Create Airflow environment

The repository is organized into four main directories:

1. airflow_dag/ – Contains Airflow DAGs for scheduling and running KPI calculations.  
2. redshift/ – Contains SQL scripts for creating tables, inserting data, and other operations on Redshift.  
3. local_dev/ – Contains Jupyter notebooks and Python scripts for local development, testing, and transformation of data using Pandas.  
4. data/ – Contains raw and processed data used for local testing.  

Additionally, the project includes a `requirements.txt` file listing all Python dependencies.

---

Estimated Time
Between 60 minutes to 90 mins 

Cost 
Less than $4 if you complete the projects and delete immediately

## Directory Structure

