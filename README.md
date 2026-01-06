# Car Rental Data Pipeline - End-to-End Project

This project implements a complete Big Data pipeline for a car rental platform. It automates the extraction of raw data, its transformation using distributed computing, and the generation of business insights for decision-making.

## 🚀 Project Overview
The goal of this project is to process car rental data to analyze fleet performance, customer preferences, and eco-friendly vehicle adoption. 

## 🛠 Architecture & Technologies
The pipeline follows a modern Big Data architecture:

* **Data Ingestion Layer:** Uses **Bash** scripts for automated data retrieval from cloud storage (AWS S3) to **HDFS**, implementing pre-processing validation checks.
* **Transformation Engine:** Leverages **PySpark** for distributed processing, focusing on data quality (handling nulls/empty values) and business-rule filtering, concluding with the automated data load into Apache Hive tables.
* **Workflow Management:** Orchestrated by **Apache Airflow**, ensuring a resilient and schedulable end-to-end pipeline from raw data to final insights.
* **Analytical Storage:** Employs **Apache Hive** as the Data Warehouse layer, organizing processed vehicle data into structured tables for efficient SQL reporting.

## 🛠️ Tech Stack
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-662100?style=for-the-badge&logo=apachehadoop&logoColor=white)
![Hive](https://img.shields.io/badge/Apache%20Hive-FDEE21?style=for-the-badge&logo=apachehive&logoColor=black)
![DBeaver](https://img.shields.io/badge/DBeaver-382923?style=for-the-badge&logo=dbeaver&logoColor=white)

## 📂 Project Structure
```text
├── dags/                            # Airflow DAGs (Parent/Child orchestration)                
│   ├── child_rental.py      
│   └── parent_rental.py
├── reports/                         # Visualizations
│   ├── fuel_type.png
│   ├── total_cars.png
│   └── trips_per_state.png
├── scripts/                         # Core logic and processing
│   ├── ingest-rental.sh             # Bash Ingestion Script                  
│   └── rental_load.py               # PySpark Transformation Engine   
├── sql/                             # SQL Layer
│   ├── ddl/          
│   │   └── hive_tables.hql          # Table schemas and definitions  
│   └── queries/
│       └── analysis_queries.sql     # Business Intelligence queries
├── .gitignore
├── LICENSE
└── README.md
```

---
*Developed by a Data Engineering Specialist.*
