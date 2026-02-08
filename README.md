# 🚀 Job Market Data Engineering Pipeline

## 📌 Overview
This project is an **end-to-end Data Engineering pipeline** built using **Databricks, Apache Spark, and Delta Lake**.  
It ingests live job-market data from an external API, processes and transforms the data at scale, and stores analytics-ready tables for SQL, BI, and future machine learning use cases.

The project focuses on **real-world data engineering concepts** such as API ingestion, distributed processing, data modeling, and analytics enablement.

---

## 🎯 Project Purpose
The purpose of this project is to:
- Collect real job-market data from an external source
- Extract and standardize required technical skills
- Build scalable, analytics-ready datasets
- Enable insights into job demand and skill trends

This project helps answer questions like:
- Which technical skills are most in demand?
- How does skill demand vary by job role?
- What technologies should professionals focus on learning?

---

## 🏗️ Architecture
External Job API (Adzuna)
↓
Python API Ingestion
↓
Databricks (Apache Spark)
↓
Delta Lake Tables
↓
SQL / Power BI / Analytics


---

**## 🧰 Tech Stack
- **Python**
- **Databricks**
- **Apache Spark (PySpark)**
- **Delta Lake**
- **SQL**
- **Adzuna Job Search API**

---


---

## 🔄 Pipeline Steps

### Step 1: Data Ingestion
- Job postings are fetched from the Adzuna API using Python
- Multiple job roles are queried (Data Analyst, Data Scientist, Power BI, SQL Analyst)
- Pagination and API limits are handled

### Step 2: Data Processing
- Raw JSON responses are cleaned and structured
- Required skills are extracted from job descriptions
- Data is prepared for distributed processing

### Step 3: Data Storage
- Data is converted into Spark DataFrames
- Tables are stored in **Delta Lake** for reliability and performance
- Normalized tables are created for analytics

### Step 4: Analytics Enablement
- SQL queries are run directly on Delta tables
- Data is ready for BI tools such as Power BI
- The pipeline supports future ML extensions

---

## 🗃️ Data Tables

### 1️⃣ `jobs_adzuna`
Main job-level table containing:
- `job_id`
- `title`
- `company`
- `location`
- `created`
- `search_term`
- `skills` (comma-separated)

### 2️⃣ `jobs_adzuna_skills`
Normalized skills table (one skill per row):
- `job_id`
- `title`
- `company`
- `location`
- `search_term`
- `skill`

This design enables efficient analytics and reporting.

---

## 🔍 Example SQL Analysis

### Top skills in demand
```sql
SELECT
  skill,
  COUNT(*) AS demand
FROM jobs_adzuna_skills
GROUP BY skill
ORDER BY demand DESC
LIMIT 20;

**### Jobs by role

SELECT
  search_term,
  COUNT(DISTINCT job_id) AS job_count
FROM jobs_adzuna
GROUP BY search_term;

### Jobs by location

SELECT
  location,
  COUNT(DISTINCT job_id) AS job_count
FROM jobs_adzuna
GROUP BY location
ORDER BY job_count DESC;
## 📈 Use Cases
Identify in-demand technical skills

Compare skill demand across job roles

Support career upskilling decisions

Enable Power BI dashboards

Serve as a foundation for ML-based trend analysis

## 🚀 Future Enhancements

Automate daily data ingestion

Add salary trend analysis

Build interactive Power BI dashboards

Deploy the pipeline on AWS or GCP

Add machine learning models for demand prediction

