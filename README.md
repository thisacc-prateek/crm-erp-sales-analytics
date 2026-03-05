# CRM & ERP Sales Analytics Pipeline
![](High%20Level%20Architecture/High%20Level%20Architecture.png)

## Overview

This project demonstrates how raw operational data from **CRM and ERP systems** can be transformed into **analytics-ready datasets** for business intelligence and reporting.

The objective of this project is to design a structured analytics pipeline that converts raw business data into a **dimensional data model optimized for analysis and dashboarding**.

The pipeline follows a **layered architecture inspired by the Medallion Model**, ensuring that data progresses through stages of refinement before reaching the final analytics layer.

---

# Project Architecture

The architecture organizes data into three logical layers that progressively improve data quality and analytical usability.

## Bronze Layer — Raw Data

The Bronze layer stores data **exactly as it is received from the source systems**.

**Purpose**

- Preserve original data
- Maintain raw records for traceability
- Serve as the initial ingestion layer

**Characteristics**

- Raw CSV ingestion
- No transformations applied
- Full batch loading

**Example Tables**

```
crm_sales_details
crm_cust_info
crm_prd_info
erp_cust_az12
erp_loc_a101
erp_px_cat_g1v2
```

---

## Silver Layer — Cleaned & Standardized Data

The Silver layer focuses on **data quality improvement and standardization**.

**Transformations performed**

- Data cleaning
- Standardization of column formats
- Handling missing values
- Data normalization
- Creation of derived attributes
- Integration of CRM and ERP datasets

The goal of this layer is to create **consistent and reliable datasets for analytics**.

---

## Gold Layer — Analytics Ready Data

The Gold layer contains **business-ready datasets optimized for analytics and reporting**.

This layer implements **dimensional modeling techniques**, organizing data into **fact and dimension tables**.

**Key Outputs**

```
fact_sales
dim_customers
dim_products
```

This structure enables efficient querying and integration with BI tools.

---

# Data Sources

The project integrates data from two enterprise systems.

## CRM System

The CRM system provides customer and sales-related information.

**Tables**

```
crm_sales_details
crm_cust_info
crm_prd_info
```

Data captured includes:

- sales transactions  
- customer details  
- product information  

---

## ERP System

The ERP system provides operational and classification data.

**Tables**

```
erp_cust_az12
erp_loc_a101
erp_px_cat_g1v2
```

Data captured includes:

- customer classifications  
- geographic information  
- product categories  

---

# Data Preparation Process

The pipeline follows a **load-first transformation approach (ELT)**.

Data flows through the following stages:

1. Raw data ingestion into the **Bronze layer**
2. Data cleaning and transformation in the **Silver layer**
3. Creation of **analytics-ready datasets in the Gold layer**

This layered approach improves:

- data reliability  
- maintainability  
- analytical performance  

---

## Fact Table

```
fact_sales
```

Contains measurable business events such as:

- sales amount  
- transaction metrics  
- product sales activity  

---

## Dimension Tables

```
dim_customers
dim_products
```

Dimension tables provide context for analysis such as:

- customer attributes  
- product categories  
- descriptive business information  

---

# Repository Structure

```
crm-erp-sales-analytics
│
├── architecture
│   ├── architecture_overview.png
│   └── data_flow.png
│
├── data
│   ├── crm_sales_details.csv
│   ├── crm_cust_info.csv
│   ├── crm_prd_info.csv
│   ├── erp_cust_az12.csv
│   ├── erp_loc_a101.csv
│   └── erp_px_cat_g1v2.csv
│
├── sql
│   ├── bronze_layer.sql
│   ├── silver_layer.sql
│   └── gold_layer.sql
│
├── docs
│   ├── data_dictionary.md
│   └── transformation_logic.md
│
└── README.md
```

---

# Tools & Concepts Used

- SQL  
- Data Cleaning  
- Data Transformation  
- Dimensional Data Modeling  
- Star Schema  
- Data Preparation for Analytics  

---

# Key Learning Outcomes

Through this project, the following analytics concepts were applied:

- Designing a layered data architecture
- Preparing raw operational data for analysis
- Implementing dimensional data models
- Structuring datasets for business intelligence
- Improving data usability for reporting and dashboards

---

# Project Outcome

This project demonstrates how **raw operational datasets from multiple systems can be transformed into structured analytics-ready data models**.

The resulting dataset supports **efficient business analysis, reporting, and dashboard development**, enabling organizations to make data-driven decisions.

---

⭐ If you found this project useful, feel free to explore the repository and review the data preparation and modeling approach.
