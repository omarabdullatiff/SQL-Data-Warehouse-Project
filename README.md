# SQL Data Warehouse Project

A comprehensive SQL-based data warehouse solution designed for storing, modeling, and serving clean, analytics-ready data using the **Medallion Architecture** pattern.

##  Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Data Sources](#data-sources)
- [Getting Started](#getting-started)
- [Database Schema](#database-schema)
- [Data Pipeline](#data-pipeline)
- [Usage](#usage)
- [License](#license)

## Overview

This project implements a modern data warehouse solution using SQL Server, following industry best practices for data ingestion, transformation, and analytics. The warehouse is designed to handle data from multiple source systems (ERP and CRM) and transform it through a three-layer architecture for optimal performance and data quality.

### Key Features

-  **Medallion Architecture** (Bronze → Silver → Gold)
-  **Multi-source Integration** (ERP & CRM systems)
-  **Automated Data Pipeline** with stored procedures
-  **Data Quality Controls** and metadata tracking


##  Architecture

The data warehouse follows the **Medallion Architecture** pattern, providing a clear separation of concerns and data quality levels:
![First Diagram (1)](https://github.com/user-attachments/assets/970f7271-9a47-4706-b7ae-1f7f9dee4991)


```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   BRONZE LAYER  │───▶│  SILVER LAYER   │───▶│   GOLD LAYER    │
│                 │    │                 │    │                 │
│ • Raw Data      │    │ • Cleaned Data  │    │ • Analytics     │
│ • As-Is Format  │    │ • Validated     │    │ • Business      │
│ • Source Schema │    │ • Enriched      │    │   Ready         │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Layer Descriptions

| Layer | Purpose | Data Quality | Schema |
|-------|---------|--------------|--------|
| **Bronze** | Raw data ingestion | Source system quality | Source schema |
| **Silver** | Data cleaning & validation | High quality, validated | Optimized schema |
| **Gold** | Business analytics | Production ready | Business schema |

##  Project Structure

```
DWH-project/
├── 📂 datasets/                    # Source data files
│   ├── 📂 source_erp/             # ERP system data
│   │   ├── PX_CAT_G1V2.csv
│   │   ├── LOC_A101.csv
│   │   └── CUST_AZ12.csv
│   └── 📂 source_crm/             # CRM system data
│       ├── sales_details.csv
│       ├── prd_info.csv
│       └── cust_info.csv
├── 📂 scripts/                    # SQL scripts by layer
│   ├── init_DBs.sql              # Database initialization
│   ├── 📂 Bronze/                # Raw data layer
│   │   ├── ddl_bronze.sql        # Table definitions
│   │   └── proc_load_bronze.sql  # Data loading procedures
│   ├── 📂 Silver/                # Cleaned data layer
│   │   ├── ddl_load_silver.sql   # Table definitions
│   │   └── proc_load_silver.sql  # Data transformation procedures
│   └── 📂 gold/                  # Analytics layer
├── 📂 docs/                      # Documentation
├── 📂 tests/                     # Test files
├── README.md                     
└── LICENSE                       # MIT License
```

##  Data Sources

### CRM System Data
- **Customer Information**: `cust_info.csv` (18,495 records)
- **Product Information**: `prd_info.csv` (399 records)
- **Sales Details**: `sales_details.csv` (3.4MB)

### ERP System Data
- **Customer Data**: `CUST_AZ12.csv` (18,485 records)
- **Location Data**: `LOC_A101.csv` (18,486 records)  
- **Product Categories**: `PX_CAT_G1V2.csv` (38 records)

##  Getting Started

### Prerequisites

- SQL Server 2019 or later
- SQL Server Management Studio (SSMS)
- Git (for version control)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/omarabdullatiff/SQL-Data-Warehouse-Project.git
   cd SQL-Data-Warehouse-Project
   ```

2. **Initialize the database**
   ```sql
   -- Run the initialization script
   sqlcmd -S your_server -i scripts/init_DBs.sql
   ```

3. **Set up the Bronze layer**
   ```sql
   -- Create Bronze tables
   sqlcmd -S your_server -d datawarehouse -i scripts/Bronze/ddl_bronze.sql
   ```

4. **Set up the Silver layer**
   ```sql
   -- Create Silver tables
   sqlcmd -S your_server -d datawarehouse -i scripts/Silver/ddl_load_silver.sql
   ```

##  Database Schema

### Bronze Layer Tables

| Table | Purpose | Source |
|-------|---------|--------|
| `bronze.crm_cust_info` | Raw customer data | CRM system |
| `bronze.crm_prd_info` | Raw product data | CRM system |
| `bronze.erp_cust_info` | Raw customer data | ERP system |
| `bronze.erp_loc_info` | Raw location data | ERP system |

### Silver Layer Tables

| Table | Purpose | Enhancements |
|-------|---------|-------------|
| `silver.crm_cust_info` | Cleaned customer data | + Data validation, + Metadata |
| `silver.crm_prd_info` | Cleaned product data | + Data validation, + Metadata |
| `silver.erp_cust_info` | Cleaned customer data | + Data validation, + Metadata |
| `silver.erp_loc_info` | Cleaned location data | + Data validation, + Metadata |

##  Data Pipeline

### 1. Data Ingestion (Bronze Layer)
```sql
-- Load raw data from CSV files
EXEC bronze.proc_load_bronze @source_file = 'datasets/source_crm/cust_info.csv'
```

### 2. Data Transformation (Silver Layer)
```sql
-- Transform and clean data
EXEC silver.proc_load_silver @bronze_table = 'bronze.crm_cust_info'
```

### 3. Analytics Preparation (Gold Layer)
```sql
-- Create business-ready datasets
-- (Gold layer scripts to be implemented)
```

## 💻 Usage

### Loading Data

1. **Load Bronze data**:
   ```sql
   USE datawarehouse;
   EXEC bronze.proc_load_bronze;
   ```

2. **Transform to Silver**:
   ```sql
   USE datawarehouse;
   EXEC silver.proc_load_silver;
   ```

### Querying Data

```sql
-- Example: Get customer analytics
SELECT 
    cst_firstname,
    cst_lastname,
    cst_gndr,
    cst_create_date,
    dwh_create_data
FROM silver.crm_cust_info
WHERE cst_create_date >= '2023-01-01';
```

##  Configuration

### Database Settings
- **Database Name**: `datawarehouse`
- **Schemas**: `bronze`, `silver`, `gold`
- **Collation**: SQL_Latin1_General_CP1_CI_AS

### Performance Optimization
- Indexes on key columns for faster queries
- Partitioning strategy for large tables
- Automated maintenance plans

##  Testing

Run the test suite to validate data quality and pipeline integrity:

```bash
# Navigate to tests directory
cd tests/

# Run validation scripts
sqlcmd -S your_server -d datawarehouse -i test_data_quality.sql
```

##  Monitoring

### Key Metrics
- Data freshness (last update timestamps)
- Data quality scores
- Pipeline execution times
- Error rates and failed records

### Logging
- All procedures include comprehensive logging
- Error handling with detailed messages
- Audit trail for data lineage

### Development Guidelines
- Follow SQL Server best practices
- Include comprehensive comments
- Test all procedures before committing
- Update documentation for new features


## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

##  Acknowledgments

- SQL Server community for best practices
- Data warehouse design patterns

---
