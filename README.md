## Overview
This project implements a retail data warehouse that integrates data from multiple sources including SQL databases, MongoDB, and external APIs. It features comprehensive ETL processes, data quality checks, and analytical capabilities.

## Setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Configure database connections in `config/database.ini`
3. Set up logging in `config/logging.conf`
4. Initialize database schema:
   ```bash
   psql -U your_username -d retail_dw -f sql/ddl/create_tables.sql
   ```

## Running the ETL Pipeline
```bash
python src/etl/pipeline.py
```

## Running Analytics
```bash
python src/analytics/run_analysis.py
```