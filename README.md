### Retail Dataware House Project

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

retail_data_warehouse/
├── venv/                        # Virtual environment
├── logs/
│   └── etl.log                 # Log file will be created here
├── src/
│   ├── main.py                 # Main application runner
│   ├── etl/                    # ETL modules
│   ├── quality/                # Data quality modules
│   └── utils/                  # Utility modules
├── sql/                        # SQL scripts
├── config/                     # Configuration files
└── requirements.txt            # Python dependencies

## Running the ETL Pipeline
```bash
python src/etl/pipeline.py
```

## Running Analytics
```bash
python src/analytics/run_analysis.py
```

# Run tests
```bash
pytest tests/
```

# Run specific test file
```bash
pytest tests/test_etl.py -v
```

### Check recent logs
```bash
tail -f logs/etl.log
```

# Check database status
```bash
psql -U postgres -d retail_dw
\dt  # List all tables
SELECT COUNT(*) FROM fact_sales;  # Check data loads
```

# For troubleshooting
-  Check the logs in logs/etl.log
- Verify database connections in config/database.ini
- Ensure all required services (PostgreSQL, MongoDB) are running
- Check permissions on directories and files