import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from pymongo import MongoClient
import requests
import json
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import hashlib
import great_expectations as ge

class DataQualityChecker:
    @staticmethod
    def check_nulls(df: pd.DataFrame, threshold: float = 0.05) -> Dict:
        """Check for null values in each column"""
        null_percentages = (df.isnull().sum() / len(df)) * 100
        failed_columns = null_percentages[null_percentages > threshold].to_dict()
        return {
            'passed': len(failed_columns) == 0,
            'failed_columns': failed_columns
        }
    
    @staticmethod
    def check_duplicates(df: pd.DataFrame, subset: List[str]) -> Dict:
        """Check for duplicate records"""
        duplicate_count = df.duplicated(subset=subset).sum()
        return {
            'passed': duplicate_count == 0,
            'duplicate_count': duplicate_count
        }
    
    @staticmethod
    def check_value_ranges(df: pd.DataFrame, rules: Dict) -> Dict:
        """Check if values fall within expected ranges"""
        failed_rules = {}
        for column, rule in rules.items():
            if column in df.columns:
                if 'min' in rule:
                    if df[column].min() < rule['min']:
                        failed_rules[f"{column}_min"] = {
                            'expected': rule['min'],
                            'actual': df[column].min()
                        }
                if 'max' in rule:
                    if df[column].max() > rule['max']:
                        failed_rules[f"{column}_max"] = {
                            'expected': rule['max'],
                            'actual': df[column].max()
                        }
        return {
            'passed': len(failed_rules) == 0,
            'failed_rules': failed_rules
        }

class EnhancedRetailETLPipeline:
    def __init__(self):
        self.sql_engine = create_engine('postgresql://user:password@localhost:5432/retail_dw')
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.api_base_url = 'https://api.retailcompany.com/v1'
        self.quality_checker = DataQualityChecker()
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Data quality rules
        self.price_rules = {
            'unit_price': {'min': 0, 'max': 10000},
            'quantity': {'min': 1, 'max': 1000},
            'discount_rate': {'min': 0, 'max': 0.9}
        }

    def get_last_processed_timestamp(self, table_name: str) -> datetime:
        """Retrieve the last processed timestamp for incremental loading"""
        try:
            query = text(f"SELECT MAX(last_updated) FROM {table_name}")
            result = self.sql_engine.execute(query).scalar()
            return result or datetime.min
        except Exception as e:
            self.logger.error(f"Error getting last timestamp: {str(e)}")
            return datetime.min

    def complex_product_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply complex transformations to product data"""
        df = df.copy()
        
        # Create product categories hierarchy
        df[['main_category', 'sub_category']] = df['category'].str.split('/', expand=True)
        
        # Calculate price tiers
        df['price_percentile'] = df['base_price'].rank(pct=True)
        df['price_tier'] = pd.qcut(df['base_price'], q=5, labels=['Budget', 'Economy', 'Standard', 'Premium', 'Luxury'])
        
        # Generate product tags based on name and description
        df['product_tags'] = df.apply(
            lambda x: ' '.join(set(x['product_name'].lower().split() + 
                                 x.get('description', '').lower().split())), 
            axis=1
        )
        
        # Calculate product complexity score
        df['complexity_score'] = (
            df['product_name'].str.len() * 0.3 +
            df['product_tags'].str.count(' ') * 0.5 +
            df['base_price'] * 0.2
        ).round(2)
        
        return df

    def complex_transaction_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply complex transformations to transaction data"""
        df = df.copy()
        
        # Calculate time-based features
        df['transaction_timestamp'] = pd.to_datetime(df['transaction_date'])
        df['hour_of_day'] = df['transaction_timestamp'].dt.hour
        df['day_of_week'] = df['transaction_timestamp'].dt.day_name()
        df['is_weekend'] = df['transaction_timestamp'].dt.dayofweek.isin([5, 6])
        
        # Calculate basket metrics
        df['basket_size'] = df.groupby('transaction_id')['quantity'].transform('sum')
        df['basket_value'] = df.groupby('transaction_id')['total_amount'].transform('sum')
        df['item_rank_in_basket'] = df.groupby('transaction_id')['quantity'].rank(ascending=False)
        
        # Calculate customer purchase patterns
        df['customer_total_purchases'] = df.groupby('customer_id')['total_amount'].transform('sum')
        df['purchase_percentile'] = df.groupby('customer_id')['total_amount'].transform(
            lambda x: pd.qcut(x, q=4, labels=['Low', 'Medium', 'High', 'Very High'])
        )
        
        return df

    def apply_data_quality_checks(self, df: pd.DataFrame, table_name: str) -> bool:
        """Apply all data quality checks and log results"""
        checks_passed = True
        
        # Check for nulls
        null_check = self.quality_checker.check_nulls(df)
        if not null_check['passed']:
            self.logger.error(f"Null check failed for {table_name}: {null_check['failed_columns']}")
            checks_passed = False
        
        # Check for duplicates
        key_columns = {
            'dim_product': ['product_id'],
            'dim_store': ['store_id'],
            'fact_sales': ['transaction_id', 'product_id', 'store_id']
        }
        if table_name in key_columns:
            dup_check = self.quality_checker.check_duplicates(df, key_columns[table_name])
            if not dup_check['passed']:
                self.logger.error(f"Duplicate check failed for {table_name}: {dup_check['duplicate_count']} duplicates found")
                checks_passed = False
        
        # Check value ranges for fact table
        if table_name == 'fact_sales':
            range_check = self.quality_checker.check_value_ranges(df, self.price_rules)
            if not range_check['passed']:
                self.logger.error(f"Value range check failed for {table_name}: {range_check['failed_rules']}")
                checks_passed = False
        
        return checks_passed

    def calculate_data_hash(self, df: pd.DataFrame) -> str:
        """Calculate hash of dataframe for change detection"""
        return hashlib.md5(pd.util.hash_pandas_object(df).values).hexdigest()

    def load_dimension_scd2(self, df: pd.DataFrame, table_name: str) -> None:
        """Load dimension using slowly changing dimension type 2"""
        try:
            # Get existing records
            existing_df = pd.read_sql(f"SELECT * FROM {table_name}", self.sql_engine)
            
            # Calculate hashes for change detection
            df['row_hash'] = df.apply(lambda row: hashlib.md5(str(row.values).encode()).hexdigest(), axis=1)
            existing_df['row_hash'] = existing_df.apply(lambda row: hashlib.md5(str(row.values).encode()).hexdigest(), axis=1)
            
            # Identify changes
            merged = df.merge(existing_df[['business_key', 'row_hash']], 
                            on='business_key', 
                            how='left', 
                            suffixes=('_new', '_existing'))
            
            # Handle new and changed records
            new_records = merged[merged['row_hash_existing'].isna()]
            changed_records = merged[
                (merged['row_hash_existing'].notna()) & 
                (merged['row_hash_new'] != merged['row_hash_existing'])
            ]
            
            if not new_records.empty:
                new_records['valid_from'] = datetime.now()
                new_records['valid_to'] = datetime.max
                new_records['is_current'] = True
                new_records.to_sql(table_name, self.sql_engine, if_exists='append', index=False)
            
            if not changed_records.empty:
                # Update existing records
                update_stmt = f"""
                    UPDATE {table_name}
                    SET valid_to = CURRENT_TIMESTAMP,
                        is_current = FALSE
                    WHERE business_key IN :keys
                    AND is_current = TRUE
                """
                self.sql_engine.execute(
                    update_stmt,
                    {'keys': tuple(changed_records['business_key'].tolist())}
                )
                
                # Insert new versions
                changed_records['valid_from'] = datetime.now()
                changed_records['valid_to'] = datetime.max
                changed_records['is_current'] = True
                changed_records.to_sql(table_name, self.sql_engine, if_exists='append', index=False)
            
            self.logger.info(f"Loaded {len(new_records)} new and {len(changed_records)} changed records to {table_name}")
            
        except Exception as e:
            self.logger.error(f"Error loading {table_name}: {str(e)}")
            raise

    def run_pipeline(self) -> None:
        """Execute the complete ETL pipeline with all enhancements"""
        try:
            # Extract and transform product data
            df_products = self.extract_product_data()
            if self.apply_data_quality_checks(df_products, 'dim_product'):
                df_products = self.complex_product_transformations(df_products)
                self.load_dimension_scd2(df_products, 'dim_product')
            
            # Extract and transform transaction data
            last_processed = self.get_last_processed_timestamp('fact_sales')
            df_transactions = self.extract_transactions(last_processed)
            if self.apply_data_quality_checks(df_transactions, 'fact_sales'):
                df_transactions = self.complex_transaction_transformations(df_transactions)
                self.load_fact_table(df_transactions, 'fact_sales')
            
            self.logger.info("Enhanced ETL pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise

if __name__ == "__main__":
    etl = EnhancedRetailETLPipeline()
    etl.run_pipeline()