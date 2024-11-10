import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import logging
from ..utils.database import DatabaseConnection

logger = logging.getLogger(__name__)

class DataQualityChecker:
    def __init__(self, db_connection: DatabaseConnection):
        self.db_conn = db_connection
    
    def check_null_values(self, df: pd.DataFrame, required_columns: List[str]) -> Tuple[bool, Dict]:
        """Check for null values in required columns"""
        results = {}
        passed = True
        
        for column in required_columns:
            null_count = df[column].isnull().sum()
            null_percentage = (null_count / len(df)) * 100
            
            results[column] = {
                'null_count': null_count,
                'null_percentage': null_percentage
            }
            
            if null_count > 0:
                passed = False
                logger.warning(f"Column {column} contains {null_count} null values ({null_percentage:.2f}%)")
        
        return passed, results
    
    def check_duplicates(self, df: pd.DataFrame, unique_columns: List[str]) -> Tuple[bool, Dict]:
        """Check for duplicate records based on specified columns"""
        duplicates = df[df.duplicated(subset=unique_columns, keep=False)]
        duplicate_count = len(duplicates)
        
        results = {
            'duplicate_count': duplicate_count,
            'duplicate_percentage': (duplicate_count / len(df)) * 100
        }
        
        passed = duplicate_count == 0
        if not passed:
            logger.warning(f"Found {duplicate_count} duplicate records")
        
        return passed, results
    
    def check_value_ranges(self, df: pd.DataFrame, range_checks: Dict) -> Tuple[bool, Dict]:
        """Check if values fall within expected ranges"""
        results = {}
        passed = True
        
        for column, ranges in range_checks.items():
            min_val, max_val = ranges
            outliers = df[
                (df[column] < min_val) | 
                (df[column] > max_val)
            ]
            
            results[column] = {
                'outlier_count': len(outliers),
                'outlier_percentage': (len(outliers) / len(df)) * 100,
                'min_value': df[column].min(),
                'max_value': df[column].max()
            }
            
            if len(outliers) > 0:
                passed = False
                logger.warning(f"Column {column} contains {len(outliers)} values outside expected range")
        
        return passed, results
    
    def check_referential_integrity(self, table_name: str, foreign_key: str, 
                                  ref_table: str, ref_key: str) -> Tuple[bool, Dict]:
        """Check referential integrity between tables"""
        try:
            conn = self.db_conn.get_postgres_connection()
            with conn.cursor() as cur:
                # Find orphaned records
                query = f"""
                SELECT COUNT(*) as orphaned_count
                FROM {table_name} t
                LEFT JOIN {ref_table} r ON t.{foreign_key} = r.{ref_key}
                WHERE r.{ref_key} IS NULL
                """
                cur.execute(query)
                orphaned_count = cur.fetchone()['orphaned_count']
                
                # Get total records
                cur.execute(f"SELECT COUNT(*) as total_count FROM {table_name}")
                total_count = cur.fetchone()['total_count']
                
                results = {
                    'orphaned_count': orphaned_count,
                    'orphaned_percentage': (orphaned_count / total_count * 100) if total_count > 0 else 0
                }
                
                passed = orphaned_count == 0
                if not passed:
                    logger.warning(f"Found {orphaned_count} orphaned records in {table_name}")
                
                return passed, results
                
        except Exception as e:
            logger.error(f"Error checking referential integrity: {str(e)}")
            raise
    def check_product_data(self, df: pd.DataFrame) -> bool:
        """Check product data quality"""
        checks_passed = True
        
        # Check required columns
        required_columns = ['product_id', 'product_name', 'category', 'base_price']
        if not self._check_required_columns(df, required_columns):
            checks_passed = False
        
        # Check for duplicates
        if df['product_id'].duplicated().any():
            self.logger.error("Duplicate product IDs found")
            checks_passed = False
        
        # Check value ranges
        if (df['base_price'] <= 0).any():
            self.logger.error("Invalid base prices found")
            checks_passed = False
        
        return checks_passed
    
    def check_transaction_data(self, df: pd.DataFrame) -> bool:
        """Check transaction data quality"""
        checks_passed = True
        
        # Check required columns
        required_columns = ['transaction_id', 'product_id', 'quantity', 'unit_price']
        if not self._check_required_columns(df, required_columns):
            checks_passed = False
        
        # Check for valid quantities
        if (df['quantity'] <= 0).any():
            self.logger.error("Invalid quantities found")
            checks_passed = False
        
        # Check for valid prices
        if (df['unit_price'] <= 0).any():
            self.logger.error("Invalid unit prices found")
            checks_passed = False
        
        # Check date validity
        if not pd.to_datetime(df['transaction_date'], errors='coerce').notnull().all():
            self.logger.error("Invalid transaction dates found")
            checks_passed = False
        
        return checks_passed
    
    def check_store_data(self, df: pd.DataFrame) -> bool:
        """Check store data quality"""
        checks_passed = True
        
        # Check required columns
        required_columns = ['store_id', 'store_name', 'city', 'state']
        if not self._check_required_columns(df, required_columns):
            checks_passed = False
        
        # Check for valid state codes
        valid_states = set(['NY', 'NJ', 'CA', 'OR'])  # Add more as needed
        if not df['state'].isin(valid_states).all():
            self.logger.error("Invalid state codes found")
            checks_passed = False
        
        return checks_passed
    
    def _check_required_columns(self, df: pd.DataFrame, required_columns: List[str]) -> bool:
        """Check if all required columns are present"""
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False
        return True
    
    def run_all_checks(self, df: pd.DataFrame, check_config: Dict) -> Dict:
        """Run all configured data quality checks"""
        results = {
            'overall_status': True,
            'checks': {}
        }
        
        # Required columns check
        if 'required_columns' in check_config:
            passed, check_results = self.check_null_values(df, check_config['required_columns'])
            results['checks']['null_values'] = check_results
            results['overall_status'] &= passed
        
        # Duplicates check
        if 'unique_columns' in check_config:
            passed, check_results = self.check_duplicates(df, check_config['unique_columns'])
            results['checks']['duplicates'] = check_results
            results['overall_status'] &= passed
        
        # Range checks
        if 'range_checks' in check_config:
            passed, check_results = self.check_value_ranges(df, check_config['range_checks'])
            results['checks']['value_ranges'] = check_results
            results['overall_status'] &= passed
        
        return results