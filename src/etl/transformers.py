import pandas as pd
import numpy as np
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class DataTransformer:
    @staticmethod
    def clean_customer_data(df: pd.DataFrame) -> pd.DataFrame:
        """Clean and transform customer data"""
        try:
            # Remove duplicates
            df = df.drop_duplicates()
            
            # Handle missing values
            df['email'] = df['email'].fillna('')
            df['phone'] = df['phone'].fillna('')
            
            # Standardize phone numbers
            df['phone'] = df['phone'].apply(lambda x: ''.join(filter(str.isdigit, str(x))))
            
            # Convert dates to datetime
            df['registration_date'] = pd.to_datetime(df['registration_date'])
            
            return df
        except Exception as e:
            logger.error(f"Error cleaning customer data: {str(e)}")
            raise
    
    @staticmethod
    def transform_sales_data(df: pd.DataFrame) -> pd.DataFrame:
        """Transform sales data"""
        try:
            # Calculate derived columns
            df['total_amount'] = df['quantity'] * df['unit_price']
            df['discount_amount'] = df['total_amount'] * df['discount_rate']
            df['final_amount'] = df['total_amount'] - df['discount_amount']
            
            # Convert dates
            df['sale_date'] = pd.to_datetime(df['sale_date'])
            
            # Add time dimensions
            df['sale_year'] = df['sale_date'].dt.year
            df['sale_month'] = df['sale_date'].dt.month
            df['sale_quarter'] = df['sale_date'].dt.quarter
            
            return df
        except Exception as e:
            logger.error(f"Error transforming sales data: {str(e)}")
            raise