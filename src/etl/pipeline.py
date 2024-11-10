import logging
from typing import Dict, List
from .extractors import DataExtractor
from .transformers import DataTransformer
from .loaders import DataLoader
from ..utils.database import DatabaseConnection

logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self, config_path: str = 'config/database.ini'):
        self.db_conn = DatabaseConnection(config_path)
        self.extractor = DataExtractor(self.db_conn)
        self.transformer = DataTransformer()
        self.loader = DataLoader(self.db_conn)
    
    def run_customer_pipeline(self):
        """Run ETL pipeline for customer data"""
        try:
            # Extract
            raw_data = self.extractor.extract_from_mongodb('customers')
            
            # Transform
            transformed_data = self.transformer.clean_customer_data(raw_data)
            
            # Load
            self.loader.load_to_warehouse(transformed_data, 'dim_customer')
            
            logger.info("Customer pipeline completed successfully")
        except Exception as e:
            logger.error(f"Error in customer pipeline: {str(e)}")
            raise
    
    def run_sales_pipeline(self):
        """Run ETL pipeline for sales data"""
        try:
            # Extract
            raw_data = self.extractor.extract_from_api('sales')
            
            # Transform
            transformed_data = self.transformer.transform_sales_data(raw_data)
            
            # Load
            self.loader.load_to_warehouse(transformed_data, 'fact_sales')
            
            logger.info("Sales pipeline completed successfully")
        except Exception as e:
            logger.error(f"Error in sales pipeline: {str(e)}")
            raise