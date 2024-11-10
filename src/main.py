import os
import sys
import logging
import logging.config
import argparse
from datetime import datetime
from etl.pipeline import ETLPipeline
from quality.checks import DataQualityChecker
from utils.database import DatabaseConnection

def setup_logging():
    """Setup logging configuration"""
    logging.config.fileConfig('config/logging.conf')
    return logging.getLogger(__name__)

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Retail Data Warehouse ETL Pipeline')
    parser.add_argument('--pipeline', choices=['all', 'customer', 'sales', 'product', 'store'],
                       default='all', help='Pipeline to run')
    parser.add_argument('--mode', choices=['full', 'incremental'],
                       default='incremental', help='Load mode')
    return parser.parse_args()

def main():
    """Main application entry point"""
    # Setup logging
    logger = setup_logging()
    logger.info("Starting Retail Data Warehouse ETL process")
    
    try:
        args = parse_arguments()
        
        # Initialize pipeline
        pipeline = ETLPipeline()
        db_conn = DatabaseConnection()
        quality_checker = DataQualityChecker(db_conn)
        
        # Run selected pipeline
        if args.pipeline in ['all', 'customer']:
            logger.info("Running customer pipeline")
            pipeline.run_customer_pipeline()
            
        if args.pipeline in ['all', 'sales']:
            logger.info("Running sales pipeline")
            pipeline.run_sales_pipeline()
            
        if args.pipeline in ['all', 'product']:
            logger.info("Running product pipeline")
            # Add product pipeline implementation
            
        if args.pipeline in ['all', 'store']:
            logger.info("Running store pipeline")
            # Add store pipeline implementation
        
        logger.info("ETL process completed successfully")
        
    except Exception as e:
        logger.error(f"Error in ETL process: {str(e)}")
        sys.exit(1)
    finally:
        # Cleanup connections
        try:
            db_conn.close_connections()
        except:
            pass

if __name__ == "__main__":
    main()