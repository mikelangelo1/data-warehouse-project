import logging
from typing import Dict, List, Any
import pandas as pd
from ..utils.database import DatabaseConnection

logger = logging.getLogger(__name__)

class DataExtractor:
    def __init__(self, db_connection: DatabaseConnection):
        self.db_conn = db_connection
        
    def extract_from_mongodb(self, collection: str, query: Dict = None) -> pd.DataFrame:
        """Extract data from MongoDB collection"""
        try:
            mongo_db = self.db_conn.get_mongo_connection()
            data = list(mongo_db[collection].find(query or {}))
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Error extracting from MongoDB: {str(e)}")
            raise
            
    def extract_from_api(self, endpoint: str, params: Dict = None) -> pd.DataFrame:
        """Extract data from REST API"""
        try:
            session = self.db_conn.get_api_session()
            api_config = self.db_conn.config['api']
            response = session.get(f"{api_config['base_url']}/{endpoint}", params=params)
            response.raise_for_status()
            return pd.DataFrame(response.json())
        except Exception as e:
            logger.error(f"Error extracting from API: {str(e)}")
            raise

