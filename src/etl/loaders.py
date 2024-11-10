import pandas as pd
from typing import List
import logging
from ..utils.database import DatabaseConnection

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, db_connection: DatabaseConnection):
        self.db_conn = db_connection
    
    def load_to_warehouse(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append') -> None:
        """Load DataFrame to data warehouse"""
        try:
            engine = self.db_conn.get_sqlalchemy_engine()
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False,
                chunksize=1000
            )
            logger.info(f"Successfully loaded {len(df)} rows to {table_name}")
        except Exception as e:
            logger.error(f"Error loading data to warehouse: {str(e)}")
            raise