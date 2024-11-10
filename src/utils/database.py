import configparser
import logging
from typing import Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient
import requests
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Handles database connections and operations for different data sources"""
    
    def __init__(self, config_path: str = 'config/database.ini'):
        self.config = self._read_config(config_path)
        self.pg_conn = None
        self.mongo_client = None
        
    def _read_config(self, config_path: str) -> configparser.ConfigParser:
        """Read configuration from the specified file"""
        config = configparser.ConfigParser()
        config.read(config_path)
        return config
    
    def get_postgres_connection(self):
        """Create and return a PostgreSQL connection"""
        try:
            if not self.pg_conn or self.pg_conn.closed:
                params = self.config['postgresql']
                self.pg_conn = psycopg2.connect(**dict(params.items()),
                                              cursor_factory=RealDictCursor)
            return self.pg_conn
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {str(e)}")
            raise
    
    def get_mongo_connection(self):
        """Create and return a MongoDB connection"""
        try:
            if not self.mongo_client:
                params = self.config['mongodb']
                self.mongo_client = MongoClient(params['uri'])
            return self.mongo_client[params['database']]
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {str(e)}")
            raise
    
    def get_api_session(self):
        """Create and return an API session with authentication"""
        try:
            session = requests.Session()
            api_config = self.config['api']
            session.headers.update({
                'Authorization': f"Bearer {api_config['api_key']}",
                'Content-Type': 'application/json'
            })
            return session
        except Exception as e:
            logger.error(f"Error setting up API session: {str(e)}")
            raise
    
    def get_sqlalchemy_engine(self):
        """Create and return SQLAlchemy engine for PostgreSQL"""
        try:
            params = self.config['postgresql']
            url = f"postgresql://{params['user']}:{params['password']}@{params['host']}/{params['database']}"
            return create_engine(url)
        except Exception as e:
            logger.error(f"Error creating SQLAlchemy engine: {str(e)}")
            raise
    
    def close_connections(self):
        """Close all active database connections"""
        if self.pg_conn:
            self.pg_conn.close()
        if self.mongo_client:
            self.mongo_client.close()