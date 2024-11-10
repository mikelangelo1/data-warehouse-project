import pytest
from src.utils.database import DatabaseConnection
import configparser

class TestDatabaseConnection:
    def test_read_config(self, mocker):
        # Arrange
        mock_config = mocker.patch('configparser.ConfigParser')
        mock_config_instance = mock_config.return_value
        db_connection = DatabaseConnection()

        # Act
        result = db_connection._read_config('test_path')

        # Assert
        mock_config_instance.read.assert_called_once_with('test_path')

    def test_get_postgres_connection(self, mocker):
        # Arrange
        mock_psycopg2 = mocker.patch('psycopg2.connect')
        db_connection = DatabaseConnection()
        db_connection.config = configparser.ConfigParser()
        db_connection.config['postgresql'] = {
            'host': 'localhost',
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass'
        }

        # Act
        connection = db_connection.get_postgres_connection()

        # Assert
        mock_psycopg2.assert_called_once()
        assert connection == mock_psycopg2.return_value

    def test_get_mongo_connection(self, mocker):
        # Arrange
        mock_mongo_client = mocker.patch('pymongo.MongoClient')
        db_connection = DatabaseConnection()
        db_connection.config = configparser.ConfigParser()
        db_connection.config['mongodb'] = {
            'uri': 'mongodb://localhost:27017',
            'database': 'test_db'
        }

        # Act
        connection = db_connection.get_mongo_connection()

        # Assert
        mock_mongo_client.assert_called_once_with('mongodb://localhost:27017')

    def test_get_sqlalchemy_engine(self, mocker):
        # Arrange
        mock_create_engine = mocker.patch('sqlalchemy.create_engine')
        db_connection = DatabaseConnection()
        db_connection.config = configparser.ConfigParser()
        db_connection.config['postgresql'] = {
            'host': 'localhost',
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass'
        }

        # Act
        engine = db_connection.get_sqlalchemy_engine()

        # Assert
        mock_create_engine.assert_called_once()
        assert engine == mock_create_engine.return_value