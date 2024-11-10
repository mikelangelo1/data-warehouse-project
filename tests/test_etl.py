# test_etl.py
import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from src.etl.extractors import DataExtractor
from src.etl.transformers import DataTransformer
from src.etl.loaders import DataLoader
from src.etl.pipeline import ETLPipeline
from src.utils.database import DatabaseConnection

@pytest.fixture
def sample_sales_data():
    return pd.DataFrame({
        'sale_date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'quantity': [2, 3, 1],
        'unit_price': [10.00, 15.00, 20.00],
        'discount_rate': [0.1, 0.0, 0.2]
    })

@pytest.fixture
def sample_customer_data():
    return pd.DataFrame({
        'customer_id': [1, 2, 3],
        'email': ['test1@example.com', 'test2@example.com', None],
        'phone': ['123-456-7890', '', '987-654-3210'],
        'registration_date': ['2024-01-01', '2024-01-02', '2024-01-03']
    })

@pytest.fixture
def db_connection(mocker):
    mock_conn = mocker.Mock(spec=DatabaseConnection)
    mock_conn.get_postgres_connection.return_value = mocker.Mock()
    mock_conn.get_mongo_connection.return_value = mocker.Mock()
    mock_conn.get_api_session.return_value = mocker.Mock()
    return mock_conn

class TestDataExtractor:
    def test_extract_from_mongodb(self, db_connection, mocker):
        # Arrange
        mock_collection = mocker.Mock()
        mock_collection.find.return_value = [
            {'id': 1, 'name': 'Test1'},
            {'id': 2, 'name': 'Test2'}
        ]
        db_connection.get_mongo_connection.return_value = {'test_collection': mock_collection}
        extractor = DataExtractor(db_connection)

        # Act
        result = extractor.extract_from_mongodb('test_collection')

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ['id', 'name']

    def test_extract_from_api(self, db_connection, mocker):
        # Arrange
        mock_response = mocker.Mock()
        mock_response.json.return_value = [
            {'id': 1, 'value': 'Test1'},
            {'id': 2, 'value': 'Test2'}
        ]
        mock_session = mocker.Mock()
        mock_session.get.return_value = mock_response
        db_connection.get_api_session.return_value = mock_session
        db_connection.config = {'api': {'base_url': 'http://test.com'}}
        extractor = DataExtractor(db_connection)

        # Act
        result = extractor.extract_from_api('endpoint')

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        mock_session.get.assert_called_once()

class TestDataTransformer:
    def test_clean_customer_data(self, sample_customer_data):
        # Arrange
        transformer = DataTransformer()

        # Act
        result = transformer.clean_customer_data(sample_customer_data)

        # Assert
        assert result['email'].isnull().sum() == 0
        assert result['phone'].isnull().sum() == 0
        assert pd.api.types.is_datetime64_any_dtype(result['registration_date'])

    def test_transform_sales_data(self, sample_sales_data):
        # Arrange
        transformer = DataTransformer()

        # Act
        result = transformer.transform_sales_data(sample_sales_data)

        # Assert
        assert 'total_amount' in result.columns
        assert 'discount_amount' in result.columns
        assert 'final_amount' in result.columns
        assert 'sale_year' in result.columns
        assert 'sale_month' in result.columns
        assert 'sale_quarter' in result.columns

        # Check calculations
        expected_total = sample_sales_data['quantity'] * sample_sales_data['unit_price']
        pd.testing.assert_series_equal(result['total_amount'], expected_total)

class TestDataLoader:
    def test_load_to_warehouse(self, db_connection, sample_sales_data, mocker):
        # Arrange
        mock_engine = mocker.Mock()
        db_connection.get_sqlalchemy_engine.return_value = mock_engine
        loader = DataLoader(db_connection)

        # Act
        loader.load_to_warehouse(sample_sales_data, 'test_table')

        # Assert
        sample_sales_data.to_sql.assert_called_once_with(
            name='test_table',
            con=mock_engine,
            if_exists='append',
            index=False,
            chunksize=1000
        )

class TestETLPipeline:
    def test_run_customer_pipeline(self, db_connection, sample_customer_data, mocker):
        # Arrange
        pipeline = ETLPipeline()
        mocker.patch.object(pipeline.extractor, 'extract_from_mongodb', return_value=sample_customer_data)
        mocker.patch.object(pipeline.loader, 'load_to_warehouse')

        # Act
        pipeline.run_customer_pipeline()

        # Assert
        pipeline.extractor.extract_from_mongodb.assert_called_once_with('customers')
        pipeline.loader.load_to_warehouse.assert_called_once()

    def test_run_sales_pipeline(self, db_connection, sample_sales_data, mocker):
        # Arrange
        pipeline = ETLPipeline()
        mocker.patch.object(pipeline.extractor, 'extract_from_api', return_value=sample_sales_data)
        mocker.patch.object(pipeline.loader, 'load_to_warehouse')

        # Act
        pipeline.run_sales_pipeline()

        # Assert
        pipeline.extractor.extract_from_api.assert_called_once_with('sales')
        pipeline.loader.load_to_warehouse.assert_called_once()

