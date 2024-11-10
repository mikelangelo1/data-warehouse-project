import pytest
import pandas as pd
from src.quality.checks import DataQualityChecker
from src.utils.database import DatabaseConnection

@pytest.fixture
def sample_data():
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Test1', 'Test2', None, 'Test4', 'Test5'],
        'value': [10, 20, 30, 40, 1000],
        'category': ['A', 'A', 'B', 'B', 'C']
    })

class TestDataQualityChecker:
    def test_check_null_values(self, db_connection, sample_data):
        # Arrange
        checker = DataQualityChecker(db_connection)
        required_columns = ['name', 'value']

        # Act
        passed, results = checker.check_null_values(sample_data, required_columns)

        # Assert
        assert not passed  # Should fail due to null in 'name' column
        assert results['name']['null_count'] == 1
        assert results['value']['null_count'] == 0

    def test_check_duplicates(self, db_connection, sample_data):
        # Arrange
        checker = DataQualityChecker(db_connection)
        unique_columns = ['category']

        # Act
        passed, results = checker.check_duplicates(sample_data, unique_columns)

        # Assert
        assert not passed  # Should fail due to duplicate categories
        assert results['duplicate_count'] > 0

    def test_check_value_ranges(self, db_connection, sample_data):
        # Arrange
        checker = DataQualityChecker(db_connection)
        range_checks = {
            'value': (0, 100)  # Min and max expected values
        }

        # Act
        passed, results = checker.check_value_ranges(sample_data, range_checks)

        # Assert
        assert not passed  # Should fail due to value 1000 being out of range
        assert results['value']['outlier_count'] == 1

    def test_check_referential_integrity(self, db_connection, mocker):
        # Arrange
        checker = DataQualityChecker(db_connection)
        mock_cursor = mocker.Mock()
        mock_cursor.fetchone.side_effect = [
            {'orphaned_count': 0},
            {'total_count': 100}
        ]
        db_connection.get_postgres_connection.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        # Act
        passed, results = checker.check_referential_integrity(
            'fact_sales', 'product_id', 'dim_product', 'product_id'
        )

        # Assert
        assert passed
        assert results['orphaned_count'] == 0
        assert results['orphaned_percentage'] == 0
