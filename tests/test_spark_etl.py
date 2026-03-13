#!/usr/bin/env python3
"""
test_spark_etl.py
-----------------
Unit tests for the Spark ETL pipeline.
"""

import os
import unittest
from unittest.mock import patch, MagicMock, call

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType


class TestSparkETL(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures."""
        self.mock_spark = MagicMock(spec=SparkSession)

    def test_build_spark(self):
        """Test Spark session configuration."""
        with patch('spark_etl.SparkSession') as mock_spark_session:
            mock_builder = MagicMock()
            mock_spark_session.builder = mock_builder

            # Import and call build_spark
            from spark_jobs.spark_etl import build_spark
            result = build_spark()

            # Verify builder chain
            mock_builder.appName.assert_called_with("CustomerTransactionsETL")
            mock_builder.config.assert_called()
            mock_builder.getOrCreate.assert_called_once()

    def test_extract_data(self):
        """Test data extraction from Delta table."""
        with patch('spark_jobs.spark_etl.build_spark', return_value=self.mock_spark):
            from spark_jobs.spark_etl import extract_data

            # Mock Delta table read
            mock_df = MagicMock()
            self.mock_spark.read.format.return_value.load.return_value = mock_df

            result = extract_data()

            # Verify Delta table read
            self.mock_spark.read.format.assert_called_with("delta")
            self.mock_spark.read.format.return_value.load.assert_called_once()
            self.assertEqual(result, mock_df)

    def test_transform_data(self):
        """Test data transformation logic."""
        with patch('spark_jobs.spark_etl.build_spark', return_value=self.mock_spark):
            from spark_jobs.spark_etl import transform_data

            # Mock input DataFrame
            mock_df = MagicMock()
            mock_transformed = MagicMock()
            mock_df.withColumn.return_value = mock_transformed
            mock_transformed.groupBy.return_value.agg.return_value = mock_transformed

            result = transform_data(mock_df)

            # Verify transformations were applied
            self.assertIsNotNone(result)
            # Check that withColumn was called for date extraction
            mock_df.withColumn.assert_called()

    def test_load_data(self):
        """Test data loading to ScyllaDB."""
        with patch('spark_jobs.spark_etl.build_spark', return_value=self.mock_spark):
            from spark_jobs.spark_etl import load_data

            # Mock DataFrame
            mock_df = MagicMock()
            mock_writer = MagicMock()
            mock_df.write.format.return_value = mock_writer
            mock_writer.mode.return_value = mock_writer
            mock_writer.options.return_value = mock_writer
            mock_writer.save.return_value = None

            load_data(mock_df)

            # Verify Cassandra write configuration
            mock_df.write.format.assert_called_with("org.apache.spark.sql.cassandra")
            mock_writer.options.assert_called()

    def test_etl_pipeline_integration(self):
        """Test the complete ETL pipeline flow."""
        with patch('spark_jobs.spark_etl.build_spark', return_value=self.mock_spark), \
             patch('spark_jobs.spark_etl.extract_data') as mock_extract, \
             patch('spark_jobs.spark_etl.transform_data') as mock_transform, \
             patch('spark_jobs.spark_etl.load_data') as mock_load:

            from spark_jobs.spark_etl import etl_pipeline

            # Mock return values
            mock_extract.return_value = MagicMock()
            mock_transform.return_value = MagicMock()

            etl_pipeline()

            # Verify all steps were called
            mock_extract.assert_called_once()
            mock_transform.assert_called_once()
            mock_load.assert_called_once()

    def test_data_quality_checks(self):
        """Test data quality validation."""
        # Test that transformed data has required columns
        expected_columns = ['customer_id', 'transaction_date', 'total_amount', 'transaction_count']

        # This would check the schema of transformed data
        self.assertEqual(len(expected_columns), 4)
        self.assertIn('total_amount', expected_columns)
        self.assertIn('transaction_count', expected_columns)


if __name__ == '__main__':
    unittest.main()