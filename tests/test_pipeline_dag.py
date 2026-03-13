#!/usr/bin/env python3
"""
test_pipeline_dag.py
--------------------
Unit tests for the Airflow DAG.
"""

import os
import unittest
from datetime import datetime, timedelta

# Set Airflow environment variables for testing
os.environ['AIRFLOW__CORE__EXECUTOR'] = 'SequentialExecutor'
os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'] = 'sqlite:///test.db'

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


class TestPipelineDAG(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures."""
        # Import the DAG
        from dags.pipeline_dag import dag
        self.dag = dag

    def test_dag_creation(self):
        """Test that the DAG is created correctly."""
        self.assertIsInstance(self.dag, DAG)
        self.assertEqual(self.dag.dag_id, 'customer_transactions_etl')
        self.assertEqual(self.dag.description, 'ETL pipeline: Delta Lake → Spark Transform → ScyllaDB')

    def test_dag_schedule(self):
        """Test DAG scheduling configuration."""
        self.assertEqual(self.dag.schedule_interval, '0 1 * * *')  # Daily at 01:00
        self.assertIsNotNone(self.dag.start_date)
        self.assertFalse(self.dag.catchup)

    def test_dag_default_args(self):
        """Test DAG default arguments."""
        default_args = self.dag.default_args
        self.assertEqual(default_args['owner'], 'data-engineering')
        self.assertEqual(default_args['retries'], 2)
        self.assertEqual(default_args['retry_delay'], timedelta(minutes=5))

    def test_dag_tasks(self):
        """Test that all expected tasks are present."""
        task_ids = [task.task_id for task in self.dag.tasks]

        expected_tasks = [
            'start_pipeline',
            'extract_delta',
            'spark_transform',
            'quality_check',
            'notify_success',
            'notify_failure'
        ]

        for task_id in expected_tasks:
            self.assertIn(task_id, task_ids)

    def test_task_dependencies(self):
        """Test task dependencies."""
        start_task = self.dag.get_task('start_pipeline')
        extract_task = self.dag.get_task('extract_delta')
        transform_task = self.dag.get_task('spark_transform')
        quality_task = self.dag.get_task('quality_check')
        success_task = self.dag.get_task('notify_success')
        failure_task = self.dag.get_task('notify_failure')

        # Check upstream/downstream relationships
        self.assertIn(start_task, extract_task.upstream_list)
        self.assertIn(extract_task, transform_task.upstream_list)
        self.assertIn(transform_task, quality_task.upstream_list)
        self.assertIn(quality_task, success_task.upstream_list)

    def test_task_types(self):
        """Test that tasks are of correct types."""
        self.assertIsInstance(self.dag.get_task('start_pipeline'), EmptyOperator)
        self.assertIsInstance(self.dag.get_task('extract_delta'), PythonOperator)
        self.assertIsInstance(self.dag.get_task('spark_transform'), BashOperator)
        self.assertIsInstance(self.dag.get_task('quality_check'), PythonOperator)
        self.assertIsInstance(self.dag.get_task('notify_success'), PythonOperator)
        self.assertIsInstance(self.dag.get_task('notify_failure'), PythonOperator)

    def test_dag_validation(self):
        """Test DAG validation."""
        # This should not raise any exceptions
        try:
            self.dag.validate()
        except Exception as e:
            self.fail(f"DAG validation failed: {e}")

    def test_task_configuration(self):
        """Test specific task configurations."""
        spark_task = self.dag.get_task('spark_transform')
        self.assertIn('spark-submit', spark_task.bash_command)
        self.assertIn('spark_etl.py', spark_task.bash_command)


if __name__ == '__main__':
    unittest.main()