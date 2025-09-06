"""
Example usage of the Data Comparator.
Demonstrates how to use the tool programmatically.
"""

import logging
from data_comparator import run_comparison, load_datasets_config
from data_connectors import load_config, create_spark_session
from metadata_comparator import compare_metadata, get_data_metadata
from fingerprinting_sampler import create_data_fingerprint, compare_fingerprints
from report_generator import generate_all_reports

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def example_basic_comparison():
    """Example: Basic comparison using configuration file."""
    logger.info("=== Example 1: Basic Comparison ===")
    
    try:
        # Run comparison with default config and datasets
        run_comparison("config.yaml", "datasets.csv")
        logger.info("Basic comparison completed successfully")
        
    except Exception as e:
        logger.error(f"Error in basic comparison: {str(e)}")

def example_specific_dataset():
    """Example: Compare specific dataset."""
    logger.info("=== Example 2: Specific Dataset Comparison ===")
    
    try:
        # Compare only customer data
        run_comparison("config.yaml", "datasets.csv", "customer_data")
        logger.info("Customer data comparison completed successfully")
        
    except Exception as e:
        logger.error(f"Error in specific dataset comparison: {str(e)}")

def example_programmatic_usage():
    """Example: Programmatic usage of individual components."""
    logger.info("=== Example 3: Programmatic Usage ===")
    
    try:
        # Load configuration
        config = load_config("config.yaml")
        
        # Create Spark session
        spark = create_spark_session(config)
        
        # Load datasets configuration
        datasets_config = load_datasets_config("datasets.csv")
        dataset_config = datasets_config['datasets'][0]
        
        # Note: In real usage, you would load actual data
        # df1 = get_sql_server_data(spark, config, dataset_config)
        # df2 = get_s3_parquet_data(spark, config, dataset_config)
        
        # For demonstration, create sample data
        from pyspark.sql import Row
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        
        # Create sample DataFrames
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", IntegerType(), True)
        ])
        
        data1 = [Row(id=1, name="Alice", value=100), Row(id=2, name="Bob", value=200)]
        data2 = [Row(id=1, name="Alice", value=100), Row(id=2, name="Bob", value=200)]
        
        df1 = spark.createDataFrame(data1, schema)
        df2 = spark.createDataFrame(data2, schema)
        
        # Add row IDs
        from pyspark.sql.functions import monotonically_increasing_id
        df1 = df1.withColumn("__row_id", monotonically_increasing_id())
        df2 = df2.withColumn("__row_id", monotonically_increasing_id())
        
        # Get metadata
        metadata1 = get_data_metadata(df1)
        metadata2 = get_data_metadata(df2)
        
        # Compare metadata
        metadata_comparison = compare_metadata(metadata1, metadata2)
        logger.info(f"Metadata comparison result: {metadata_comparison['overall_match']}")
        
        # Create fingerprints
        df1_fp = create_data_fingerprint(df1, algorithm="md5")
        df2_fp = create_data_fingerprint(df2, algorithm="md5")
        
        # Compare fingerprints
        fingerprint_comparison = compare_fingerprints(df1_fp, df2_fp)
        logger.info(f"Fingerprint comparison result: {fingerprint_comparison['fingerprints_match']}")
        
        # Generate reports
        comparison_results = {
            'metadata_comparison': metadata_comparison,
            'fingerprint_comparison': fingerprint_comparison
        }
        
        reports = generate_all_reports(comparison_results, "./example_results")
        logger.info(f"Reports generated: {list(reports.keys())}")
        
        spark.stop()
        logger.info("Programmatic usage example completed successfully")
        
    except Exception as e:
        logger.error(f"Error in programmatic usage: {str(e)}")

def example_custom_configuration():
    """Example: Using custom configuration."""
    logger.info("=== Example 4: Custom Configuration ===")
    
    try:
        # Create custom configuration
        custom_config = {
            'data_sources': {
                'sql_server': {
                    'server': 'custom-server.database.windows.net',
                    'database': 'custom_db',
                    'table': 'custom_table',
                    'username': 'user',
                    'password': 'pass',
                    'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
                    'url_template': 'jdbc:sqlserver://{server}:1433;database={database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;'
                },
                's3_parquet': {
                    'bucket': 'custom-bucket',
                    'key': 'custom/path/file.parquet',
                    'region': 'us-west-2',
                    'access_key': 'access_key',
                    'secret_key': 'secret_key'
                }
            },
            'comparison_settings': {
                'chunk_size': 500000,
                'max_parallelism': 4,
                'sample_size': 50000,
                'enable_metadata_comparison': True,
                'enable_fingerprinting': True,
                'enable_sampling': True,
                'enable_full_comparison': True,
                'output_path': './custom_results'
            },
            'datasets': [
                {
                    'name': 'custom_dataset',
                    'sql_server': {'table': 'custom_table'},
                    's3_parquet': {'key': 'custom/path/file.parquet'}
                }
            ],
            'spark_config': {
                'app_name': 'CustomDataComparator',
                'master': 'local[*]',
                'executor_memory': '2g',
                'driver_memory': '1g'
            }
        }
        
        # Save custom configuration
        import yaml
        with open('custom_config.yaml', 'w') as f:
            yaml.dump(custom_config, f, default_flow_style=False)
        
        # Run comparison with custom config
        run_comparison('custom_config.yaml')
        logger.info("Custom configuration example completed successfully")
        
    except Exception as e:
        logger.error(f"Error in custom configuration example: {str(e)}")

def example_performance_tuning():
    """Example: Performance tuning for large datasets."""
    logger.info("=== Example 5: Performance Tuning ===")
    
    try:
        # Load base configuration
        config = load_config("config.yaml")
        
        # Optimize for large datasets
        config['comparison_settings']['chunk_size'] = 2000000  # 2M rows per chunk
        config['comparison_settings']['max_parallelism'] = 16  # 16 parallel workers
        config['comparison_settings']['sample_size'] = 200000  # Larger sample
        
        # Optimize Spark configuration
        config['spark_config']['executor_memory'] = '8g'
        config['spark_config']['driver_memory'] = '4g'
        config['spark_config']['max_result_size'] = '4g'
        
        # Save optimized configuration
        import yaml
        with open('optimized_config.yaml', 'w') as f:
            yaml.dump(config, f, default_flow_style=False)
        
        logger.info("Performance tuning configuration created: optimized_config.yaml")
        logger.info("Use this configuration for large datasets (10M+ rows)")
        
    except Exception as e:
        logger.error(f"Error in performance tuning example: {str(e)}")

if __name__ == "__main__":
    """Run all examples."""
    logger.info("Starting Data Comparator Examples")
    
    # Run examples
    example_basic_comparison()
    example_specific_dataset()
    example_programmatic_usage()
    example_custom_configuration()
    example_performance_tuning()
    
    logger.info("All examples completed")
