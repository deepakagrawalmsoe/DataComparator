"""
Simple test script for the Data Comparator.
Tests basic functionality without requiring actual data sources.
"""

import logging
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import monotonically_increasing_id

# Import our modules
from metadata_comparator import compare_metadata, get_data_metadata
from fingerprinting_sampler import create_data_fingerprint, compare_fingerprints
from report_generator import generate_all_reports

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_test_data(spark):
    """Create test data for comparison."""
    
    # Schema for test data
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True)
    ])
    
    # Test data for dataset 1
    data1 = [
        Row(id=1, name="Alice", age=25, city="New York"),
        Row(id=2, name="Bob", age=30, city="Los Angeles"),
        Row(id=3, name="Charlie", age=35, city="Chicago"),
        Row(id=4, name="Diana", age=28, city="Houston"),
        Row(id=5, name="Eve", age=32, city="Phoenix")
    ]
    
    # Test data for dataset 2 (slightly different)
    data2 = [
        Row(id=1, name="Alice", age=25, city="New York"),
        Row(id=2, name="Bob", age=30, city="Los Angeles"),
        Row(id=3, name="Charlie", age=35, city="Chicago"),
        Row(id=4, name="Diana", age=28, city="Houston"),
        Row(id=6, name="Frank", age=29, city="Philadelphia")  # Different row
    ]
    
    # Create DataFrames
    df1 = spark.createDataFrame(data1, schema)
    df2 = spark.createDataFrame(data2, schema)
    
    # Add row IDs
    df1 = df1.withColumn("__row_id", monotonically_increasing_id())
    df2 = df2.withColumn("__row_id", monotonically_increasing_id())
    
    return df1, df2

def test_metadata_comparison(df1, df2):
    """Test metadata comparison functionality."""
    logger.info("Testing metadata comparison...")
    
    try:
        # Get metadata
        metadata1 = get_data_metadata(df1)
        metadata2 = get_data_metadata(df2)
        
        # Compare metadata
        result = compare_metadata(metadata1, metadata2)
        
        logger.info(f"Metadata comparison result: {result['overall_match']}")
        logger.info(f"Row count difference: {result['row_count_comparison']['difference']}")
        logger.info(f"Column count difference: {result['column_count_comparison']['difference']}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in metadata comparison test: {str(e)}")
        raise

def test_fingerprinting_comparison(df1, df2):
    """Test fingerprinting comparison functionality."""
    logger.info("Testing fingerprinting comparison...")
    
    try:
        # Create fingerprints
        df1_fp = create_data_fingerprint(df1, algorithm="md5")
        df2_fp = create_data_fingerprint(df2, algorithm="md5")
        
        # Compare fingerprints
        result = compare_fingerprints(df1_fp, df2_fp)
        
        logger.info(f"Fingerprint comparison result: {result['fingerprints_match']}")
        logger.info(f"Match percentage: {result['match_percentage']}%")
        logger.info(f"Common fingerprints: {result['common_fingerprints']}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in fingerprinting comparison test: {str(e)}")
        raise

def test_report_generation(comparison_results):
    """Test report generation functionality."""
    logger.info("Testing report generation...")
    
    try:
        # Generate reports
        reports = generate_all_reports(comparison_results, "./test_results")
        
        logger.info(f"Reports generated successfully:")
        for report_type, report_path in reports.items():
            logger.info(f"  {report_type}: {report_path}")
        
        return reports
        
    except Exception as e:
        logger.error(f"Error in report generation test: {str(e)}")
        raise

def run_tests():
    """Run all tests."""
    logger.info("Starting Data Comparator Tests")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("DataComparatorTest") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        # Create test data
        logger.info("Creating test data...")
        df1, df2 = create_test_data(spark)
        
        logger.info(f"Dataset 1: {df1.count()} rows, {len(df1.columns)} columns")
        logger.info(f"Dataset 2: {df2.count()} rows, {len(df2.columns)} columns")
        
        # Test metadata comparison
        metadata_result = test_metadata_comparison(df1, df2)
        
        # Test fingerprinting comparison
        fingerprint_result = test_fingerprinting_comparison(df1, df2)
        
        # Prepare comparison results
        comparison_results = {
            'dataset_name': 'test_dataset',
            'metadata_comparison': metadata_result,
            'fingerprint_comparison': fingerprint_result,
            'test_mode': True
        }
        
        # Test report generation
        reports = test_report_generation(comparison_results)
        
        logger.info("All tests completed successfully!")
        
        # Print summary
        print("\n" + "="*50)
        print("TEST SUMMARY")
        print("="*50)
        print(f"Metadata Match: {metadata_result['overall_match']}")
        print(f"Fingerprint Match: {fingerprint_result['fingerprints_match']}")
        print(f"Match Percentage: {fingerprint_result['match_percentage']}%")
        print(f"Reports Generated: {len(reports)}")
        print("="*50)
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    run_tests()
