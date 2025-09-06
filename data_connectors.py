"""
Data source connectors for SQL Server and S3 Parquet files.
Optimized for large datasets with chunking and parallelism support.
"""

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, lit, monotonically_increasing_id, row_number, when, isnan, isnull
import logging
from typing import Dict, Any, Optional, List
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def create_spark_session(config: Dict[str, Any]) -> SparkSession:
    """Create optimized Spark session for data comparison."""
    spark_config = config.get('spark_config', {})
    
    builder = SparkSession.builder \
        .appName(spark_config.get('app_name', 'DataComparator')) \
        .master(spark_config.get('master', 'local[*]'))
    
    # Set memory configurations
    if 'executor_memory' in spark_config:
        builder = builder.config("spark.executor.memory", spark_config['executor_memory'])
    if 'driver_memory' in spark_config:
        builder = builder.config("spark.driver.memory", spark_config['driver_memory'])
    if 'max_result_size' in spark_config:
        builder = builder.config("spark.driver.maxResultSize", spark_config['max_result_size'])
    
    # Enable adaptive query execution
    if spark_config.get('sql_adaptive_enabled', True):
        builder = builder.config("spark.sql.adaptive.enabled", "true")
    if spark_config.get('sql_adaptive_coalesce_partitions_enabled', True):
        builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    return builder.getOrCreate()

def get_sql_server_data(spark: SparkSession, config: Dict[str, Any], 
                        dataset_config: Optional[Dict[str, Any]] = None) -> Any:
    """
    Load data from SQL Server with optimized settings for large datasets.
    
    Args:
        spark: Spark session
        config: Main configuration
        dataset_config: Dataset-specific configuration
    
    Returns:
        DataFrame: SQL Server data
    """
    sql_config = config['data_sources']['sql_server']
    if dataset_config and 'sql_server' in dataset_config:
        sql_config = {**sql_config, **dataset_config['sql_server']}
    
    # Build JDBC URL
    url = sql_config['url_template'].format(
        server=sql_config['server'],
        database=sql_config['database']
    )
    
    # JDBC connection properties
    properties = {
        "user": sql_config['username'],
        "password": sql_config['password'],
        "driver": sql_config['driver'],
        "fetchsize": "10000",  # Optimize fetch size
        "batchsize": "10000",  # Optimize batch size
        "isolationLevel": "READ_UNCOMMITTED"  # Faster reads
    }
    
    table_name = sql_config['table']
    
    try:
        logger.info(f"Loading data from SQL Server table: {table_name}")
        
        # Read data with partitioning for better performance
        df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", table_name) \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .option("driver", properties["driver"]) \
            .option("fetchsize", properties["fetchsize"]) \
            .option("batchsize", properties["batchsize"]) \
            .option("isolationLevel", properties["isolationLevel"]) \
            .option("numPartitions", config['comparison_settings']['max_parallelism']) \
            .load()
        
        # Add row identifier for comparison
        df = df.withColumn("__row_id", monotonically_increasing_id())
        
        logger.info(f"Successfully loaded {df.count()} rows from SQL Server")
        return df
        
    except Exception as e:
        logger.error(f"Error loading data from SQL Server: {str(e)}")
        raise

def get_s3_parquet_data(spark: SparkSession, config: Dict[str, Any], 
                       dataset_config: Optional[Dict[str, Any]] = None) -> Any:
    """
    Load data from S3 Parquet file with optimized settings.
    
    Args:
        spark: Spark session
        config: Main configuration
        dataset_config: Dataset-specific configuration
    
    Returns:
        DataFrame: S3 Parquet data
    """
    s3_config = config['data_sources']['s3_parquet']
    if dataset_config and 's3_parquet' in dataset_config:
        s3_config = {**s3_config, **dataset_config['s3_parquet']}
    
    # Configure S3 access
    spark.conf.set("spark.hadoop.fs.s3a.access.key", s3_config['access_key'])
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", s3_config['secret_key'])
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", f"s3.{s3_config['region']}.amazonaws.com")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", 
                   "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    s3_path = f"s3a://{s3_config['bucket']}/{s3_config['key']}"
    
    try:
        logger.info(f"Loading data from S3: {s3_path}")
        
        # Read parquet with optimized settings
        df = spark.read \
            .option("mergeSchema", "true") \
            .option("spark.sql.parquet.mergeSchema", "true") \
            .parquet(s3_path)
        
        # Add row identifier for comparison
        df = df.withColumn("__row_id", monotonically_increasing_id())
        
        logger.info(f"Successfully loaded {df.count()} rows from S3")
        return df
        
    except Exception as e:
        logger.error(f"Error loading data from S3: {str(e)}")
        raise

def get_data_sample(df: Any, sample_size: int, sampling_strategy: str = "random") -> Any:
    """
    Get a sample of the data for initial analysis.
    
    Args:
        df: Input DataFrame
        sample_size: Number of rows to sample
        sampling_strategy: Strategy for sampling (random, systematic)
    
    Returns:
        DataFrame: Sampled data
    """
    total_rows = df.count()
    
    if total_rows <= sample_size:
        logger.info("Dataset size is smaller than sample size, returning full dataset")
        return df
    
    if sampling_strategy == "random":
        # Random sampling
        sample_ratio = min(sample_size / total_rows, 1.0)
        return df.sample(withReplacement=False, fraction=sample_ratio, seed=42)
    
    elif sampling_strategy == "systematic":
        # Systematic sampling - every nth row
        step = total_rows // sample_size
        return df.filter((col("__row_id") % step) == 0).limit(sample_size)
    
    else:
        raise ValueError(f"Unsupported sampling strategy: {sampling_strategy}")

def get_data_metadata(df: Any) -> Dict[str, Any]:
    """
    Extract metadata from DataFrame.
    
    Args:
        df: Input DataFrame
    
    Returns:
        Dict: Metadata information
    """
    try:
        # Get schema information
        schema_info = []
        for field in df.schema.fields:
            schema_info.append({
                "name": field.name,
                "type": str(field.dataType),
                "nullable": field.nullable
            })
        
        # Get basic statistics
        row_count = df.count()
        column_count = len(df.columns)
        
        # Get null counts per column
        null_counts = {}
        for col_name in df.columns:
            null_count = df.filter(isnull(col(col_name))).count()
            null_counts[col_name] = null_count
        
        metadata = {
            "row_count": row_count,
            "column_count": column_count,
            "schema": schema_info,
            "null_counts": null_counts,
            "columns": df.columns
        }
        
        logger.info(f"Metadata extracted: {row_count} rows, {column_count} columns")
        return metadata
        
    except Exception as e:
        logger.error(f"Error extracting metadata: {str(e)}")
        raise

def chunk_dataframe(df: Any, chunk_size: int) -> List[Any]:
    """
    Split DataFrame into chunks for parallel processing.
    
    Args:
        df: Input DataFrame
        chunk_size: Size of each chunk
    
    Returns:
        List[DataFrame]: List of chunked DataFrames
    """
    try:
        total_rows = df.count()
        num_chunks = (total_rows + chunk_size - 1) // chunk_size
        
        logger.info(f"Splitting {total_rows} rows into {num_chunks} chunks of size {chunk_size}")
        
        # Add chunk identifier
        df_with_chunk = df.withColumn("__chunk_id", (col("__row_id") / chunk_size).cast("int"))
        
        chunks = []
        for i in range(num_chunks):
            chunk_df = df_with_chunk.filter(col("__chunk_id") == i).drop("__chunk_id")
            chunks.append(chunk_df)
        
        return chunks
        
    except Exception as e:
        logger.error(f"Error chunking DataFrame: {str(e)}")
        raise
