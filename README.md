# Data Comparator

A high-performance, scalable data comparison tool built with PySpark for comparing large datasets from SQL Server and S3 Parquet files. The tool is designed to handle datasets with up to 150 columns and 20 million rows efficiently using advanced techniques like metadata comparison, fingerprinting, sampling, and parallel processing.

## Features

- **Multi-Phase Comparison**: Metadata → Fingerprinting → Sampling → Full Comparison
- **High Performance**: Optimized for large datasets with chunking and parallelism
- **Multiple Data Sources**: SQL Server and S3 Parquet support
- **Advanced Techniques**: 
  - Metadata comparison (schema, data types, null counts)
  - Data fingerprinting for quick comparison
  - Statistical sampling for initial analysis
  - Data drift detection
  - Full row-by-row comparison with chunking
- **Comprehensive Reporting**: JSON, CSV, and HTML reports
- **Configurable**: YAML-based configuration for multiple datasets
- **Scalable**: Supports multiple datasets and parallel processing

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Download and install Apache Spark:
   - Download Spark 3.5.0 from [Apache Spark website](https://spark.apache.org/downloads.html)
   - Extract and set `SPARK_HOME` environment variable
   - Add Spark binaries to your PATH

3. Install JDBC driver for SQL Server:
   - Download `mssql-jdbc-12.4.0.jre8.jar` from Microsoft
   - Place it in Spark's `jars` directory

## Configuration

The tool uses two configuration files:

### Main Configuration (`config.yaml`)
Configure data sources and comparison settings:

```yaml
data_sources:
  sql_server:
    server: "your-sql-server.database.windows.net"
    database: "your_database"
    username: "your_username"
    password: "your_password"
    
  s3_parquet:
    bucket: "your-s3-bucket"
    region: "us-east-1"
    access_key: "your_access_key"
    secret_key: "your_secret_key"

comparison_settings:
  chunk_size: 1000000
  max_parallelism: 8
  sample_size: 100000
  enable_metadata_comparison: true
  enable_fingerprinting: true
  enable_sampling: true
  enable_full_comparison: true
```

### Datasets Configuration (`datasets.csv`)
Define multiple datasets for comparison using a tabular CSV format:

```csv
name,description,sql_server_table,s3_parquet_key,chunk_size_override,max_parallelism_override,enable_metadata_comparison,enable_fingerprinting,enable_sampling,enable_full_comparison,sample_size_override,notes
customer_data,Customer master data comparison,customers,data/customers.parquet,,,,,,,,,
product_data,Product catalog comparison,products,data/products.parquet,,,,,,,,,
order_data,Order transaction data comparison,orders,data/orders.parquet,2000000,16,true,true,true,true,200000,Large dataset - optimized settings
inventory_data,Inventory levels comparison,inventory,data/inventory.parquet,,,,,,,,,
financial_data,Financial transactions comparison,financial_transactions,data/financial_transactions.parquet,,,,,,,,,
```

**CSV Format Benefits:**
- **Easy to manage** with 100+ datasets
- **Spreadsheet-friendly** for non-technical users
- **Version control friendly** with clear diffs
- **Bulk editing** capabilities
- **Import/export** from other tools

**CSV Columns:**
- `name`: Dataset identifier (required)
- `description`: Human-readable description
- `sql_server_table`: SQL Server table name (required)
- `s3_parquet_key`: S3 Parquet file path (required)
- `chunk_size_override`: Override chunk size for this dataset
- `max_parallelism_override`: Override parallelism for this dataset
- `enable_*_override`: Override comparison phases for this dataset
- `sample_size_override`: Override sample size for this dataset
- `notes`: Additional notes about the dataset

## Usage

### Basic Usage

Compare all datasets defined in configuration:
```bash
python data_comparator.py
```

### Compare Specific Dataset

Compare only a specific dataset:
```bash
python data_comparator.py --dataset customer_data
```

### Custom Configuration Files

Use custom configuration files:
```bash
python data_comparator.py --config my_config.yaml --datasets my_datasets.csv
```

### Dataset Management

Use the dataset management utility for easier CSV management:

```bash
# List all datasets
python manage_datasets.py list

# Validate CSV structure
python manage_datasets.py validate

# Create sample CSV template
python manage_datasets.py sample --output my_datasets.csv

# Convert from YAML to CSV
python manage_datasets.py convert --yaml datasets.yaml --output datasets.csv

# Show details for specific dataset
python manage_datasets.py details --name customer_data
```

### Verbose Logging

Enable detailed logging:
```bash
python data_comparator.py --verbose
```

## Architecture

### Components

1. **Data Connectors** (`data_connectors.py`)
   - SQL Server connectivity with JDBC
   - S3 Parquet file reading
   - Data sampling and chunking utilities

2. **Metadata Comparator** (`metadata_comparator.py`)
   - Schema comparison
   - Data type validation
   - Null count analysis
   - Column statistics comparison

3. **Fingerprinting & Sampling** (`fingerprinting_sampler.py`)
   - Data fingerprinting using MD5/SHA256/XXHash
   - Multiple sampling strategies (random, systematic, stratified)
   - Data drift detection

4. **Comparison Engine** (`comparison_engine.py`)
   - Full data comparison with chunking
   - Parallel processing support
   - Row-by-row comparison

5. **Report Generator** (`report_generator.py`)
   - JSON summary reports
   - CSV detailed reports
   - HTML web reports
   - Column-level analysis

6. **Main Orchestrator** (`data_comparator.py`)
   - Coordinates all phases
   - Manages Spark sessions
   - Handles multiple datasets

### Comparison Phases

1. **Metadata Comparison**
   - Compare schemas and data types
   - Analyze null counts and basic statistics
   - Identify structural differences

2. **Fingerprinting Comparison**
   - Create data fingerprints for quick comparison
   - Identify exact matches and differences
   - Fast initial assessment

3. **Sampling Comparison**
   - Statistical sampling for detailed analysis
   - Data drift detection
   - Performance optimization

4. **Full Comparison**
   - Complete row-by-row comparison
   - Chunked processing for large datasets
   - Parallel execution

## Performance Optimization

### Chunking Strategy
- Datasets are split into configurable chunks (default: 1M rows)
- Each chunk is processed independently
- Enables parallel processing and memory management

### Parallelism
- Configurable parallelism level (default: 8 workers)
- ThreadPoolExecutor for chunk processing
- Spark's built-in parallelization

### Memory Management
- Optimized Spark configurations
- Adaptive query execution
- Efficient data structures

## Output Reports

The tool generates both individual dataset reports and consolidated reports:

### Individual Dataset Reports
Each dataset gets its own set of reports in `./comparison_results/individual/{dataset_name}/`:

- **Summary Report (JSON)**: Executive summary with key metrics
- **CSV Report**: Detailed metrics in tabular format
- **HTML Report**: Web-friendly visualization
- **Detailed Column Report**: Column-level statistics

### Consolidated Reports
Overall summary reports in `./comparison_results/consolidated/`:

- **Consolidated Summary (JSON)**: Complete overview of all datasets
- **Consolidated CSV**: Summary table of all datasets
- **Consolidated HTML**: Executive dashboard with all results

### Report Features
- Executive summary with key metrics
- Performance statistics across all datasets
- Success/failure tracking
- Dataset-specific overrides and configurations
- Color-coded results for easy interpretation

## Configuration Options

### Data Source Settings
- **SQL Server**: Server, database, table, credentials
- **S3**: Bucket, key, region, credentials

### Comparison Settings
- **chunk_size**: Rows per chunk (default: 1,000,000)
- **max_parallelism**: Parallel workers (default: 8)
- **sample_size**: Sample size for analysis (default: 100,000)
- **enable_metadata_comparison**: Enable/disable metadata phase
- **enable_fingerprinting**: Enable/disable fingerprinting phase
- **enable_sampling**: Enable/disable sampling phase
- **enable_full_comparison**: Enable/disable full comparison phase

### Fingerprinting Settings
- **fingerprint_columns**: Specific columns to fingerprint (empty = all)
- **fingerprint_algorithm**: Hashing algorithm (md5, sha256, xxhash)

### Sampling Settings
- **sampling_strategy**: Strategy (random, systematic, stratified)
- **sampling_ratio**: Sample ratio (default: 0.01 = 1%)

## Error Handling

- Comprehensive logging at all levels
- Graceful error handling with detailed messages
- Partial results preservation on errors
- Retry mechanisms for transient failures

## Monitoring and Logging

- Detailed logging to console and file
- Performance metrics tracking
- Progress indicators for long-running operations
- Error reporting with context

## Examples

### Example 1: Basic Comparison
```bash
# Compare all datasets in config.yaml
python data_comparator.py
```

### Example 2: Specific Dataset
```bash
# Compare only customer data
python data_comparator.py --dataset customer_data
```

### Example 3: Custom Configuration
```bash
# Use custom config with verbose logging
python data_comparator.py --config production_config.yaml --verbose
```

## Troubleshooting

### Common Issues

1. **Spark Session Issues**
   - Ensure Spark is properly installed
   - Check SPARK_HOME environment variable
   - Verify Java version compatibility

2. **SQL Server Connection**
   - Verify credentials and server details
   - Check network connectivity
   - Ensure JDBC driver is in Spark jars directory

3. **S3 Access Issues**
   - Verify AWS credentials
   - Check bucket permissions
   - Ensure region is correct

4. **Memory Issues**
   - Reduce chunk_size in configuration
   - Increase driver/executor memory
   - Enable adaptive query execution

### Performance Tuning

1. **For Large Datasets**
   - Increase chunk_size (up to 5M rows)
   - Increase max_parallelism (up to 16)
   - Use more powerful hardware

2. **For Memory Constraints**
   - Decrease chunk_size
   - Reduce max_parallelism
   - Enable adaptive query execution

3. **For Network Issues**
   - Increase fetch/batch sizes
   - Use compression
   - Optimize data formats

## License

This project is licensed under the MIT License.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review the logs for error details
3. Create an issue with detailed information
4. Include configuration and error logs
