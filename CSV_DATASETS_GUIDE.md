# CSV Datasets Configuration Guide

## Overview

The Data Comparator now supports CSV-based dataset configuration for better manageability when dealing with large numbers of datasets (up to 100+). This approach provides significant advantages over YAML for large-scale configurations.

## Benefits of CSV Format

### ✅ **Scalability**
- **Easy to manage** 100+ datasets in a single file
- **Spreadsheet-friendly** for non-technical users
- **Bulk editing** capabilities
- **Import/export** from Excel, Google Sheets, etc.

### ✅ **Maintainability**
- **Version control friendly** with clear diffs
- **Column-based editing** for consistent updates
- **Validation tools** to ensure data integrity
- **Template-based** creation and management

### ✅ **Usability**
- **Visual organization** in tabular format
- **Search and filter** capabilities in spreadsheet applications
- **Sorting** by any column
- **Copy/paste** operations for bulk changes

## CSV Structure

### Required Columns
- `name`: Unique dataset identifier
- `sql_server_table`: SQL Server table name
- `s3_parquet_key`: S3 Parquet file path

### Optional Columns
- `description`: Human-readable description
- `chunk_size_override`: Override chunk size (integer)
- `max_parallelism_override`: Override parallelism (integer)
- `enable_metadata_comparison`: Enable/disable metadata phase (true/false)
- `enable_fingerprinting`: Enable/disable fingerprinting phase (true/false)
- `enable_sampling`: Enable/disable sampling phase (true/false)
- `enable_full_comparison`: Enable/disable full comparison phase (true/false)
- `sample_size_override`: Override sample size (integer)
- `notes`: Additional notes about the dataset

## Example CSV

```csv
name,description,sql_server_table,s3_parquet_key,chunk_size_override,max_parallelism_override,enable_metadata_comparison,enable_fingerprinting,enable_sampling,enable_full_comparison,sample_size_override,notes
customer_data,Customer master data comparison,customers,data/customers.parquet,,,,,,,,,
product_data,Product catalog comparison,products,data/products.parquet,,,,,,,,,
order_data,Order transaction data comparison,orders,data/orders.parquet,2000000,16,true,true,true,true,200000,Large dataset - optimized settings
inventory_data,Inventory levels comparison,inventory,data/inventory.parquet,,,,,,,,,
financial_data,Financial transactions comparison,financial_transactions,data/financial_transactions.parquet,,,,,,,,,
```

## Usage

### Basic Usage
```bash
# Use CSV datasets (default)
python data_comparator.py

# Use specific CSV file
python data_comparator.py --datasets my_datasets.csv

# Compare specific dataset
python data_comparator.py --dataset customer_data
```

### Dataset Management
```bash
# List all datasets
python manage_datasets.py list

# Validate CSV structure
python manage_datasets.py validate

# Create sample template
python manage_datasets.py sample --output my_datasets.csv

# Convert from YAML to CSV
python manage_datasets.py convert --yaml datasets.yaml --output datasets.csv

# Show dataset details
python manage_datasets.py details --name customer_data
```

## Large-Scale Examples

### 100 Datasets Example
```bash
# Create example with 100 datasets
python create_large_datasets_example.py

# This creates:
# - datasets_100_example.csv (100 random datasets)
# - datasets_enterprise.csv (25 enterprise datasets)
```

### Enterprise Naming Convention
For enterprise environments, consider using structured naming:

```csv
name,description,sql_server_table,s3_parquet_key
CUST_MASTER,Customer master data,customers,data/customer/master.parquet
PROD_CATALOG,Product catalog,products,data/product/catalog.parquet
ORD_HEADERS,Order headers,order_headers,data/order/headers.parquet
FIN_TRANSACTIONS,Financial transactions,financial_transactions,data/financial/transactions.parquet
```

## Best Practices

### 1. **Naming Conventions**
- Use consistent naming patterns
- Include domain/area prefixes
- Use underscores for separation
- Keep names descriptive but concise

### 2. **Organization**
- Group related datasets together
- Use consistent S3 key patterns
- Add meaningful descriptions
- Include notes for special cases

### 3. **Performance Optimization**
- Set appropriate chunk sizes for large datasets
- Adjust parallelism based on data size
- Disable unnecessary phases for reference data
- Use sampling for very large datasets

### 4. **Validation**
- Always validate CSV structure before running
- Check for required fields
- Verify S3 keys and table names
- Test with small subsets first

## Migration from YAML

### Automatic Conversion
```bash
# Convert existing YAML to CSV
python manage_datasets.py convert --yaml datasets.yaml --output datasets.csv
```

### Manual Migration
1. Create CSV with required columns
2. Copy dataset information from YAML
3. Add any overrides as separate columns
4. Validate the CSV structure
5. Test with a few datasets first

## Troubleshooting

### Common Issues

1. **Missing Required Fields**
   - Ensure `name`, `sql_server_table`, and `s3_parquet_key` are populated
   - Use validation tool to check

2. **Invalid Override Values**
   - Boolean values should be `true`/`false`
   - Numeric values should be valid integers
   - Empty values are allowed for optional fields

3. **CSV Format Issues**
   - Ensure proper CSV escaping
   - Check for missing quotes around values with commas
   - Verify line endings

### Validation Commands
```bash
# Validate structure
python manage_datasets.py validate

# List datasets to check
python manage_datasets.py list

# Show specific dataset details
python manage_datasets.py details --name dataset_name
```

## Performance Considerations

### Large Datasets (100+)
- Use CSV format for better management
- Consider dataset-specific overrides
- Group similar datasets for batch processing
- Use appropriate chunk sizes and parallelism

### Memory Management
- Monitor memory usage with large datasets
- Adjust chunk sizes based on available memory
- Use sampling for initial analysis
- Consider processing in batches

This CSV-based approach makes the Data Comparator much more suitable for enterprise environments with large numbers of datasets while maintaining all the powerful comparison capabilities.
