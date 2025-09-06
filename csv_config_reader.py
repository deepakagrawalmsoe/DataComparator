"""
CSV Configuration Reader for datasets.
Handles reading and parsing CSV-based dataset configurations.
"""

import csv
import logging
from typing import Dict, Any, List, Optional
import os

logger = logging.getLogger(__name__)

def load_datasets_from_csv(csv_path: str = "datasets.csv") -> Dict[str, Any]:
    """
    Load datasets configuration from CSV file.
    
    Args:
        csv_path: Path to CSV file containing dataset configurations
    
    Returns:
        Dict: Parsed datasets configuration
    """
    logger.info(f"Loading datasets configuration from CSV: {csv_path}")
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Datasets CSV file not found: {csv_path}")
    
    datasets = []
    global_settings = {'overrides': {}}
    
    try:
        with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 for header
                try:
                    # Parse dataset configuration
                    dataset_config = parse_dataset_row(row, row_num)
                    datasets.append(dataset_config)
                    
                    # Check for overrides
                    overrides = extract_overrides(row)
                    if overrides:
                        global_settings['overrides'][dataset_config['name']] = overrides
                        
                except Exception as e:
                    logger.error(f"Error parsing row {row_num} in CSV: {str(e)}")
                    logger.error(f"Row data: {row}")
                    continue
        
        result = {
            'datasets': datasets,
            'global_settings': global_settings
        }
        
        logger.info(f"Successfully loaded {len(datasets)} datasets from CSV")
        return result
        
    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")
        raise

def parse_dataset_row(row: Dict[str, str], row_num: int) -> Dict[str, Any]:
    """
    Parse a single row from the CSV into dataset configuration.
    
    Args:
        row: Dictionary representing a CSV row
        row_num: Row number for error reporting
    
    Returns:
        Dict: Parsed dataset configuration
    """
    # Required fields
    required_fields = ['name', 'sql_server_table', 's3_parquet_key']
    for field in required_fields:
        if not row.get(field, '').strip():
            raise ValueError(f"Required field '{field}' is missing or empty in row {row_num}")
    
    # Parse dataset configuration
    dataset_config = {
        'name': row['name'].strip(),
        'description': row.get('description', '').strip() or f"Dataset: {row['name']}",
        'sql_server': {
            'table': row['sql_server_table'].strip()
        },
        's3_parquet': {
            'key': row['s3_parquet_key'].strip()
        }
    }
    
    # Add notes if provided
    if row.get('notes', '').strip():
        dataset_config['notes'] = row['notes'].strip()
    
    return dataset_config

def extract_overrides(row: Dict[str, str]) -> Dict[str, Any]:
    """
    Extract override settings from a CSV row.
    
    Args:
        row: Dictionary representing a CSV row
    
    Returns:
        Dict: Override settings (empty if none)
    """
    overrides = {}
    
    # Check for override fields
    override_fields = {
        'chunk_size_override': 'chunk_size',
        'max_parallelism_override': 'max_parallelism',
        'sample_size_override': 'sample_size',
        'enable_metadata_comparison': 'enable_metadata_comparison',
        'enable_fingerprinting': 'enable_fingerprinting',
        'enable_sampling': 'enable_sampling',
        'enable_full_comparison': 'enable_full_comparison'
    }
    
    for csv_field, config_field in override_fields.items():
        value = row.get(csv_field, '').strip()
        if value:
            # Convert string values to appropriate types
            if value.lower() in ['true', 'false']:
                overrides[config_field] = value.lower() == 'true'
            elif value.isdigit():
                overrides[config_field] = int(value)
            else:
                # Keep as string for other values
                overrides[config_field] = value
    
    return overrides

def validate_csv_structure(csv_path: str) -> bool:
    """
    Validate that the CSV file has the required structure.
    
    Args:
        csv_path: Path to CSV file
    
    Returns:
        bool: True if valid, False otherwise
    """
    try:
        with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            headers = reader.fieldnames
            
            required_headers = ['name', 'sql_server_table', 's3_parquet_key']
            optional_headers = [
                'description', 'chunk_size_override', 'max_parallelism_override',
                'enable_metadata_comparison', 'enable_fingerprinting', 
                'enable_sampling', 'enable_full_comparison', 'sample_size_override', 'notes'
            ]
            
            # Check required headers
            missing_headers = [h for h in required_headers if h not in headers]
            if missing_headers:
                logger.error(f"Missing required headers: {missing_headers}")
                return False
            
            # Check for unknown headers
            all_valid_headers = required_headers + optional_headers
            unknown_headers = [h for h in headers if h not in all_valid_headers]
            if unknown_headers:
                logger.warning(f"Unknown headers found: {unknown_headers}")
            
            logger.info("CSV structure validation passed")
            return True
            
    except Exception as e:
        logger.error(f"Error validating CSV structure: {str(e)}")
        return False

def create_sample_csv(output_path: str = "datasets_sample.csv") -> str:
    """
    Create a sample CSV file with example data.
    
    Args:
        output_path: Path where to create the sample file
    
    Returns:
        str: Path to created sample file
    """
    sample_data = [
        {
            'name': 'customer_data',
            'description': 'Customer master data comparison',
            'sql_server_table': 'customers',
            's3_parquet_key': 'data/customers.parquet',
            'chunk_size_override': '',
            'max_parallelism_override': '',
            'enable_metadata_comparison': '',
            'enable_fingerprinting': '',
            'enable_sampling': '',
            'enable_full_comparison': '',
            'sample_size_override': '',
            'notes': 'Primary customer dataset'
        },
        {
            'name': 'order_data',
            'description': 'Order transaction data comparison',
            'sql_server_table': 'orders',
            's3_parquet_key': 'data/orders.parquet',
            'chunk_size_override': '2000000',
            'max_parallelism_override': '16',
            'enable_metadata_comparison': 'true',
            'enable_fingerprinting': 'true',
            'enable_sampling': 'true',
            'enable_full_comparison': 'true',
            'sample_size_override': '200000',
            'notes': 'Large dataset - optimized settings'
        },
        {
            'name': 'product_categories',
            'description': 'Product category data comparison',
            'sql_server_table': 'product_categories',
            's3_parquet_key': 'data/product_categories.parquet',
            'chunk_size_override': '',
            'max_parallelism_override': '',
            'enable_metadata_comparison': 'true',
            'enable_fingerprinting': 'true',
            'enable_sampling': 'false',
            'enable_full_comparison': 'false',
            'sample_size_override': '',
            'notes': 'Small dataset - metadata only'
        }
    ]
    
    try:
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'name', 'description', 'sql_server_table', 's3_parquet_key',
                'chunk_size_override', 'max_parallelism_override',
                'enable_metadata_comparison', 'enable_fingerprinting',
                'enable_sampling', 'enable_full_comparison', 'sample_size_override', 'notes'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(sample_data)
        
        logger.info(f"Sample CSV created: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Error creating sample CSV: {str(e)}")
        raise

def convert_yaml_to_csv(yaml_path: str, csv_path: str) -> str:
    """
    Convert existing YAML datasets configuration to CSV format.
    
    Args:
        yaml_path: Path to existing YAML file
        csv_path: Path where to create CSV file
    
    Returns:
        str: Path to created CSV file
    """
    import yaml
    
    try:
        # Load YAML configuration
        with open(yaml_path, 'r') as f:
            yaml_config = yaml.safe_load(f)
        
        datasets = yaml_config.get('datasets', [])
        global_settings = yaml_config.get('global_settings', {})
        overrides = global_settings.get('overrides', {})
        
        # Convert to CSV format
        csv_data = []
        for dataset in datasets:
            row = {
                'name': dataset.get('name', ''),
                'description': dataset.get('description', ''),
                'sql_server_table': dataset.get('sql_server', {}).get('table', ''),
                's3_parquet_key': dataset.get('s3_parquet', {}).get('key', ''),
                'chunk_size_override': '',
                'max_parallelism_override': '',
                'enable_metadata_comparison': '',
                'enable_fingerprinting': '',
                'enable_sampling': '',
                'enable_full_comparison': '',
                'sample_size_override': '',
                'notes': ''
            }
            
            # Add overrides if they exist
            dataset_name = dataset.get('name', '')
            if dataset_name in overrides:
                override_data = overrides[dataset_name]
                row.update({
                    'chunk_size_override': str(override_data.get('chunk_size', '')),
                    'max_parallelism_override': str(override_data.get('max_parallelism', '')),
                    'sample_size_override': str(override_data.get('sample_size', '')),
                    'enable_metadata_comparison': str(override_data.get('enable_metadata_comparison', '')).lower(),
                    'enable_fingerprinting': str(override_data.get('enable_fingerprinting', '')).lower(),
                    'enable_sampling': str(override_data.get('enable_sampling', '')).lower(),
                    'enable_full_comparison': str(override_data.get('enable_full_comparison', '')).lower()
                })
            
            csv_data.append(row)
        
        # Write CSV file
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'name', 'description', 'sql_server_table', 's3_parquet_key',
                'chunk_size_override', 'max_parallelism_override',
                'enable_metadata_comparison', 'enable_fingerprinting',
                'enable_sampling', 'enable_full_comparison', 'sample_size_override', 'notes'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_data)
        
        logger.info(f"Successfully converted YAML to CSV: {csv_path}")
        return csv_path
        
    except Exception as e:
        logger.error(f"Error converting YAML to CSV: {str(e)}")
        raise
