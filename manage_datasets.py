"""
Dataset Management Utility
Provides tools to manage CSV-based dataset configurations.
"""

import argparse
import logging
from csv_config_reader import (
    load_datasets_from_csv, validate_csv_structure, 
    create_sample_csv, convert_yaml_to_csv
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def list_datasets(csv_path: str = "datasets.csv"):
    """List all datasets in the CSV file."""
    try:
        datasets_config = load_datasets_from_csv(csv_path)
        datasets = datasets_config['datasets']
        
        print(f"\nFound {len(datasets)} datasets in {csv_path}:")
        print("-" * 80)
        print(f"{'Name':<20} {'Description':<30} {'SQL Table':<15} {'S3 Key':<20}")
        print("-" * 80)
        
        for dataset in datasets:
            print(f"{dataset['name']:<20} {dataset['description'][:30]:<30} "
                  f"{dataset['sql_server']['table']:<15} {dataset['s3_parquet']['key'][:20]:<20}")
        
        # Show overrides
        overrides = datasets_config['global_settings']['overrides']
        if overrides:
            print(f"\nDataset-specific overrides:")
            for name, override in overrides.items():
                print(f"  {name}: {override}")
        
    except Exception as e:
        logger.error(f"Error listing datasets: {str(e)}")

def validate_datasets(csv_path: str = "datasets.csv"):
    """Validate the CSV datasets file."""
    try:
        if validate_csv_structure(csv_path):
            print(f"✓ CSV structure is valid: {csv_path}")
            
            # Try to load datasets
            datasets_config = load_datasets_from_csv(csv_path)
            datasets = datasets_config['datasets']
            print(f"✓ Successfully loaded {len(datasets)} datasets")
            
            # Check for required fields
            missing_info = []
            for dataset in datasets:
                if not dataset.get('name'):
                    missing_info.append(f"Dataset missing name")
                if not dataset.get('sql_server', {}).get('table'):
                    missing_info.append(f"Dataset '{dataset.get('name', 'unknown')}' missing SQL table")
                if not dataset.get('s3_parquet', {}).get('key'):
                    missing_info.append(f"Dataset '{dataset.get('name', 'unknown')}' missing S3 key")
            
            if missing_info:
                print("⚠ Issues found:")
                for issue in missing_info:
                    print(f"  - {issue}")
            else:
                print("✓ All datasets have required information")
                
        else:
            print(f"✗ CSV structure is invalid: {csv_path}")
            
    except Exception as e:
        logger.error(f"Error validating datasets: {str(e)}")

def create_sample(csv_path: str = "datasets_sample.csv"):
    """Create a sample CSV file."""
    try:
        create_sample_csv(csv_path)
        print(f"✓ Sample CSV created: {csv_path}")
        print("You can use this as a template for your own datasets.")
        
    except Exception as e:
        logger.error(f"Error creating sample: {str(e)}")

def convert_from_yaml(yaml_path: str = "datasets.yaml", csv_path: str = "datasets.csv"):
    """Convert existing YAML datasets to CSV format."""
    try:
        convert_yaml_to_csv(yaml_path, csv_path)
        print(f"✓ Converted {yaml_path} to {csv_path}")
        print("You can now use the CSV format for easier management.")
        
    except Exception as e:
        logger.error(f"Error converting from YAML: {str(e)}")

def show_dataset_details(csv_path: str = "datasets.csv", dataset_name: str = None):
    """Show detailed information about a specific dataset."""
    try:
        datasets_config = load_datasets_from_csv(csv_path)
        datasets = datasets_config['datasets']
        
        if dataset_name:
            # Find specific dataset
            dataset = next((d for d in datasets if d['name'] == dataset_name), None)
            if not dataset:
                print(f"Dataset '{dataset_name}' not found")
                return
            
            print(f"\nDataset Details: {dataset['name']}")
            print("-" * 40)
            print(f"Name: {dataset['name']}")
            print(f"Description: {dataset['description']}")
            print(f"SQL Server Table: {dataset['sql_server']['table']}")
            print(f"S3 Parquet Key: {dataset['s3_parquet']['key']}")
            if 'notes' in dataset:
                print(f"Notes: {dataset['notes']}")
            
            # Show overrides
            overrides = datasets_config['global_settings']['overrides'].get(dataset_name, {})
            if overrides:
                print(f"Overrides: {overrides}")
            else:
                print("No overrides configured")
        else:
            # Show summary of all datasets
            print(f"\nDataset Summary ({len(datasets)} datasets):")
            print("-" * 60)
            for dataset in datasets:
                overrides = datasets_config['global_settings']['overrides'].get(dataset['name'], {})
                override_count = len(overrides)
                print(f"{dataset['name']:<20} | {override_count} overrides | {dataset['description']}")
        
    except Exception as e:
        logger.error(f"Error showing dataset details: {str(e)}")

def main():
    """Main entry point for dataset management."""
    parser = argparse.ArgumentParser(description='Dataset Management Utility')
    parser.add_argument('--csv', default='datasets.csv', 
                       help='Path to CSV datasets file')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List all datasets')
    list_parser.set_defaults(func=lambda args: list_datasets(args.csv))
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate CSV structure')
    validate_parser.set_defaults(func=lambda args: validate_datasets(args.csv))
    
    # Create sample command
    sample_parser = subparsers.add_parser('sample', help='Create sample CSV file')
    sample_parser.add_argument('--output', default='datasets_sample.csv',
                              help='Output file path for sample')
    sample_parser.set_defaults(func=lambda args: create_sample(args.output))
    
    # Convert command
    convert_parser = subparsers.add_parser('convert', help='Convert YAML to CSV')
    convert_parser.add_argument('--yaml', default='datasets.yaml',
                               help='Input YAML file')
    convert_parser.add_argument('--output', default='datasets.csv',
                               help='Output CSV file')
    convert_parser.set_defaults(func=lambda args: convert_from_yaml(args.yaml, args.output))
    
    # Details command
    details_parser = subparsers.add_parser('details', help='Show dataset details')
    details_parser.add_argument('--name', help='Specific dataset name')
    details_parser.set_defaults(func=lambda args: show_dataset_details(args.csv, args.name))
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        args.func(args)
    except Exception as e:
        logger.error(f"Command failed: {str(e)}")

if __name__ == "__main__":
    main()
