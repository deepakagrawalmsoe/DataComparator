"""
Main orchestrator script for data comparison.
Coordinates all components for efficient large-scale data comparison.
"""

import logging
import time
import json
import csv
from datetime import datetime
from typing import Dict, Any, List, Optional
import argparse
import sys
import os

# Import all modules
from data_connectors import (
    load_config, create_spark_session, get_sql_server_data, 
    get_s3_parquet_data, get_data_metadata, get_data_sample
)
from csv_config_reader import load_datasets_from_csv, validate_csv_structure
from metadata_comparator import compare_metadata, get_detailed_column_comparison
from fingerprinting_sampler import (
    create_data_fingerprint, compare_fingerprints, 
    create_sample_comparison, detect_data_drift
)
from comparison_engine import (
    full_data_comparison, compare_specific_columns, 
    find_detailed_differences
)
from report_generator import generate_all_reports

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_comparator.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_metadata_comparison(df1, df2, config):
    """Run metadata comparison phase."""
    logger.info("=== PHASE 1: METADATA COMPARISON ===")
    start_time = time.time()
    
    try:
        # Extract metadata from both datasets
        metadata1 = get_data_metadata(df1)
        metadata2 = get_data_metadata(df2)
        
        # Compare metadata
        metadata_comparison = compare_metadata(metadata1, metadata2)
        
        # Get detailed column comparison
        common_columns = metadata_comparison['schema_comparison']['common_columns']
        detailed_column_comparison = get_detailed_column_comparison(df1, df2, common_columns)
        
        processing_time = time.time() - start_time
        
        result = {
            'metadata_comparison': metadata_comparison,
            'detailed_column_comparison': detailed_column_comparison,
            'processing_time': processing_time
        }
        
        logger.info(f"Metadata comparison completed in {processing_time:.2f} seconds")
        return result
        
    except Exception as e:
        logger.error(f"Error in metadata comparison: {str(e)}")
        raise

def run_fingerprinting_comparison(df1, df2, config):
    """Run fingerprinting comparison phase."""
    logger.info("=== PHASE 2: FINGERPRINTING COMPARISON ===")
    start_time = time.time()
    
    try:
        comparison_settings = config['comparison_settings']
        
        if not comparison_settings.get('enable_fingerprinting', True):
            logger.info("Fingerprinting disabled in configuration")
            return {'fingerprint_comparison': None, 'processing_time': 0}
        
        # Create fingerprints
        fingerprint_columns = comparison_settings.get('fingerprint_columns', [])
        algorithm = comparison_settings.get('fingerprint_algorithm', 'md5')
        
        df1_fp = create_data_fingerprint(df1, fingerprint_columns, algorithm)
        df2_fp = create_data_fingerprint(df2, fingerprint_columns, algorithm)
        
        # Compare fingerprints
        fingerprint_comparison = compare_fingerprints(df1_fp, df2_fp)
        
        processing_time = time.time() - start_time
        
        result = {
            'fingerprint_comparison': fingerprint_comparison,
            'processing_time': processing_time
        }
        
        logger.info(f"Fingerprinting comparison completed in {processing_time:.2f} seconds")
        return result
        
    except Exception as e:
        logger.error(f"Error in fingerprinting comparison: {str(e)}")
        raise

def run_sampling_comparison(df1, df2, config):
    """Run sampling comparison phase."""
    logger.info("=== PHASE 3: SAMPLING COMPARISON ===")
    start_time = time.time()
    
    try:
        comparison_settings = config['comparison_settings']
        
        if not comparison_settings.get('enable_sampling', True):
            logger.info("Sampling disabled in configuration")
            return {'sample_comparison': None, 'processing_time': 0}
        
        # Get sampling parameters
        sample_size = comparison_settings.get('sample_size', 100000)
        sampling_strategy = comparison_settings.get('sampling_strategy', 'random')
        
        # Create sample comparison
        sample_comparison = create_sample_comparison(
            df1, df2, sample_size, sampling_strategy
        )
        
        # Detect data drift
        drift_detection = detect_data_drift(df1, df2, sample_size)
        
        processing_time = time.time() - start_time
        
        result = {
            'sample_comparison': sample_comparison,
            'data_drift': drift_detection,
            'processing_time': processing_time
        }
        
        logger.info(f"Sampling comparison completed in {processing_time:.2f} seconds")
        return result
        
    except Exception as e:
        logger.error(f"Error in sampling comparison: {str(e)}")
        raise

def run_full_comparison(df1, df2, config):
    """Run full data comparison phase."""
    logger.info("=== PHASE 4: FULL DATA COMPARISON ===")
    start_time = time.time()
    
    try:
        comparison_settings = config['comparison_settings']
        
        if not comparison_settings.get('enable_full_comparison', True):
            logger.info("Full comparison disabled in configuration")
            return {'full_comparison': None, 'processing_time': 0}
        
        # Perform full comparison
        full_comparison = full_data_comparison(df1, df2, comparison_settings)
        
        # Find detailed differences
        detailed_differences = find_detailed_differences(df1, df2)
        
        processing_time = time.time() - start_time
        
        result = {
            'full_comparison': full_comparison,
            'detailed_differences': detailed_differences,
            'processing_time': processing_time
        }
        
        logger.info(f"Full comparison completed in {processing_time:.2f} seconds")
        return result
        
    except Exception as e:
        logger.error(f"Error in full comparison: {str(e)}")
        raise

def compare_datasets(dataset_config: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare two datasets based on configuration.
    
    Args:
        dataset_config: Dataset-specific configuration
        config: Main configuration
    
    Returns:
        Dict: Complete comparison results
    """
    logger.info(f"Starting comparison for dataset: {dataset_config.get('name', 'unknown')}")
    
    # Create Spark session
    spark = create_spark_session(config)
    
    try:
        # Load data from both sources
        logger.info("Loading data from SQL Server...")
        df1 = get_sql_server_data(spark, config, dataset_config)
        
        logger.info("Loading data from S3...")
        df2 = get_s3_parquet_data(spark, config, dataset_config)
        
        # Initialize results
        comparison_results = {
            'dataset_name': dataset_config.get('name', 'unknown'),
            'comparison_start_time': datetime.now().isoformat(),
            'configuration': config
        }
        
        # Phase 1: Metadata Comparison
        if config['comparison_settings'].get('enable_metadata_comparison', True):
            metadata_result = run_metadata_comparison(df1, df2, config)
            comparison_results.update(metadata_result)
        
        # Phase 2: Fingerprinting Comparison
        fingerprint_result = run_fingerprinting_comparison(df1, df2, config)
        comparison_results.update(fingerprint_result)
        
        # Phase 3: Sampling Comparison
        sampling_result = run_sampling_comparison(df1, df2, config)
        comparison_results.update(sampling_result)
        
        # Phase 4: Full Comparison
        full_result = run_full_comparison(df1, df2, config)
        comparison_results.update(full_result)
        
        # Calculate total processing time
        total_processing_time = (
            comparison_results.get('processing_time', 0) +
            fingerprint_result.get('processing_time', 0) +
            sampling_result.get('processing_time', 0) +
            full_result.get('processing_time', 0)
        )
        
        comparison_results.update({
            'comparison_end_time': datetime.now().isoformat(),
            'performance_metrics': {
                'total_processing_time': total_processing_time,
                'phases_completed': 4
            }
        })
        
        logger.info(f"Dataset comparison completed in {total_processing_time:.2f} seconds")
        return comparison_results
        
    except Exception as e:
        logger.error(f"Error comparing datasets: {str(e)}")
        raise
    finally:
        # Clean up Spark session
        spark.stop()

def load_datasets_config(datasets_path: str = "datasets.yaml") -> Dict[str, Any]:
    """Load datasets configuration from separate file (YAML or CSV)."""
    try:
        # Check file extension to determine format
        if datasets_path.lower().endswith('.csv'):
            logger.info(f"Loading datasets from CSV: {datasets_path}")
            # Validate CSV structure first
            if not validate_csv_structure(datasets_path):
                raise ValueError(f"Invalid CSV structure in {datasets_path}")
            return load_datasets_from_csv(datasets_path)
        else:
            logger.info(f"Loading datasets from YAML: {datasets_path}")
            with open(datasets_path, 'r') as file:
                return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Error loading datasets configuration: {str(e)}")
        raise

def run_comparison(config_path: str = "config.yaml", datasets_path: str = "datasets.csv", 
                  dataset_name: Optional[str] = None):
    """
    Run data comparison based on configuration.
    
    Args:
        config_path: Path to configuration file
        datasets_path: Path to datasets configuration file
        dataset_name: Specific dataset to compare (None for all)
    """
    logger.info("Starting Data Comparator")
    
    try:
        # Load main configuration
        config = load_config(config_path)
        
        # Load datasets configuration
        datasets_config = load_datasets_config(datasets_path)
        datasets = datasets_config.get('datasets', [])
        
        if dataset_name:
            datasets = [d for d in datasets if d.get('name') == dataset_name]
        
        if not datasets:
            logger.error("No datasets found in datasets configuration")
            return
        
        # Process each dataset
        all_results = []
        successful_results = []
        failed_results = []
        
        for dataset_config in datasets:
            try:
                logger.info(f"Processing dataset: {dataset_config.get('name', 'unknown')}")
                
                # Apply dataset-specific overrides if any
                dataset_overrides = datasets_config.get('global_settings', {}).get('overrides', {}).get(dataset_config.get('name'), {})
                if dataset_overrides:
                    logger.info(f"Applying overrides for {dataset_config.get('name')}: {dataset_overrides}")
                    # Merge overrides into comparison settings
                    config['comparison_settings'].update(dataset_overrides)
                
                result = compare_datasets(dataset_config, config)
                result['dataset_name'] = dataset_config.get('name', 'unknown')
                result['dataset_description'] = dataset_config.get('description', '')
                all_results.append(result)
                successful_results.append(result)
                
                # Generate individual dataset reports
                output_path = config.get('comparison_settings', {}).get('output_path', './comparison_results')
                dataset_output_path = os.path.join(output_path, 'individual', dataset_config.get('name', 'unknown'))
                
                reports = generate_all_reports(result, dataset_output_path)
                logger.info(f"Individual reports generated for {dataset_config.get('name')}: {list(reports.keys())}")
                
            except Exception as e:
                logger.error(f"Error processing dataset {dataset_config.get('name')}: {str(e)}")
                failed_result = {
                    'dataset_name': dataset_config.get('name', 'unknown'),
                    'dataset_description': dataset_config.get('description', ''),
                    'error': str(e),
                    'status': 'failed'
                }
                all_results.append(failed_result)
                failed_results.append(failed_result)
                continue
        
        # Generate consolidated summary report
        consolidated_results = create_consolidated_report(all_results, successful_results, failed_results, config)
        
        # Generate consolidated reports
        output_path = config.get('comparison_settings', {}).get('output_path', './comparison_results')
        consolidated_output_path = os.path.join(output_path, 'consolidated')
        
        consolidated_reports = generate_consolidated_reports(consolidated_results, consolidated_output_path)
        logger.info(f"Consolidated reports generated: {list(consolidated_reports.keys())}")
        
        logger.info("Data comparison completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main comparison process: {str(e)}")
        raise

def create_consolidated_report(all_results: List[Dict[str, Any]], 
                             successful_results: List[Dict[str, Any]], 
                             failed_results: List[Dict[str, Any]], 
                             config: Dict[str, Any]) -> Dict[str, Any]:
    """Create consolidated report for all datasets."""
    logger.info("Creating consolidated report")
    
    # Calculate overall statistics
    total_datasets = len(all_results)
    successful_count = len(successful_results)
    failed_count = len(failed_results)
    
    # Calculate overall match status
    overall_match = all(
        result.get('metadata_comparison', {}).get('overall_match', False) and
        result.get('fingerprint_comparison', {}).get('fingerprints_match', False) and
        result.get('full_comparison', {}).get('datasets_match', False)
        for result in successful_results
    )
    
    # Calculate total processing time
    total_processing_time = sum(
        result.get('performance_metrics', {}).get('total_processing_time', 0)
        for result in successful_results
    )
    
    # Calculate total rows processed
    total_rows_processed = sum(
        result.get('metadata_comparison', {}).get('row_count_comparison', {}).get('count1', 0) +
        result.get('metadata_comparison', {}).get('row_count_comparison', {}).get('count2', 0)
        for result in successful_results
    )
    
    # Create dataset summary
    dataset_summaries = []
    for result in all_results:
        if 'error' in result:
            dataset_summaries.append({
                'name': result['dataset_name'],
                'description': result['dataset_description'],
                'status': 'failed',
                'error': result['error']
            })
        else:
            metadata = result.get('metadata_comparison', {})
            fingerprint = result.get('fingerprint_comparison', {})
            full_comp = result.get('full_comparison', {})
            
            dataset_summaries.append({
                'name': result['dataset_name'],
                'description': result['dataset_description'],
                'status': 'success',
                'metadata_match': metadata.get('overall_match', False),
                'fingerprint_match': fingerprint.get('fingerprints_match', False),
                'full_match': full_comp.get('datasets_match', False),
                'overall_match': (
                    metadata.get('overall_match', False) and
                    fingerprint.get('fingerprints_match', False) and
                    full_comp.get('datasets_match', False)
                ),
                'row_count_1': metadata.get('row_count_comparison', {}).get('count1', 0),
                'row_count_2': metadata.get('row_count_comparison', {}).get('count2', 0),
                'processing_time': result.get('performance_metrics', {}).get('total_processing_time', 0)
            })
    
    # Create consolidated report
    consolidated_report = {
        'consolidated_summary': {
            'total_datasets': total_datasets,
            'successful_comparisons': successful_count,
            'failed_comparisons': failed_count,
            'overall_match': overall_match,
            'total_processing_time': total_processing_time,
            'total_rows_processed': total_rows_processed,
            'average_processing_time': total_processing_time / successful_count if successful_count > 0 else 0,
            'success_rate': (successful_count / total_datasets) * 100 if total_datasets > 0 else 0
        },
        'dataset_summaries': dataset_summaries,
        'detailed_results': all_results,
        'configuration': config,
        'report_metadata': {
            'generated_at': datetime.now().isoformat(),
            'report_type': 'consolidated_summary',
            'version': '1.0'
        }
    }
    
    logger.info(f"Consolidated report created: {successful_count}/{total_datasets} datasets successful")
    return consolidated_report

def generate_consolidated_reports(consolidated_results: Dict[str, Any], 
                                output_path: str) -> Dict[str, str]:
    """Generate consolidated reports in multiple formats."""
    logger.info("Generating consolidated reports")
    
    try:
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        reports = {}
        
        # Generate JSON summary report
        json_file = os.path.join(output_path, f"consolidated_summary_{timestamp}.json")
        with open(json_file, 'w') as f:
            json.dump(consolidated_results, f, indent=2, default=str)
        reports['json'] = json_file
        
        # Generate CSV summary report
        csv_file = os.path.join(output_path, f"consolidated_summary_{timestamp}.csv")
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow([
                'Dataset', 'Description', 'Status', 'Overall_Match', 
                'Metadata_Match', 'Fingerprint_Match', 'Full_Match',
                'Row_Count_1', 'Row_Count_2', 'Processing_Time_Seconds', 'Error'
            ])
            
            # Write data
            for summary in consolidated_results['dataset_summaries']:
                writer.writerow([
                    summary.get('name', ''),
                    summary.get('description', ''),
                    summary.get('status', ''),
                    summary.get('overall_match', False),
                    summary.get('metadata_match', False),
                    summary.get('fingerprint_match', False),
                    summary.get('full_match', False),
                    summary.get('row_count_1', 0),
                    summary.get('row_count_2', 0),
                    summary.get('processing_time', 0),
                    summary.get('error', '')
                ])
        
        reports['csv'] = csv_file
        
        # Generate HTML summary report
        html_file = os.path.join(output_path, f"consolidated_summary_{timestamp}.html")
        generate_consolidated_html_report(consolidated_results, html_file)
        reports['html'] = html_file
        
        logger.info(f"Consolidated reports generated: {list(reports.keys())}")
        return reports
        
    except Exception as e:
        logger.error(f"Error generating consolidated reports: {str(e)}")
        raise

def generate_consolidated_html_report(consolidated_results: Dict[str, Any], html_file: str):
    """Generate HTML consolidated report."""
    summary = consolidated_results['consolidated_summary']
    dataset_summaries = consolidated_results['dataset_summaries']
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Consolidated Data Comparison Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
            .summary {{ background-color: #e8f5e8; padding: 15px; margin: 10px 0; border-radius: 5px; }}
            .dataset {{ background-color: #fff3cd; padding: 15px; margin: 10px 0; border-radius: 5px; }}
            .metric {{ display: inline-block; margin: 10px; padding: 10px; background-color: white; border-radius: 3px; }}
            .pass {{ color: green; font-weight: bold; }}
            .fail {{ color: red; font-weight: bold; }}
            .success {{ color: green; }}
            .error {{ color: red; }}
            table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Consolidated Data Comparison Report</h1>
            <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <div class="summary">
            <h2>Overall Summary</h2>
            <div class="metric">
                <strong>Total Datasets:</strong> {summary['total_datasets']}
            </div>
            <div class="metric">
                <strong>Successful:</strong> <span class="success">{summary['successful_comparisons']}</span>
            </div>
            <div class="metric">
                <strong>Failed:</strong> <span class="error">{summary['failed_comparisons']}</span>
            </div>
            <div class="metric">
                <strong>Success Rate:</strong> {summary['success_rate']:.1f}%
            </div>
            <div class="metric">
                <strong>Overall Match:</strong> 
                <span class="{'pass' if summary['overall_match'] else 'fail'}">
                    {'Yes' if summary['overall_match'] else 'No'}
                </span>
            </div>
            <div class="metric">
                <strong>Total Processing Time:</strong> {summary['total_processing_time']:.2f} seconds
            </div>
            <div class="metric">
                <strong>Total Rows Processed:</strong> {summary['total_rows_processed']:,}
            </div>
        </div>
        
        <div class="dataset">
            <h2>Dataset Summary</h2>
            <table>
                <tr>
                    <th>Dataset</th>
                    <th>Description</th>
                    <th>Status</th>
                    <th>Overall Match</th>
                    <th>Metadata Match</th>
                    <th>Fingerprint Match</th>
                    <th>Full Match</th>
                    <th>Rows (1)</th>
                    <th>Rows (2)</th>
                    <th>Processing Time</th>
                </tr>
                {''.join(f'''
                <tr>
                    <td>{ds['name']}</td>
                    <td>{ds['description']}</td>
                    <td class="{'success' if ds['status'] == 'success' else 'error'}">{ds['status']}</td>
                    <td class="{'pass' if ds.get('overall_match', False) else 'fail'}">{'Yes' if ds.get('overall_match', False) else 'No'}</td>
                    <td class="{'pass' if ds.get('metadata_match', False) else 'fail'}">{'Yes' if ds.get('metadata_match', False) else 'No'}</td>
                    <td class="{'pass' if ds.get('fingerprint_match', False) else 'fail'}">{'Yes' if ds.get('fingerprint_match', False) else 'No'}</td>
                    <td class="{'pass' if ds.get('full_match', False) else 'fail'}">{'Yes' if ds.get('full_match', False) else 'No'}</td>
                    <td>{ds.get('row_count_1', 0):,}</td>
                    <td>{ds.get('row_count_2', 0):,}</td>
                    <td>{ds.get('processing_time', 0):.2f}s</td>
                </tr>
                ''' for ds in dataset_summaries)}
            </table>
        </div>
    </body>
    </html>
    """
    
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(html_content)

def main():
    """Main entry point for the data comparator."""
    parser = argparse.ArgumentParser(description='Data Comparator for SQL Server and S3 Parquet')
    parser.add_argument('--config', '-c', default='config.yaml', 
                       help='Path to configuration file')
    parser.add_argument('--datasets', '-ds', default='datasets.csv',
                       help='Path to datasets configuration file (CSV or YAML)')
    parser.add_argument('--dataset', '-d', 
                       help='Specific dataset name to compare')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Run comparison
    try:
        run_comparison(args.config, args.datasets, args.dataset)
    except KeyboardInterrupt:
        logger.info("Comparison interrupted by user")
    except Exception as e:
        logger.error(f"Comparison failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
