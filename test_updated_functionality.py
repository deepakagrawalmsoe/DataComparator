"""
Test script for the updated functionality with separate datasets configuration.
"""

import logging
import os
import yaml
from data_comparator import load_datasets_config, create_consolidated_report, generate_consolidated_reports

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_datasets_config_loading():
    """Test loading datasets configuration."""
    logger.info("Testing datasets configuration loading...")
    
    try:
        # Test loading datasets config
        datasets_config = load_datasets_config("datasets.yaml")
        
        # Verify structure
        assert 'datasets' in datasets_config
        assert len(datasets_config['datasets']) > 0
        
        # Check first dataset
        first_dataset = datasets_config['datasets'][0]
        assert 'name' in first_dataset
        assert 'sql_server' in first_dataset
        assert 's3_parquet' in first_dataset
        
        logger.info(f"Successfully loaded {len(datasets_config['datasets'])} datasets")
        logger.info(f"First dataset: {first_dataset['name']}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error testing datasets config loading: {str(e)}")
        return False

def test_consolidated_report_creation():
    """Test consolidated report creation."""
    logger.info("Testing consolidated report creation...")
    
    try:
        # Create mock results
        mock_results = [
            {
                'dataset_name': 'test_dataset_1',
                'dataset_description': 'Test dataset 1',
                'metadata_comparison': {'overall_match': True},
                'fingerprint_comparison': {'fingerprints_match': True},
                'full_comparison': {'datasets_match': True},
                'performance_metrics': {'total_processing_time': 10.5}
            },
            {
                'dataset_name': 'test_dataset_2',
                'dataset_description': 'Test dataset 2',
                'metadata_comparison': {'overall_match': False},
                'fingerprint_comparison': {'fingerprints_match': False},
                'full_comparison': {'datasets_match': False},
                'performance_metrics': {'total_processing_time': 15.2}
            }
        ]
        
        mock_config = {'comparison_settings': {}}
        
        # Test consolidated report creation
        consolidated_report = create_consolidated_report(
            mock_results, mock_results, [], mock_config
        )
        
        # Verify structure
        assert 'consolidated_summary' in consolidated_report
        assert 'dataset_summaries' in consolidated_report
        assert 'detailed_results' in consolidated_report
        
        # Verify summary
        summary = consolidated_report['consolidated_summary']
        assert summary['total_datasets'] == 2
        assert summary['successful_comparisons'] == 2
        assert summary['failed_comparisons'] == 0
        
        logger.info("Consolidated report creation test passed")
        return True
        
    except Exception as e:
        logger.error(f"Error testing consolidated report creation: {str(e)}")
        return False

def test_consolidated_report_generation():
    """Test consolidated report generation."""
    logger.info("Testing consolidated report generation...")
    
    try:
        # Create mock consolidated results
        mock_consolidated_results = {
            'consolidated_summary': {
                'total_datasets': 2,
                'successful_comparisons': 2,
                'failed_comparisons': 0,
                'overall_match': False,
                'total_processing_time': 25.7,
                'total_rows_processed': 1000000,
                'success_rate': 100.0
            },
            'dataset_summaries': [
                {
                    'name': 'test_dataset_1',
                    'description': 'Test dataset 1',
                    'status': 'success',
                    'overall_match': True,
                    'metadata_match': True,
                    'fingerprint_match': True,
                    'full_match': True,
                    'row_count_1': 500000,
                    'row_count_2': 500000,
                    'processing_time': 10.5
                },
                {
                    'name': 'test_dataset_2',
                    'description': 'Test dataset 2',
                    'status': 'success',
                    'overall_match': False,
                    'metadata_match': False,
                    'fingerprint_match': False,
                    'full_match': False,
                    'row_count_1': 500000,
                    'row_count_2': 500000,
                    'processing_time': 15.2
                }
            ],
            'detailed_results': [],
            'configuration': {},
            'report_metadata': {
                'generated_at': '2024-01-01T00:00:00',
                'report_type': 'consolidated_summary',
                'version': '1.0'
            }
        }
        
        # Test report generation
        output_path = "./test_consolidated_output"
        reports = generate_consolidated_reports(mock_consolidated_results, output_path)
        
        # Verify reports were generated
        assert 'json' in reports
        assert 'csv' in reports
        assert 'html' in reports
        
        # Verify files exist
        for report_type, report_path in reports.items():
            assert os.path.exists(report_path)
            logger.info(f"{report_type} report generated: {report_path}")
        
        logger.info("Consolidated report generation test passed")
        return True
        
    except Exception as e:
        logger.error(f"Error testing consolidated report generation: {str(e)}")
        return False

def test_configuration_separation():
    """Test that configuration is properly separated."""
    logger.info("Testing configuration separation...")
    
    try:
        # Load main config
        with open("config.yaml", 'r') as f:
            main_config = yaml.safe_load(f)
        
        # Verify datasets section is removed
        assert 'datasets' not in main_config
        
        # Load datasets config
        datasets_config = load_datasets_config("datasets.yaml")
        
        # Verify datasets are in separate file
        assert 'datasets' in datasets_config
        assert len(datasets_config['datasets']) > 0
        
        logger.info("Configuration separation test passed")
        return True
        
    except Exception as e:
        logger.error(f"Error testing configuration separation: {str(e)}")
        return False

def run_all_tests():
    """Run all tests."""
    logger.info("Starting updated functionality tests")
    
    tests = [
        test_datasets_config_loading,
        test_consolidated_report_creation,
        test_consolidated_report_generation,
        test_configuration_separation
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
                logger.info(f"✓ {test.__name__} PASSED")
            else:
                failed += 1
                logger.error(f"✗ {test.__name__} FAILED")
        except Exception as e:
            failed += 1
            logger.error(f"✗ {test.__name__} FAILED with exception: {str(e)}")
    
    logger.info(f"\nTest Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        logger.info("All tests passed! Updated functionality is working correctly.")
    else:
        logger.error(f"{failed} tests failed. Please check the implementation.")
    
    return failed == 0

if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
