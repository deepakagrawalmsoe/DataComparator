"""
Comprehensive reporting system for data comparison results.
Generates detailed reports in multiple formats.
"""

import json
import csv
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def generate_summary_report(comparison_results: Dict[str, Any], 
                          output_path: str = "./comparison_results") -> str:
    """
    Generate a comprehensive summary report.
    
    Args:
        comparison_results: Complete comparison results
        output_path: Output directory path
    
    Returns:
        str: Path to generated report file
    """
    logger.info("Generating summary report")
    
    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(output_path, f"comparison_summary_{timestamp}.json")
    
    try:
        # Create comprehensive summary
        summary = {
            'report_metadata': {
                'generated_at': datetime.now().isoformat(),
                'report_type': 'comprehensive_summary',
                'version': '1.0'
            },
            'executive_summary': create_executive_summary(comparison_results),
            'metadata_comparison': comparison_results.get('metadata_comparison', {}),
            'fingerprint_comparison': comparison_results.get('fingerprint_comparison', {}),
            'sample_comparison': comparison_results.get('sample_comparison', {}),
            'full_comparison': comparison_results.get('full_comparison', {}),
            'detailed_differences': comparison_results.get('detailed_differences', {}),
            'data_drift': comparison_results.get('data_drift', {}),
            'performance_metrics': comparison_results.get('performance_metrics', {}),
            'recommendations': generate_recommendations(comparison_results)
        }
        
        # Write to file
        with open(report_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        logger.info(f"Summary report generated: {report_file}")
        return report_file
        
    except Exception as e:
        logger.error(f"Error generating summary report: {str(e)}")
        raise

def create_executive_summary(comparison_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create executive summary of comparison results.
    
    Args:
        comparison_results: Complete comparison results
    
    Returns:
        Dict: Executive summary
    """
    # Extract key metrics
    metadata_match = comparison_results.get('metadata_comparison', {}).get('overall_match', False)
    fingerprint_match = comparison_results.get('fingerprint_comparison', {}).get('fingerprints_match', False)
    full_match = comparison_results.get('full_comparison', {}).get('datasets_match', False)
    
    # Calculate overall match status
    overall_match = metadata_match and fingerprint_match and full_match
    
    # Get row counts
    metadata = comparison_results.get('metadata_comparison', {})
    row_count1 = metadata.get('row_count_comparison', {}).get('count1', 0)
    row_count2 = metadata.get('row_count_comparison', {}).get('count2', 0)
    
    # Get performance metrics
    performance = comparison_results.get('performance_metrics', {})
    processing_time = performance.get('total_processing_time', 0)
    
    # Get drift information
    drift_info = comparison_results.get('data_drift', {})
    drift_detected = drift_info.get('drift_detected', False)
    columns_with_drift = drift_info.get('columns_with_drift', [])
    
    return {
        'overall_match': overall_match,
        'datasets_identical': overall_match,
        'row_count_dataset1': row_count1,
        'row_count_dataset2': row_count2,
        'row_count_difference': row_count2 - row_count1,
        'processing_time_seconds': processing_time,
        'data_drift_detected': drift_detected,
        'columns_with_drift': len(columns_with_drift),
        'comparison_status': 'PASS' if overall_match else 'FAIL',
        'key_findings': generate_key_findings(comparison_results)
    }

def generate_key_findings(comparison_results: Dict[str, Any]) -> List[str]:
    """
    Generate key findings from comparison results.
    
    Args:
        comparison_results: Complete comparison results
    
    Returns:
        List[str]: Key findings
    """
    findings = []
    
    # Metadata findings
    metadata = comparison_results.get('metadata_comparison', {})
    if not metadata.get('overall_match', True):
        findings.append("Metadata comparison failed - schemas or data types differ")
        
        schema_diff = metadata.get('schema_comparison', {})
        if schema_diff.get('type_differences'):
            findings.append(f"Found {len(schema_diff['type_differences'])} columns with type differences")
        
        if schema_diff.get('only_in_dataset1'):
            findings.append(f"Found {len(schema_diff['only_in_dataset1'])} columns only in dataset 1")
        
        if schema_diff.get('only_in_dataset2'):
            findings.append(f"Found {len(schema_diff['only_in_dataset2'])} columns only in dataset 2")
    
    # Fingerprint findings
    fingerprint = comparison_results.get('fingerprint_comparison', {})
    if not fingerprint.get('fingerprints_match', True):
        match_pct = fingerprint.get('match_percentage', 0)
        findings.append(f"Fingerprint comparison shows only {match_pct}% match")
    
    # Full comparison findings
    full_comp = comparison_results.get('full_comparison', {})
    if not full_comp.get('datasets_match', True):
        differences = full_comp.get('total_differences', 0)
        findings.append(f"Full comparison found {differences} row differences")
    
    # Drift findings
    drift = comparison_results.get('data_drift', {})
    if drift.get('drift_detected', False):
        drift_cols = drift.get('columns_with_drift', [])
        findings.append(f"Data drift detected in {len(drift_cols)} columns: {drift_cols}")
    
    # Performance findings
    performance = comparison_results.get('performance_metrics', {})
    if performance.get('total_processing_time', 0) > 300:  # 5 minutes
        findings.append("Processing time exceeded 5 minutes - consider optimizing")
    
    if not findings:
        findings.append("All comparisons passed - datasets appear to be identical")
    
    return findings

def generate_recommendations(comparison_results: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Generate recommendations based on comparison results.
    
    Args:
        comparison_results: Complete comparison results
    
    Returns:
        List[Dict]: Recommendations
    """
    recommendations = []
    
    # Metadata recommendations
    metadata = comparison_results.get('metadata_comparison', {})
    if not metadata.get('overall_match', True):
        recommendations.append({
            'category': 'Schema',
            'priority': 'High',
            'recommendation': 'Review and align schemas between datasets before comparison'
        })
    
    # Performance recommendations
    performance = comparison_results.get('performance_metrics', {})
    if performance.get('total_processing_time', 0) > 600:  # 10 minutes
        recommendations.append({
            'category': 'Performance',
            'priority': 'Medium',
            'recommendation': 'Consider increasing chunk size or parallelism for better performance'
        })
    
    # Drift recommendations
    drift = comparison_results.get('data_drift', {})
    if drift.get('drift_detected', False):
        recommendations.append({
            'category': 'Data Quality',
            'priority': 'High',
            'recommendation': 'Investigate data drift in identified columns - may indicate data pipeline issues'
        })
    
    # Full comparison recommendations
    full_comp = comparison_results.get('full_comparison', {})
    if not full_comp.get('datasets_match', True):
        recommendations.append({
            'category': 'Data Integrity',
            'priority': 'High',
            'recommendation': 'Review data differences and ensure data consistency'
        })
    
    return recommendations

def generate_csv_report(comparison_results: Dict[str, Any], 
                       output_path: str = "./comparison_results") -> str:
    """
    Generate CSV report for detailed analysis.
    
    Args:
        comparison_results: Complete comparison results
        output_path: Output directory path
    
    Returns:
        str: Path to generated CSV file
    """
    logger.info("Generating CSV report")
    
    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = os.path.join(output_path, f"comparison_details_{timestamp}.csv")
    
    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow([
                'Category', 'Metric', 'Dataset1_Value', 'Dataset2_Value', 
                'Difference', 'Match_Status', 'Notes'
            ])
            
            # Metadata comparison
            metadata = comparison_results.get('metadata_comparison', {})
            if metadata:
                writer.writerow(['Metadata', 'Row Count', 
                               metadata.get('row_count_comparison', {}).get('count1', ''),
                               metadata.get('row_count_comparison', {}).get('count2', ''),
                               metadata.get('row_count_comparison', {}).get('difference', ''),
                               'PASS' if metadata.get('row_count_comparison', {}).get('match', False) else 'FAIL',
                               ''])
                
                writer.writerow(['Metadata', 'Column Count',
                               metadata.get('column_count_comparison', {}).get('count1', ''),
                               metadata.get('column_count_comparison', {}).get('count2', ''),
                               metadata.get('column_count_comparison', {}).get('difference', ''),
                               'PASS' if metadata.get('column_count_comparison', {}).get('match', False) else 'FAIL',
                               ''])
            
            # Fingerprint comparison
            fingerprint = comparison_results.get('fingerprint_comparison', {})
            if fingerprint:
                writer.writerow(['Fingerprint', 'Match Percentage',
                               '', '', '',
                               fingerprint.get('match_percentage', 0),
                               'PASS' if fingerprint.get('fingerprints_match', False) else 'FAIL'])
            
            # Full comparison
            full_comp = comparison_results.get('full_comparison', {})
            if full_comp:
                writer.writerow(['Full Comparison', 'Total Matches',
                               '', '', '',
                               full_comp.get('total_matches', 0),
                               ''])
                
                writer.writerow(['Full Comparison', 'Total Differences',
                               '', '', '',
                               full_comp.get('total_differences', 0),
                               'PASS' if full_comp.get('datasets_match', False) else 'FAIL'])
        
        logger.info(f"CSV report generated: {csv_file}")
        return csv_file
        
    except Exception as e:
        logger.error(f"Error generating CSV report: {str(e)}")
        raise

def generate_html_report(comparison_results: Dict[str, Any], 
                        output_path: str = "./comparison_results") -> str:
    """
    Generate HTML report for web viewing.
    
    Args:
        comparison_results: Complete comparison results
        output_path: Output directory path
    
    Returns:
        str: Path to generated HTML file
    """
    logger.info("Generating HTML report")
    
    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_file = os.path.join(output_path, f"comparison_report_{timestamp}.html")
    
    try:
        # Extract key information
        executive_summary = create_executive_summary(comparison_results)
        key_findings = generate_key_findings(comparison_results)
        recommendations = generate_recommendations(comparison_results)
        
        # Generate HTML content
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Comparison Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
                .summary {{ background-color: #e8f5e8; padding: 15px; margin: 10px 0; border-radius: 5px; }}
                .findings {{ background-color: #fff3cd; padding: 15px; margin: 10px 0; border-radius: 5px; }}
                .recommendations {{ background-color: #d1ecf1; padding: 15px; margin: 10px 0; border-radius: 5px; }}
                .metric {{ display: inline-block; margin: 10px; padding: 10px; background-color: white; border-radius: 3px; }}
                .pass {{ color: green; font-weight: bold; }}
                .fail {{ color: red; font-weight: bold; }}
                table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Data Comparison Report</h1>
                <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            
            <div class="summary">
                <h2>Executive Summary</h2>
                <div class="metric">
                    <strong>Overall Status:</strong> 
                    <span class="{'pass' if executive_summary['overall_match'] else 'fail'}">
                        {executive_summary['comparison_status']}
                    </span>
                </div>
                <div class="metric">
                    <strong>Dataset 1 Rows:</strong> {executive_summary['row_count_dataset1']:,}
                </div>
                <div class="metric">
                    <strong>Dataset 2 Rows:</strong> {executive_summary['row_count_dataset2']:,}
                </div>
                <div class="metric">
                    <strong>Processing Time:</strong> {executive_summary['processing_time_seconds']:.2f} seconds
                </div>
                <div class="metric">
                    <strong>Data Drift Detected:</strong> 
                    <span class="{'fail' if executive_summary['data_drift_detected'] else 'pass'}">
                        {'Yes' if executive_summary['data_drift_detected'] else 'No'}
                    </span>
                </div>
            </div>
            
            <div class="findings">
                <h2>Key Findings</h2>
                <ul>
                    {''.join(f'<li>{finding}</li>' for finding in key_findings)}
                </ul>
            </div>
            
            <div class="recommendations">
                <h2>Recommendations</h2>
                <table>
                    <tr>
                        <th>Category</th>
                        <th>Priority</th>
                        <th>Recommendation</th>
                    </tr>
                    {''.join(f'<tr><td>{rec["category"]}</td><td>{rec["priority"]}</td><td>{rec["recommendation"]}</td></tr>' for rec in recommendations)}
                </table>
            </div>
        </body>
        </html>
        """
        
        # Write HTML file
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML report generated: {html_file}")
        return html_file
        
    except Exception as e:
        logger.error(f"Error generating HTML report: {str(e)}")
        raise

def generate_detailed_column_report(comparison_results: Dict[str, Any], 
                                  output_path: str = "./comparison_results") -> str:
    """
    Generate detailed column-by-column comparison report.
    
    Args:
        comparison_results: Complete comparison results
        output_path: Output directory path
    
    Returns:
        str: Path to generated detailed report file
    """
    logger.info("Generating detailed column report")
    
    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    detailed_file = os.path.join(output_path, f"column_details_{timestamp}.json")
    
    try:
        # Extract detailed column information
        metadata = comparison_results.get('metadata_comparison', {})
        detailed_comparison = comparison_results.get('detailed_column_comparison', {})
        
        detailed_report = {
            'report_metadata': {
                'generated_at': datetime.now().isoformat(),
                'report_type': 'detailed_column_analysis'
            },
            'schema_analysis': metadata.get('schema_comparison', {}),
            'null_count_analysis': metadata.get('null_count_comparison', {}),
            'column_statistics': detailed_comparison,
            'summary': {
                'total_columns_analyzed': len(detailed_comparison),
                'columns_with_differences': len([col for col, data in detailed_comparison.items() 
                                               if data.get('comparison', {}).get('statistics_match', True) == False]),
                'columns_with_errors': len([col for col, data in detailed_comparison.items() 
                                          if 'error' in data])
            }
        }
        
        # Write detailed report
        with open(detailed_file, 'w') as f:
            json.dump(detailed_report, f, indent=2, default=str)
        
        logger.info(f"Detailed column report generated: {detailed_file}")
        return detailed_file
        
    except Exception as e:
        logger.error(f"Error generating detailed column report: {str(e)}")
        raise

def generate_all_reports(comparison_results: Dict[str, Any], 
                        output_path: str = "./comparison_results") -> Dict[str, str]:
    """
    Generate all available reports.
    
    Args:
        comparison_results: Complete comparison results
        output_path: Output directory path
    
    Returns:
        Dict[str, str]: Paths to all generated report files
    """
    logger.info("Generating all reports")
    
    reports = {}
    
    try:
        # Check if this is a consolidated report
        if 'consolidated_summary' in comparison_results:
            # Generate consolidated reports
            reports = generate_consolidated_reports(comparison_results, output_path)
        else:
            # Generate individual dataset reports
            reports['summary'] = generate_summary_report(comparison_results, output_path)
            reports['csv'] = generate_csv_report(comparison_results, output_path)
            reports['html'] = generate_html_report(comparison_results, output_path)
            reports['detailed'] = generate_detailed_column_report(comparison_results, output_path)
        
        logger.info(f"All reports generated successfully in: {output_path}")
        return reports
        
    except Exception as e:
        logger.error(f"Error generating reports: {str(e)}")
        raise

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
