"""
Metadata comparison module for data validation.
Compares schema, data types, null counts, and basic statistics.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, isnull, isnan, min as spark_min, max as spark_max, 
                                mean, stddev, sum as spark_sum, countDistinct
from typing import Dict, Any, List, Tuple
import logging

logger = logging.getLogger(__name__)

def compare_schemas(schema1: List[Dict], schema2: List[Dict]) -> Dict[str, Any]:
    """
    Compare schemas between two datasets.
    
    Args:
        schema1: Schema from first dataset
        schema2: Schema from second dataset
    
    Returns:
        Dict: Schema comparison results
    """
    schema1_dict = {field['name']: field for field in schema1}
    schema2_dict = {field['name']: field for field in schema2}
    
    # Find common columns
    common_columns = set(schema1_dict.keys()) & set(schema2_dict.keys())
    only_in_1 = set(schema1_dict.keys()) - set(schema2_dict.keys())
    only_in_2 = set(schema2_dict.keys()) - set(schema1_dict.keys())
    
    # Compare data types for common columns
    type_differences = []
    for col_name in common_columns:
        type1 = schema1_dict[col_name]['type']
        type2 = schema2_dict[col_name]['type']
        if type1 != type2:
            type_differences.append({
                'column': col_name,
                'type1': type1,
                'type2': type2
            })
    
    return {
        'common_columns': list(common_columns),
        'only_in_dataset1': list(only_in_1),
        'only_in_dataset2': list(only_in_2),
        'type_differences': type_differences,
        'schema_match': len(type_differences) == 0 and len(only_in_1) == 0 and len(only_in_2) == 0
    }

def compare_null_counts(null_counts1: Dict[str, int], null_counts2: Dict[str, int], 
                       row_count1: int, row_count2: int) -> Dict[str, Any]:
    """
    Compare null counts between datasets.
    
    Args:
        null_counts1: Null counts from first dataset
        null_counts2: Null counts from second dataset
        row_count1: Row count of first dataset
        row_count2: Row count of second dataset
    
    Returns:
        Dict: Null count comparison results
    """
    common_columns = set(null_counts1.keys()) & set(null_counts2.keys())
    
    null_differences = []
    for col_name in common_columns:
        null1 = null_counts1[col_name]
        null2 = null_counts2[col_name]
        null_pct1 = (null1 / row_count1) * 100 if row_count1 > 0 else 0
        null_pct2 = (null2 / row_count2) * 100 if row_count2 > 0 else 0
        
        if null1 != null2:
            null_differences.append({
                'column': col_name,
                'null_count1': null1,
                'null_count2': null2,
                'null_pct1': round(null_pct1, 2),
                'null_pct2': round(null_pct2, 2),
                'difference': null2 - null1
            })
    
    return {
        'null_differences': null_differences,
        'null_counts_match': len(null_differences) == 0
    }

def get_column_statistics(df: DataFrame, column_name: str) -> Dict[str, Any]:
    """
    Get statistical information for a specific column.
    
    Args:
        df: Input DataFrame
        column_name: Name of the column
    
    Returns:
        Dict: Column statistics
    """
    try:
        col_data = df.select(col(column_name))
        
        # Basic counts
        total_count = col_data.count()
        null_count = col_data.filter(isnull(col(column_name))).count()
        distinct_count = col_data.select(countDistinct(col(column_name))).collect()[0][0]
        
        stats = {
            'total_count': total_count,
            'null_count': null_count,
            'distinct_count': distinct_count,
            'null_percentage': round((null_count / total_count) * 100, 2) if total_count > 0 else 0
        }
        
        # Try to get numeric statistics
        try:
            numeric_stats = col_data.select(
                spark_min(col(column_name)).alias('min'),
                spark_max(col(column_name)).alias('max'),
                mean(col(column_name)).alias('mean'),
                stddev(col(column_name)).alias('stddev')
            ).collect()[0]
            
            stats.update({
                'min': numeric_stats['min'],
                'max': numeric_stats['max'],
                'mean': round(numeric_stats['mean'], 2) if numeric_stats['mean'] is not None else None,
                'stddev': round(numeric_stats['stddev'], 2) if numeric_stats['stddev'] is not None else None
            })
        except:
            # Column is not numeric
            stats.update({
                'min': None,
                'max': None,
                'mean': None,
                'stddev': None
            })
        
        return stats
        
    except Exception as e:
        logger.error(f"Error getting statistics for column {column_name}: {str(e)}")
        return {
            'total_count': 0,
            'null_count': 0,
            'distinct_count': 0,
            'null_percentage': 0,
            'min': None,
            'max': None,
            'mean': None,
            'stddev': None
        }

def compare_column_statistics(stats1: Dict[str, Any], stats2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare statistics between two columns.
    
    Args:
        stats1: Statistics from first dataset
        stats2: Statistics from second dataset
    
    Returns:
        Dict: Column statistics comparison
    """
    differences = []
    
    # Compare basic counts
    if stats1['total_count'] != stats2['total_count']:
        differences.append({
            'metric': 'total_count',
            'value1': stats1['total_count'],
            'value2': stats2['total_count'],
            'difference': stats2['total_count'] - stats1['total_count']
        })
    
    if stats1['null_count'] != stats2['null_count']:
        differences.append({
            'metric': 'null_count',
            'value1': stats1['null_count'],
            'value2': stats2['null_count'],
            'difference': stats2['null_count'] - stats1['null_count']
        })
    
    if stats1['distinct_count'] != stats2['distinct_count']:
        differences.append({
            'metric': 'distinct_count',
            'value1': stats1['distinct_count'],
            'value2': stats2['distinct_count'],
            'difference': stats2['distinct_count'] - stats1['distinct_count']
        })
    
    # Compare numeric statistics if available
    for metric in ['min', 'max', 'mean', 'stddev']:
        if stats1[metric] is not None and stats2[metric] is not None:
            if abs(stats1[metric] - stats2[metric]) > 0.01:  # Small tolerance for floating point
                differences.append({
                    'metric': metric,
                    'value1': stats1[metric],
                    'value2': stats2[metric],
                    'difference': stats2[metric] - stats1[metric]
                })
    
    return {
        'differences': differences,
        'statistics_match': len(differences) == 0
    }

def compare_metadata(metadata1: Dict[str, Any], metadata2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Comprehensive metadata comparison between two datasets.
    
    Args:
        metadata1: Metadata from first dataset
        metadata2: Metadata from second dataset
    
    Returns:
        Dict: Complete metadata comparison results
    """
    logger.info("Starting metadata comparison")
    
    # Compare schemas
    schema_comparison = compare_schemas(metadata1['schema'], metadata2['schema'])
    
    # Compare null counts
    null_comparison = compare_null_counts(
        metadata1['null_counts'], 
        metadata2['null_counts'],
        metadata1['row_count'],
        metadata2['row_count']
    )
    
    # Compare row counts
    row_count_difference = metadata2['row_count'] - metadata1['row_count']
    row_count_match = row_count_difference == 0
    
    # Compare column counts
    column_count_difference = metadata2['column_count'] - metadata1['column_count']
    column_count_match = column_count_difference == 0
    
    # Overall metadata match
    overall_match = (schema_comparison['schema_match'] and 
                    null_comparison['null_counts_match'] and 
                    row_count_match and 
                    column_count_match)
    
    comparison_result = {
        'overall_match': overall_match,
        'row_count_comparison': {
            'count1': metadata1['row_count'],
            'count2': metadata2['row_count'],
            'difference': row_count_difference,
            'match': row_count_match
        },
        'column_count_comparison': {
            'count1': metadata1['column_count'],
            'count2': metadata2['column_count'],
            'difference': column_count_difference,
            'match': column_count_match
        },
        'schema_comparison': schema_comparison,
        'null_count_comparison': null_comparison,
        'summary': {
            'total_columns_compared': len(schema_comparison['common_columns']),
            'columns_only_in_dataset1': len(schema_comparison['only_in_dataset1']),
            'columns_only_in_dataset2': len(schema_comparison['only_in_dataset2']),
            'type_differences': len(schema_comparison['type_differences']),
            'null_count_differences': len(null_comparison['null_differences'])
        }
    }
    
    logger.info(f"Metadata comparison completed. Overall match: {overall_match}")
    return comparison_result

def get_detailed_column_comparison(df1: DataFrame, df2: DataFrame, 
                                 common_columns: List[str]) -> Dict[str, Any]:
    """
    Get detailed comparison for each common column.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        common_columns: List of common column names
    
    Returns:
        Dict: Detailed column comparisons
    """
    logger.info(f"Starting detailed comparison for {len(common_columns)} columns")
    
    column_comparisons = {}
    
    for col_name in common_columns:
        try:
            logger.info(f"Comparing column: {col_name}")
            
            # Get statistics for both datasets
            stats1 = get_column_statistics(df1, col_name)
            stats2 = get_column_statistics(df2, col_name)
            
            # Compare statistics
            comparison = compare_column_statistics(stats1, stats2)
            
            column_comparisons[col_name] = {
                'dataset1_stats': stats1,
                'dataset2_stats': stats2,
                'comparison': comparison
            }
            
        except Exception as e:
            logger.error(f"Error comparing column {col_name}: {str(e)}")
            column_comparisons[col_name] = {
                'error': str(e),
                'dataset1_stats': None,
                'dataset2_stats': None,
                'comparison': None
            }
    
    return column_comparisons
