"""
Full comparison engine with chunking and parallelism support.
Handles large-scale data comparison efficiently.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, isnull, isnan, lit, concat_ws, hash, count, 
                                collect_list, struct, row_number, monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, StructType, StructField
from typing import Dict, Any, List, Tuple, Optional
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime

logger = logging.getLogger(__name__)

def compare_data_chunks(chunk1: DataFrame, chunk2: DataFrame, 
                       comparison_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare two data chunks and return detailed results.
    
    Args:
        chunk1: First data chunk
        chunk2: Second data chunk
        comparison_config: Configuration for comparison
    
    Returns:
        Dict: Chunk comparison results
    """
    try:
        chunk_id = comparison_config.get('chunk_id', 0)
        logger.info(f"Comparing chunk {chunk_id}")
        
        # Get common columns (excluding system columns)
        common_columns = [c for c in chunk1.columns if c in chunk2.columns 
                         and c not in ['__row_id', '__fingerprint', '__chunk_id']]
        
        if not common_columns:
            return {
                'chunk_id': chunk_id,
                'error': 'No common columns found',
                'matches': 0,
                'differences': 0,
                'total_rows': 0
            }
        
        # Create comparison keys for each row
        chunk1_with_key = chunk1.withColumn(
            "__comparison_key", 
            concat_ws("|", *[col(c).cast(StringType()) for c in common_columns])
        )
        chunk2_with_key = chunk2.withColumn(
            "__comparison_key", 
            concat_ws("|", *[col(c).cast(StringType()) for c in common_columns])
        )
        
        # Find exact matches
        chunk1_keys = chunk1_with_key.select("__comparison_key", "__row_id").alias("c1")
        chunk2_keys = chunk2_with_key.select("__comparison_key", "__row_id").alias("c2")
        
        matches = chunk1_keys.join(chunk2_keys, "__comparison_key", "inner")
        match_count = matches.count()
        
        # Find rows only in chunk1
        only_in_1 = chunk1_keys.join(chunk2_keys, "__comparison_key", "left_anti")
        only_in_1_count = only_in_1.count()
        
        # Find rows only in chunk2
        only_in_2 = chunk2_keys.join(chunk1_keys, "__comparison_key", "left_anti")
        only_in_2_count = only_in_2.count()
        
        # Calculate total rows
        total_rows = chunk1.count() + chunk2.count()
        
        result = {
            'chunk_id': chunk_id,
            'matches': match_count,
            'only_in_dataset1': only_in_1_count,
            'only_in_dataset2': only_in_2_count,
            'total_rows': total_rows,
            'common_columns': common_columns,
            'match_percentage': (match_count / max(chunk1.count(), chunk2.count())) * 100 if max(chunk1.count(), chunk2.count()) > 0 else 0,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Chunk {chunk_id} comparison completed: {match_count} matches")
        return result
        
    except Exception as e:
        logger.error(f"Error comparing chunk {chunk_id}: {str(e)}")
        return {
            'chunk_id': chunk_id,
            'error': str(e),
            'matches': 0,
            'differences': 0,
            'total_rows': 0
        }

def parallel_chunk_comparison(chunks1: List[DataFrame], chunks2: List[DataFrame], 
                            comparison_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Compare chunks in parallel for better performance.
    
    Args:
        chunks1: List of chunks from first dataset
        chunks2: List of chunks from second dataset
        comparison_config: Configuration for comparison
    
    Returns:
        List[Dict]: Results from all chunk comparisons
    """
    max_parallelism = comparison_config.get('max_parallelism', 4)
    results = []
    
    logger.info(f"Starting parallel comparison of {len(chunks1)} chunks with {max_parallelism} workers")
    
    with ThreadPoolExecutor(max_workers=max_parallelism) as executor:
        # Submit all comparison tasks
        future_to_chunk = {}
        
        for i, (chunk1, chunk2) in enumerate(zip(chunks1, chunks2)):
            config = comparison_config.copy()
            config['chunk_id'] = i
            future = executor.submit(compare_data_chunks, chunk1, chunk2, config)
            future_to_chunk[future] = i
        
        # Collect results as they complete
        for future in as_completed(future_to_chunk):
            try:
                result = future.result()
                results.append(result)
                logger.info(f"Completed chunk {result['chunk_id']}")
            except Exception as e:
                chunk_id = future_to_chunk[future]
                logger.error(f"Error in chunk {chunk_id}: {str(e)}")
                results.append({
                    'chunk_id': chunk_id,
                    'error': str(e),
                    'matches': 0,
                    'differences': 0,
                    'total_rows': 0
                })
    
    # Sort results by chunk_id
    results.sort(key=lambda x: x['chunk_id'])
    
    logger.info(f"Parallel comparison completed. Processed {len(results)} chunks")
    return results

def aggregate_chunk_results(chunk_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Aggregate results from all chunk comparisons.
    
    Args:
        chunk_results: List of chunk comparison results
    
    Returns:
        Dict: Aggregated comparison results
    """
    logger.info("Aggregating chunk results")
    
    total_matches = 0
    total_only_in_1 = 0
    total_only_in_2 = 0
    total_rows = 0
    successful_chunks = 0
    failed_chunks = 0
    errors = []
    
    for result in chunk_results:
        if 'error' in result:
            failed_chunks += 1
            errors.append(f"Chunk {result['chunk_id']}: {result['error']}")
        else:
            successful_chunks += 1
            total_matches += result.get('matches', 0)
            total_only_in_1 += result.get('only_in_dataset1', 0)
            total_only_in_2 += result.get('only_in_dataset2', 0)
            total_rows += result.get('total_rows', 0)
    
    # Calculate overall statistics
    total_differences = total_only_in_1 + total_only_in_2
    match_percentage = (total_matches / max(total_rows - total_differences, 1)) * 100 if total_rows > 0 else 0
    
    aggregated_result = {
        'total_matches': total_matches,
        'total_only_in_dataset1': total_only_in_1,
        'total_only_in_dataset2': total_only_in_2,
        'total_differences': total_differences,
        'total_rows_processed': total_rows,
        'match_percentage': round(match_percentage, 2),
        'successful_chunks': successful_chunks,
        'failed_chunks': failed_chunks,
        'total_chunks': len(chunk_results),
        'errors': errors,
        'datasets_match': total_differences == 0 and failed_chunks == 0,
        'timestamp': datetime.now().isoformat()
    }
    
    logger.info(f"Aggregation completed: {total_matches} matches, {total_differences} differences")
    return aggregated_result

def full_data_comparison(df1: DataFrame, df2: DataFrame, 
                        comparison_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Perform full data comparison with chunking and parallelism.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        comparison_config: Configuration for comparison
    
    Returns:
        Dict: Complete comparison results
    """
    logger.info("Starting full data comparison")
    start_time = time.time()
    
    try:
        # Get chunk size and parallelism settings
        chunk_size = comparison_config.get('chunk_size', 1000000)
        max_parallelism = comparison_config.get('max_parallelism', 4)
        
        # Chunk the datasets
        logger.info(f"Chunking datasets with chunk size: {chunk_size}")
        chunks1 = chunk_dataframe(df1, chunk_size)
        chunks2 = chunk_dataframe(df2, chunk_size)
        
        logger.info(f"Created {len(chunks1)} chunks for dataset1 and {len(chunks2)} chunks for dataset2")
        
        # Ensure both datasets have the same number of chunks
        min_chunks = min(len(chunks1), len(chunks2))
        chunks1 = chunks1[:min_chunks]
        chunks2 = chunks2[:min_chunks]
        
        # Perform parallel chunk comparison
        chunk_results = parallel_chunk_comparison(chunks1, chunks2, comparison_config)
        
        # Aggregate results
        aggregated_results = aggregate_chunk_results(chunk_results)
        
        # Add timing information
        end_time = time.time()
        processing_time = end_time - start_time
        
        aggregated_results.update({
            'processing_time_seconds': round(processing_time, 2),
            'chunks_processed': len(chunk_results),
            'chunk_size': chunk_size,
            'max_parallelism': max_parallelism
        })
        
        logger.info(f"Full data comparison completed in {processing_time:.2f} seconds")
        return aggregated_results
        
    except Exception as e:
        logger.error(f"Error in full data comparison: {str(e)}")
        raise

def chunk_dataframe(df: DataFrame, chunk_size: int) -> List[DataFrame]:
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

def compare_specific_columns(df1: DataFrame, df2: DataFrame, 
                           columns: List[str], 
                           comparison_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare specific columns between datasets.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        columns: List of columns to compare
        comparison_config: Configuration for comparison
    
    Returns:
        Dict: Column-specific comparison results
    """
    logger.info(f"Comparing specific columns: {columns}")
    
    try:
        # Select only the specified columns
        df1_selected = df1.select(*columns, "__row_id")
        df2_selected = df2.select(*columns, "__row_id")
        
        # Perform full comparison on selected columns
        result = full_data_comparison(df1_selected, df2_selected, comparison_config)
        
        # Add column information
        result['columns_compared'] = columns
        result['comparison_type'] = 'column_specific'
        
        logger.info(f"Column-specific comparison completed for {len(columns)} columns")
        return result
        
    except Exception as e:
        logger.error(f"Error in column-specific comparison: {str(e)}")
        raise

def find_detailed_differences(df1: DataFrame, df2: DataFrame, 
                            sample_size: int = 1000) -> Dict[str, Any]:
    """
    Find detailed differences between datasets (sampled for performance).
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        sample_size: Sample size for detailed analysis
    
    Returns:
        Dict: Detailed difference analysis
    """
    logger.info(f"Finding detailed differences with sample size: {sample_size}")
    
    try:
        # Sample both datasets
        sample1 = df1.sample(withReplacement=False, fraction=sample_size/df1.count(), seed=42)
        sample2 = df2.sample(withReplacement=False, fraction=sample_size/df2.count(), seed=42)
        
        # Get common columns
        common_columns = [c for c in sample1.columns if c in sample2.columns 
                         and c not in ['__row_id', '__fingerprint']]
        
        detailed_differences = []
        
        for col_name in common_columns:
            try:
                # Get unique values in each sample
                unique1 = sample1.select(col_name).distinct().collect()
                unique2 = sample2.select(col_name).distinct().collect()
                
                values1 = set(row[col_name] for row in unique1)
                values2 = set(row[col_name] for row in unique2)
                
                only_in_1 = values1 - values2
                only_in_2 = values2 - values1
                common_values = values1 & values2
                
                if only_in_1 or only_in_2:
                    detailed_differences.append({
                        'column': col_name,
                        'only_in_dataset1': list(only_in_1)[:10],  # Limit to first 10
                        'only_in_dataset2': list(only_in_2)[:10],  # Limit to first 10
                        'common_values_count': len(common_values),
                        'unique_values_dataset1': len(values1),
                        'unique_values_dataset2': len(values2)
                    })
                    
            except Exception as e:
                logger.error(f"Error analyzing column {col_name}: {str(e)}")
                detailed_differences.append({
                    'column': col_name,
                    'error': str(e)
                })
        
        result = {
            'detailed_differences': detailed_differences,
            'columns_with_differences': len(detailed_differences),
            'sample_size': sample_size,
            'total_columns_analyzed': len(common_columns)
        }
        
        logger.info(f"Detailed difference analysis completed for {len(common_columns)} columns")
        return result
        
    except Exception as e:
        logger.error(f"Error in detailed difference analysis: {str(e)}")
        raise
