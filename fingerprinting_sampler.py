"""
Fingerprinting and sampling strategies for efficient data comparison.
Implements various algorithms for data fingerprinting and sampling.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, hash, md5, sha2, monotonically_increasing_id, 
                                row_number, when, isnull, isnan, lit, collect_list, struct
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from typing import Dict, Any, List, Optional
import logging
import xxhash
import hashlib

logger = logging.getLogger(__name__)

def create_data_fingerprint(df: DataFrame, columns: Optional[List[str]] = None, 
                          algorithm: str = "md5") -> DataFrame:
    """
    Create data fingerprints for efficient comparison.
    
    Args:
        df: Input DataFrame
        columns: Columns to include in fingerprint (None for all)
        algorithm: Hashing algorithm (md5, sha256, xxhash)
    
    Returns:
        DataFrame: Original data with fingerprint column
    """
    if columns is None:
        columns = [c for c in df.columns if c != "__row_id"]
    
    logger.info(f"Creating fingerprints for {len(columns)} columns using {algorithm}")
    
    try:
        # Handle null values by converting to string representation
        df_processed = df
        for col_name in columns:
            df_processed = df_processed.withColumn(
                col_name,
                when(isnull(col(col_name)), lit("__NULL__"))
                .when(isnan(col(col_name)), lit("__NAN__"))
                .otherwise(col(col_name).cast(StringType()))
            )
        
        # Create fingerprint based on algorithm
        if algorithm == "md5":
            # Use Spark's built-in MD5
            fingerprint_expr = md5(concat_ws("|", *[col(c) for c in columns]))
        elif algorithm == "sha256":
            # Use Spark's built-in SHA2
            fingerprint_expr = sha2(concat_ws("|", *[col(c) for c in columns]), 256)
        elif algorithm == "xxhash":
            # Use hash function (Spark doesn't have xxhash, using hash as approximation)
            fingerprint_expr = hash(concat_ws("|", *[col(c) for c in columns]))
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        
        # Add fingerprint column
        df_with_fingerprint = df_processed.withColumn("__fingerprint", fingerprint_expr)
        
        logger.info("Fingerprint creation completed")
        return df_with_fingerprint
        
    except Exception as e:
        logger.error(f"Error creating fingerprints: {str(e)}")
        raise

def compare_fingerprints(df1: DataFrame, df2: DataFrame) -> Dict[str, Any]:
    """
    Compare fingerprints between two datasets.
    
    Args:
        df1: First DataFrame with fingerprints
        df2: Second DataFrame with fingerprints
    
    Returns:
        Dict: Fingerprint comparison results
    """
    logger.info("Starting fingerprint comparison")
    
    try:
        # Get fingerprint counts
        fp1_counts = df1.groupBy("__fingerprint").count().alias("fp1")
        fp2_counts = df2.groupBy("__fingerprint").count().alias("fp2")
        
        # Find common fingerprints
        common_fps = fp1_counts.join(fp2_counts, "__fingerprint", "inner")
        
        # Find fingerprints only in dataset 1
        only_in_1 = fp1_counts.join(fp2_counts, "__fingerprint", "left_anti")
        
        # Find fingerprints only in dataset 2
        only_in_2 = fp2_counts.join(fp1_counts, "__fingerprint", "left_anti")
        
        # Count results
        common_count = common_fps.count()
        only_in_1_count = only_in_1.count()
        only_in_2_count = only_in_2.count()
        
        # Calculate match percentage
        total_fp1 = df1.count()
        total_fp2 = df2.count()
        match_percentage = (common_count / max(total_fp1, total_fp2)) * 100 if max(total_fp1, total_fp2) > 0 else 0
        
        result = {
            'common_fingerprints': common_count,
            'only_in_dataset1': only_in_1_count,
            'only_in_dataset2': only_in_2_count,
            'total_dataset1': total_fp1,
            'total_dataset2': total_fp2,
            'match_percentage': round(match_percentage, 2),
            'fingerprints_match': only_in_1_count == 0 and only_in_2_count == 0
        }
        
        logger.info(f"Fingerprint comparison completed. Match: {result['fingerprints_match']}")
        return result
        
    except Exception as e:
        logger.error(f"Error comparing fingerprints: {str(e)}")
        raise

def systematic_sampling(df: DataFrame, sample_size: int) -> DataFrame:
    """
    Perform systematic sampling on the dataset.
    
    Args:
        df: Input DataFrame
        sample_size: Desired sample size
    
    Returns:
        DataFrame: Sampled data
    """
    total_rows = df.count()
    
    if total_rows <= sample_size:
        logger.info("Dataset size is smaller than sample size, returning full dataset")
        return df
    
    # Calculate sampling interval
    interval = total_rows // sample_size
    
    logger.info(f"Systematic sampling: {sample_size} samples from {total_rows} rows (interval: {interval})")
    
    # Add row numbers and sample
    df_with_rownum = df.withColumn("__row_num", row_number().over(Window.partitionBy().orderBy("__row_id")))
    
    sampled_df = df_with_rownum.filter((col("__row_num") % interval) == 0).limit(sample_size)
    
    return sampled_df.drop("__row_num")

def stratified_sampling(df: DataFrame, sample_size: int, 
                       stratify_column: str) -> DataFrame:
    """
    Perform stratified sampling based on a specific column.
    
    Args:
        df: Input DataFrame
        sample_size: Desired sample size
        stratify_column: Column to stratify on
    
    Returns:
        DataFrame: Stratified sampled data
    """
    logger.info(f"Stratified sampling on column: {stratify_column}")
    
    try:
        # Get value counts for stratification
        value_counts = df.groupBy(stratify_column).count().collect()
        total_rows = df.count()
        
        sampled_dfs = []
        remaining_sample = sample_size
        
        for row in value_counts:
            value = row[stratify_column]
            count = row['count']
            
            # Calculate sample size for this stratum
            stratum_sample_size = max(1, int((count / total_rows) * sample_size))
            stratum_sample_size = min(stratum_sample_size, count, remaining_sample)
            
            if stratum_sample_size > 0:
                stratum_df = df.filter(col(stratify_column) == value)
                stratum_sample = stratum_df.sample(withReplacement=False, 
                                                 fraction=stratum_sample_size / count, 
                                                 seed=42).limit(stratum_sample_size)
                sampled_dfs.append(stratum_sample)
                remaining_sample -= stratum_sample_size
        
        # Union all stratified samples
        if sampled_dfs:
            result = sampled_dfs[0]
            for df_part in sampled_dfs[1:]:
                result = result.union(df_part)
            return result
        else:
            return df.limit(0)
            
    except Exception as e:
        logger.error(f"Error in stratified sampling: {str(e)}")
        # Fallback to random sampling
        return df.sample(withReplacement=False, fraction=sample_size / df.count(), seed=42)

def random_sampling(df: DataFrame, sample_size: int) -> DataFrame:
    """
    Perform random sampling on the dataset.
    
    Args:
        df: Input DataFrame
        sample_size: Desired sample size
    
    Returns:
        DataFrame: Randomly sampled data
    """
    total_rows = df.count()
    
    if total_rows <= sample_size:
        logger.info("Dataset size is smaller than sample size, returning full dataset")
        return df
    
    sample_ratio = sample_size / total_rows
    logger.info(f"Random sampling: {sample_size} samples from {total_rows} rows (ratio: {sample_ratio:.4f})")
    
    return df.sample(withReplacement=False, fraction=sample_ratio, seed=42)

def adaptive_sampling(df: DataFrame, sample_size: int, 
                     sampling_strategy: str = "random") -> DataFrame:
    """
    Perform adaptive sampling based on data characteristics.
    
    Args:
        df: Input DataFrame
        sample_size: Desired sample size
        sampling_strategy: Strategy to use (random, systematic, stratified)
    
    Returns:
        DataFrame: Sampled data
    """
    logger.info(f"Adaptive sampling using strategy: {sampling_strategy}")
    
    if sampling_strategy == "random":
        return random_sampling(df, sample_size)
    elif sampling_strategy == "systematic":
        return systematic_sampling(df, sample_size)
    elif sampling_strategy == "stratified":
        # Try to find a good column for stratification
        numeric_columns = [field.name for field in df.schema.fields 
                          if field.dataType in ['int', 'long', 'float', 'double']]
        
        if numeric_columns:
            # Use first numeric column for stratification
            stratify_col = numeric_columns[0]
            logger.info(f"Using column '{stratify_col}' for stratification")
            return stratified_sampling(df, sample_size, stratify_col)
        else:
            logger.info("No suitable column for stratification, falling back to random sampling")
            return random_sampling(df, sample_size)
    else:
        raise ValueError(f"Unsupported sampling strategy: {sampling_strategy}")

def create_sample_comparison(df1: DataFrame, df2: DataFrame, 
                           sample_size: int, sampling_strategy: str = "random") -> Dict[str, Any]:
    """
    Create and compare samples from both datasets.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        sample_size: Sample size for each dataset
        sampling_strategy: Sampling strategy to use
    
    Returns:
        Dict: Sample comparison results
    """
    logger.info("Creating sample comparison")
    
    try:
        # Create samples
        sample1 = adaptive_sampling(df1, sample_size, sampling_strategy)
        sample2 = adaptive_sampling(df2, sample_size, sampling_strategy)
        
        # Get sample metadata
        sample1_metadata = {
            'row_count': sample1.count(),
            'columns': sample1.columns
        }
        sample2_metadata = {
            'row_count': sample2.count(),
            'columns': sample2.columns
        }
        
        # Compare samples using fingerprints
        sample1_fp = create_data_fingerprint(sample1)
        sample2_fp = create_data_fingerprint(sample2)
        
        fingerprint_comparison = compare_fingerprints(sample1_fp, sample2_fp)
        
        result = {
            'sample1_metadata': sample1_metadata,
            'sample2_metadata': sample2_metadata,
            'fingerprint_comparison': fingerprint_comparison,
            'sampling_strategy': sampling_strategy,
            'sample_size': sample_size
        }
        
        logger.info("Sample comparison completed")
        return result
        
    except Exception as e:
        logger.error(f"Error in sample comparison: {str(e)}")
        raise

def detect_data_drift(df1: DataFrame, df2: DataFrame, 
                     sample_size: int = 10000) -> Dict[str, Any]:
    """
    Detect potential data drift between datasets using statistical methods.
    
    Args:
        df1: First DataFrame (baseline)
        df2: Second DataFrame (current)
        sample_size: Sample size for drift detection
    
    Returns:
        Dict: Data drift detection results
    """
    logger.info("Detecting data drift")
    
    try:
        # Create samples for drift detection
        sample1 = adaptive_sampling(df1, sample_size, "random")
        sample2 = adaptive_sampling(df2, sample_size, "random")
        
        # Get common columns
        common_columns = set(sample1.columns) & set(sample2.columns)
        common_columns = [c for c in common_columns if c not in ["__row_id", "__fingerprint"]]
        
        drift_results = {}
        
        for col_name in common_columns:
            try:
                # Get basic statistics for both samples
                stats1 = sample1.select(col(col_name)).describe().collect()
                stats2 = sample2.select(col(col_name)).describe().collect()
                
                # Convert to dictionary for easier comparison
                stats1_dict = {row['summary']: row[col_name] for row in stats1}
                stats2_dict = {row['summary']: row[col_name] for row in stats2}
                
                # Calculate drift indicators
                drift_indicators = []
                
                # Compare means if available
                if 'mean' in stats1_dict and 'mean' in stats2_dict:
                    try:
                        mean1 = float(stats1_dict['mean'])
                        mean2 = float(stats2_dict['mean'])
                        mean_diff = abs(mean2 - mean1)
                        mean_change_pct = (mean_diff / abs(mean1)) * 100 if mean1 != 0 else 0
                        
                        if mean_change_pct > 10:  # 10% threshold
                            drift_indicators.append({
                                'type': 'mean_change',
                                'value': mean_change_pct,
                                'threshold': 10
                            })
                    except (ValueError, TypeError):
                        pass
                
                # Compare standard deviations
                if 'stddev' in stats1_dict and 'stddev' in stats2_dict:
                    try:
                        std1 = float(stats1_dict['stddev'])
                        std2 = float(stats2_dict['stddev'])
                        std_diff = abs(std2 - std1)
                        std_change_pct = (std_diff / std1) * 100 if std1 != 0 else 0
                        
                        if std_change_pct > 20:  # 20% threshold
                            drift_indicators.append({
                                'type': 'stddev_change',
                                'value': std_change_pct,
                                'threshold': 20
                            })
                    except (ValueError, TypeError):
                        pass
                
                drift_results[col_name] = {
                    'drift_detected': len(drift_indicators) > 0,
                    'drift_indicators': drift_indicators,
                    'stats1': stats1_dict,
                    'stats2': stats2_dict
                }
                
            except Exception as e:
                logger.error(f"Error detecting drift for column {col_name}: {str(e)}")
                drift_results[col_name] = {
                    'drift_detected': False,
                    'error': str(e)
                }
        
        # Overall drift detection
        columns_with_drift = [col for col, result in drift_results.items() 
                             if result.get('drift_detected', False)]
        
        overall_drift = {
            'drift_detected': len(columns_with_drift) > 0,
            'columns_with_drift': columns_with_drift,
            'total_columns_checked': len(common_columns),
            'drift_percentage': (len(columns_with_drift) / len(common_columns)) * 100 if common_columns else 0,
            'detailed_results': drift_results
        }
        
        logger.info(f"Data drift detection completed. Drift detected in {len(columns_with_drift)} columns")
        return overall_drift
        
    except Exception as e:
        logger.error(f"Error in drift detection: {str(e)}")
        raise
