import pandas as pd
import numpy as np
import os
from tqdm import tqdm


def analyze_ci_step(connection_matrices, ci_matrices, output_base_dir, experiment_name, upper_threshold, lower_threshold):
    """
    Analyze original connection matrices against CI matrices.
    
    For each cell position:
    - If original_value > upper_threshold * upper_ci: write original_value/upper_ci, otherwise 0
    - If original_value < lower_threshold * lower_ci: write original_value/lower_ci, otherwise 0
    
    Args:
        connection_matrices: Dictionary mapping matrix names to connection matrix DataFrames
                           Keys format: "original_{base_name}" or "bootstrap_{base_name}_{idx}"
        ci_matrices: Dictionary mapping filter types to CI matrices
                   Keys: filter type names (e.g., "young", "old")
                   Values: DataFrames with CI tuples as cell values
        output_base_dir: Base directory for saving outputs
        experiment_name: Experiment name to append to output directory
        upper_threshold: Threshold multiplier for upper CI comparison
        lower_threshold: Threshold multiplier for lower CI comparison
    
    Returns:
        dict: Dictionary mapping filter types to analysis matrices
              Keys: filter type names
              Values: DataFrames with analysis results (upper CI ratios and lower CI ratios)
    """
    print("\n" + "="*70)
    print("Starting Analyze CI Step")
    print("="*70)
    print(f"Experiment: {experiment_name}")
    print(f"Output base directory: {output_base_dir}")
    print(f"Upper threshold: {upper_threshold}")
    print(f"Lower threshold: {lower_threshold}")
    print(f"Number of CI matrices: {len(ci_matrices)}")
    
    # Set working directory to src (parent of steps directory)
    SRC_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    os.chdir(SRC_DIR)
    print(f"Working directory set to: {os.getcwd()}")
    
    # Create output directory
    output_dir = os.path.join(output_base_dir, experiment_name, "ci_analysis")
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory: {output_dir}")
    
    # Extract original matrices
    original_matrices = {}
    for key, matrix_df in connection_matrices.items():
        if key.startswith("original_"):
            base_name = key.replace("original_", "")
            original_matrices[base_name] = matrix_df
    
    print(f"\nFound {len(original_matrices)} original matrix type(s)")
    print(f"Found {len(ci_matrices)} CI matrix type(s)")
    
    if len(ci_matrices) == 0:
        print("  ⚠ Warning: No CI matrices found. Cannot perform analysis.")
        return {}
    
    if len(original_matrices) == 0:
        print("  ⚠ Warning: No original matrices found. Cannot perform analysis.")
        return {}
    
    analysis_results = {}
    
    # Process each filter type that has both original and CI matrices
    common_types = set(original_matrices.keys()) & set(ci_matrices.keys())
    
    if len(common_types) == 0:
        print("  ⚠ Warning: No matching filter types between original and CI matrices.")
        print(f"  Original types: {list(original_matrices.keys())}")
        print(f"  CI types: {list(ci_matrices.keys())}")
        return {}
    
    print(f"\nProcessing {len(common_types)} matching filter type(s)")
    
    for filter_type in sorted(common_types):
        print(f"\n[Processing] Filter type: {filter_type}")
        
        original_matrix = original_matrices[filter_type]
        ci_matrix = ci_matrices[filter_type]
        
        # Ensure matrices have the same structure
        diseases = sorted(set(original_matrix.index.tolist()) & set(ci_matrix.index.tolist()))
        
        if len(diseases) == 0:
            print(f"  ⚠ Warning: No common diseases between original and CI matrices for {filter_type}")
            continue
        
        # Reindex both matrices to have the same structure
        original_aligned = original_matrix.reindex(index=diseases, columns=diseases, fill_value=0)
        ci_aligned = ci_matrix.reindex(index=diseases, columns=diseases, fill_value=(0, 0))
        
        matrix_size = len(diseases)
        print(f"  ✓ Matrix size: {matrix_size} x {matrix_size} diseases")
        
        # Create matrices for upper and lower CI analysis
        upper_ci_analysis = pd.DataFrame(
            index=diseases,
            columns=diseases,
            dtype=float
        )
        
        lower_ci_analysis = pd.DataFrame(
            index=diseases,
            columns=diseases,
            dtype=float
        )
        
        print(f"  Analyzing each cell position...")
        
        # Analyze each cell
        for i in tqdm(range(matrix_size), desc=f"  {filter_type} cells", leave=False):
            for j in range(matrix_size):
                original_value = original_aligned.iloc[i, j]
                ci_tuple = ci_aligned.iloc[i, j]
                
                # Extract CI bounds
                if isinstance(ci_tuple, tuple) and len(ci_tuple) == 2:
                    lower_ci, upper_ci = ci_tuple
                else:
                    # Handle case where CI might be stored as string or other format
                    try:
                        if isinstance(ci_tuple, str):
                            # Parse string like "(0.123, 0.456)"
                            ci_str = ci_tuple.strip('()')
                            parts = ci_str.split(',')
                            lower_ci = float(parts[0].strip())
                            upper_ci = float(parts[1].strip())
                        else:
                            lower_ci, upper_ci = 0.0, 0.0
                    except:
                        lower_ci, upper_ci = 0.0, 0.0
                
                # Upper CI analysis: if original_value > upper_threshold * upper_ci
                if upper_ci > 0 and original_value > upper_threshold * upper_ci:
                    upper_ci_analysis.iloc[i, j] = original_value / upper_ci
                else:
                    upper_ci_analysis.iloc[i, j] = 0.0
                
                # Lower CI analysis: if original_value < lower_threshold * lower_ci
                if lower_ci > 0 and original_value < lower_threshold * lower_ci:
                    lower_ci_analysis.iloc[i, j] = original_value / lower_ci
                else:
                    lower_ci_analysis.iloc[i, j] = 0.0
        
        analysis_results[filter_type] = {
            'upper_ci': upper_ci_analysis,
            'lower_ci': lower_ci_analysis
        }
        
        # Save the analysis matrices
        upper_output_file = os.path.join(output_dir, f"{filter_type}_upper_ci_analysis.csv")
        lower_output_file = os.path.join(output_dir, f"{filter_type}_lower_ci_analysis.csv")
        
        upper_ci_analysis.to_csv(upper_output_file)
        lower_ci_analysis.to_csv(lower_output_file)
        
        upper_file_size_mb = os.path.getsize(upper_output_file) / (1024 * 1024)
        lower_file_size_mb = os.path.getsize(lower_output_file) / (1024 * 1024)
        
        print(f"  ✓ Saved upper CI analysis to: {upper_output_file} ({upper_file_size_mb:.2f} MB)")
        print(f"  ✓ Saved lower CI analysis to: {lower_output_file} ({lower_file_size_mb:.2f} MB)")
        
        # Print statistics
        upper_non_zero = (upper_ci_analysis > 0).sum().sum()
        lower_non_zero = (lower_ci_analysis > 0).sum().sum()
        total_cells = matrix_size * matrix_size
        
        print(f"  ✓ Analysis Statistics:")
        print(f"    - Total cells: {total_cells:,}")
        print(f"    - Upper CI threshold exceeded: {upper_non_zero:,} cells ({100*upper_non_zero/total_cells:.2f}%)")
        print(f"    - Lower CI threshold exceeded: {lower_non_zero:,} cells ({100*lower_non_zero/total_cells:.2f}%)")
        if upper_non_zero > 0:
            print(f"    - Mean upper CI ratio: {upper_ci_analysis[upper_ci_analysis > 0].mean().mean():.6f}")
            print(f"    - Max upper CI ratio: {upper_ci_analysis.max().max():.6f}")
        if lower_non_zero > 0:
            print(f"    - Mean lower CI ratio: {lower_ci_analysis[lower_ci_analysis > 0].mean().mean():.6f}")
            print(f"    - Min lower CI ratio: {lower_ci_analysis[lower_ci_analysis > 0].min().min():.6f}")
    
    print("\n" + "="*70)
    print("Analyze CI Step Completed Successfully!")
    print("="*70)
    print(f"Summary:")
    print(f"  - Processed {len(analysis_results)} filter type(s)")
    print(f"  - Output directory: {output_dir}")
    for filter_type in analysis_results.keys():
        print(f"  - {filter_type}:")
        print(f"    * {filter_type}_upper_ci_analysis.csv")
        print(f"    * {filter_type}_lower_ci_analysis.csv")
    print("="*70 + "\n")
    
    return analysis_results


if __name__ == '__main__':
    # Default configuration for direct execution
    print("This step should be called from the main pipeline with connection_matrices and ci_matrices as input.")

