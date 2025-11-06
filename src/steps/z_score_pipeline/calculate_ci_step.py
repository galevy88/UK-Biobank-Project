import pandas as pd
import numpy as np
import os
from collections import defaultdict
from tqdm import tqdm


def calculate_ci_step(connection_matrices, output_base_dir, experiment_name, z_alpha=1.96):
    """
    Calculate confidence intervals for each cell position across bootstrap connection matrices.
    
    Args:
        connection_matrices: Dictionary mapping matrix names to connection matrix DataFrames
                           Keys format: "original_{base_name}" or "bootstrap_{base_name}_{idx}"
        output_base_dir: Base directory for saving outputs
        experiment_name: Experiment name to append to output directory
        z_alpha: Z-score for confidence interval (default: 1.96 for 95% CI)
    
    Returns:
        dict: Dictionary mapping filter types to CI matrices
              Keys: filter type names (e.g., "young", "old")
              Values: DataFrames with CI tuples as cell values
    """
    print("\n" + "="*70)
    print("Starting Calculate CI Step")
    print("="*70)
    print(f"Experiment: {experiment_name}")
    print(f"Output base directory: {output_base_dir}")
    print(f"Z-alpha (confidence level): {z_alpha}")
    print(f"Number of connection matrices: {len(connection_matrices)}")
    
    # Set working directory to src (parent of steps directory)
    SRC_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    os.chdir(SRC_DIR)
    print(f"Working directory set to: {os.getcwd()}")
    
    # Create output directory
    output_dir = os.path.join(output_base_dir, experiment_name, "ci_matrices")
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory: {output_dir}")
    
    # Separate bootstrap matrices by filter type
    bootstrap_matrices = defaultdict(list)
    
    for key, matrix_df in connection_matrices.items():
        if key.startswith("bootstrap_"):
            # Extract filter type and bootstrap index
            # Format: "bootstrap_{base_name}_{idx}"
            parts = key.replace("bootstrap_", "").split("_")
            if len(parts) >= 2:
                bootstrap_idx = int(parts[-1])
                base_name = "_".join(parts[:-1])
                bootstrap_matrices[base_name].append((bootstrap_idx, matrix_df))
    
    print(f"\nFound {len(bootstrap_matrices)} bootstrap filter type(s)")
    
    if len(bootstrap_matrices) == 0:
        print("  ⚠ Warning: No bootstrap matrices found. Cannot calculate CIs.")
        return {}
    
    ci_matrices = {}
    
    # Process each filter type
    for filter_type in sorted(bootstrap_matrices.keys()):
        print(f"\n[Processing] Filter type: {filter_type}")
        
        # Get all bootstrap matrices for this filter type, sorted by index
        bootstrap_list = sorted(bootstrap_matrices[filter_type], key=lambda x: x[0])
        num_bootstrap = len(bootstrap_list)
        
        print(f"  ✓ Found {num_bootstrap} bootstrap matrices")
        
        if num_bootstrap == 0:
            print(f"  ⚠ Warning: No bootstrap matrices for {filter_type}, skipping...")
            continue
        
        # Get the first matrix to determine structure
        first_matrix = bootstrap_list[0][1]
        diseases = sorted(first_matrix.index.tolist())
        matrix_size = len(diseases)
        
        print(f"  ✓ Matrix size: {matrix_size} x {matrix_size} diseases")
        
        # Ensure all matrices have the same structure (same diseases in same order)
        aligned_matrices = []
        for bootstrap_idx, matrix_df in bootstrap_list:
            # Reindex to ensure consistent structure
            aligned_matrix = matrix_df.reindex(index=diseases, columns=diseases, fill_value=0)
            aligned_matrices.append(aligned_matrix.values)
        
        # Stack all matrices into a 3D array: (num_bootstrap, matrix_size, matrix_size)
        stacked_matrices = np.array(aligned_matrices)
        
        print(f"  ✓ Stacked matrices shape: {stacked_matrices.shape}")
        
        # Calculate mean and std for each cell position across bootstrap iterations
        print(f"  Calculating mean and std for each cell position...")
        mean_matrix = np.mean(stacked_matrices, axis=0)
        std_matrix = np.std(stacked_matrices, axis=0, ddof=1)  # Sample standard deviation
        
        # Calculate confidence intervals for each cell
        # CI = (x̄ - Zα * (σ / √n), x̄ + Zα * (σ / √n))
        print(f"  Calculating confidence intervals...")
        standard_error = std_matrix / np.sqrt(num_bootstrap)
        margin_of_error = z_alpha * standard_error
        
        # Lower bound: x̄ - Zα * (σ / √n)
        ci_lower = mean_matrix - margin_of_error
        # Upper bound: x̄ + Zα * (σ / √n)
        ci_upper = mean_matrix + margin_of_error
        
        # Create DataFrame with CI tuples as cell values
        # Format: (lower_bound, upper_bound)
        ci_matrix = pd.DataFrame(
            index=diseases,
            columns=diseases,
            dtype=object
        )
        
        for i in range(matrix_size):
            for j in range(matrix_size):
                ci_matrix.iloc[i, j] = (ci_lower[i, j], ci_upper[i, j])
        
        ci_matrices[filter_type] = ci_matrix
        
        # Save the CI matrix
        ci_output_file = os.path.join(output_dir, f"{filter_type}_ci_matrix.csv")
        
        # For saving, we'll create a more readable format
        # Save as CSV with string representation of tuples
        ci_matrix_str = ci_matrix.applymap(lambda x: f"({x[0]:.6f}, {x[1]:.6f})" if isinstance(x, tuple) else str(x))
        ci_matrix_str.to_csv(ci_output_file)
        
        ci_file_size_mb = os.path.getsize(ci_output_file) / (1024 * 1024)
        print(f"  ✓ Saved CI matrix to: {ci_output_file}")
        print(f"  ✓ CI matrix file size: {ci_file_size_mb:.2f} MB")
        
        # Also save mean and std matrices for reference
        mean_df = pd.DataFrame(mean_matrix, index=diseases, columns=diseases)
        std_df = pd.DataFrame(std_matrix, index=diseases, columns=diseases)
        
        mean_output_file = os.path.join(output_dir, f"{filter_type}_mean_matrix.csv")
        std_output_file = os.path.join(output_dir, f"{filter_type}_std_matrix.csv")
        
        mean_df.to_csv(mean_output_file)
        std_df.to_csv(std_output_file)
        
        mean_file_size_mb = os.path.getsize(mean_output_file) / (1024 * 1024)
        std_file_size_mb = os.path.getsize(std_output_file) / (1024 * 1024)
        
        print(f"  ✓ Saved mean matrix to: {mean_output_file} ({mean_file_size_mb:.2f} MB)")
        print(f"  ✓ Saved std matrix to: {std_output_file} ({std_file_size_mb:.2f} MB)")
        
        # Print some statistics
        print(f"  ✓ CI Statistics:")
        print(f"    - Mean of lower bounds: {ci_lower.mean():.6f}")
        print(f"    - Mean of upper bounds: {ci_upper.mean():.6f}")
        print(f"    - Mean CI width: {(ci_upper - ci_lower).mean():.6f}")
        print(f"    - Min CI width: {(ci_upper - ci_lower).min():.6f}")
        print(f"    - Max CI width: {(ci_upper - ci_lower).max():.6f}")
    
    print("\n" + "="*70)
    print("Calculate CI Step Completed Successfully!")
    print("="*70)
    print(f"Summary:")
    print(f"  - Processed {len(ci_matrices)} filter type(s)")
    print(f"  - Output directory: {output_dir}")
    for filter_type in ci_matrices.keys():
        print(f"  - {filter_type}:")
        print(f"    * {filter_type}_ci_matrix.csv (confidence intervals)")
        print(f"    * {filter_type}_mean_matrix.csv (mean values)")
        print(f"    * {filter_type}_std_matrix.csv (standard deviations)")
    print("="*70 + "\n")
    
    return ci_matrices


if __name__ == '__main__':
    # Default configuration for direct execution
    print("This step should be called from the main pipeline with connection_matrices as input.")

