import yaml
import os
from steps.z_score_pipeline.filter_step import filter_step
from steps.z_score_pipeline.bootstrap_step import bootstrap_step
from steps.z_score_pipeline.connection_matrices_step import connection_matrices_step
from steps.z_score_pipeline.calculate_ci_step import calculate_ci_step
from steps.z_score_pipeline.analyze_ci_step import analyze_ci_step

def read_config():
    # Set working directory to src (script directory)
    SRC_DIR = os.path.dirname(os.path.abspath(__file__))
    os.chdir(SRC_DIR)
    with open('configs/z_score_pipeline.yaml', 'r') as file:
        config = yaml.safe_load(file)
        return config


if __name__ == '__main__':
    config = read_config()
    
    # Get experiment_name from top-level config
    experiment_name = config.get('experiment_name')
    if experiment_name is None:
        raise ValueError("experiment_name must be specified at the top level of the configuration")

    # Filter step configuration
    if 'filter_step' in config:
        hesin_data_path = config['filter_step']['HESIN_DATA_PATH']
        codes_path = config['filter_step']['CODES_PATH']
        method = config['filter_step']['method']
        filter_path = config['filter_step']['FILTER_PATH']
        output_path = config['filter_step']['OUTPUT_PATH']
        filteration = config['filter_step']['filteration']
        
        # Run filter step
        filter_step(experiment_name, hesin_data_path, codes_path, method, filter_path, output_path, filteration)

    # Bootstrap step configuration
    shuffled_dfs = []
    shuffle_iterations = 0
    if 'bootstrap_step' in config:
        # Fix the input directory to use the correct experiment name from filter_step
        # This ensures bootstrap step reads from the same filtered_data directory that filter_step created
        bootstrap_input_dir = os.path.join(config['filter_step']['OUTPUT_PATH'], experiment_name, "filtered_data")
        bootstrap_output_base_dir = config['bootstrap_step']['OUTPUT_BASE_DIR']
        fields_to_keep = config['bootstrap_step']['FIELDS_TO_KEEP']
        shuffle_iterations = config['bootstrap_step']['SHUFFLE_ITERATIONS']
        save_bootstrap_data = config['bootstrap_step'].get('SAVE_BOOTSTRAP_DATA', True)
        
        # Run bootstrap step - it will automatically detect all CSV files in the input directory
        # Capture the returned shuffled DataFrames
        shuffled_dfs = bootstrap_step(experiment_name, bootstrap_input_dir, bootstrap_output_base_dir, 
                                      fields_to_keep, shuffle_iterations, save_bootstrap_data)
        
        if shuffled_dfs is None:
            shuffled_dfs = []

    # Disease score step configuration
    connection_matrices = {}
    if 'disease_score_step' in config:
        original_data_dir = os.path.join(config['filter_step']['OUTPUT_PATH'], experiment_name, "filtered_data")
        output_base_dir = config['disease_score_step']['OUTPUT_BASE_DIR']
        
        # Run connection matrices step with original data directory and shuffled DataFrames
        # Capture the returned connection matrices
        if shuffled_dfs:
            connection_matrices = connection_matrices_step(original_data_dir, shuffled_dfs, output_base_dir, experiment_name, shuffle_iterations)
        else:
            print("  ⚠ Warning: No shuffled DataFrames available. Skipping connection matrices step.")

    # Calculate CI step configuration
    ci_matrices = {}
    if 'calculate_ci_step' in config:
        output_base_dir = config['calculate_ci_step'].get('OUTPUT_BASE_DIR', config.get('disease_score_step', {}).get('OUTPUT_BASE_DIR'))
        z_alpha = config['calculate_ci_step'].get('Z_ALPHA', 1.96)
        
        # Run calculate CI step with connection matrices
        if connection_matrices:
            ci_matrices = calculate_ci_step(connection_matrices, output_base_dir, experiment_name, z_alpha)
        else:
            print("  ⚠ Warning: No connection matrices available. Skipping calculate CI step.")

    # Analyze CI step configuration
    analysis_results = {}
    if 'analyze_ci_step' in config:
        output_base_dir = config['analyze_ci_step'].get('OUTPUT_BASE_DIR', config.get('calculate_ci_step', {}).get('OUTPUT_BASE_DIR', config.get('disease_score_step', {}).get('OUTPUT_BASE_DIR')))
        upper_threshold = config['analyze_ci_step'].get('UPPER_THRESHOLD', 3.0)
        lower_threshold = config['analyze_ci_step'].get('LOWER_THRESHOLD', 3.0)
        
        # Run analyze CI step with connection matrices and CI matrices
        if connection_matrices and ci_matrices:
            analysis_results = analyze_ci_step(connection_matrices, ci_matrices, output_base_dir, experiment_name, upper_threshold, lower_threshold)
        else:
            if not connection_matrices:
                print("  ⚠ Warning: No connection matrices available. Skipping analyze CI step.")
            if not ci_matrices:
                print("  ⚠ Warning: No CI matrices available. Skipping analyze CI step.")

