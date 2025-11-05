import yaml
import os
from steps.z_score_pipeline.filter_step import filter_step
from steps.z_score_pipeline.bootstrap_step import bootstrap_step
from steps.z_score_pipeline.connection_matrices_step import connection_matrices_step

def read_config():
    # Set working directory to src (script directory)
    SRC_DIR = os.path.dirname(os.path.abspath(__file__))
    os.chdir(SRC_DIR)
    with open('configs/z_score_pipeline.yaml', 'r') as file:
        config = yaml.safe_load(file)
        return config


if __name__ == '__main__':
    config = read_config()

    # Filter step configuration
    experiment_name = config['filter_step']['experiment_name']
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
        bootstrap_input_dir = config['bootstrap_step']['INPUT_DATA_DIR']
        bootstrap_output_base_dir = config['bootstrap_step']['OUTPUT_BASE_DIR']
        fields_to_keep = config['bootstrap_step']['FIELDS_TO_KEEP']
        shuffle_iterations = config['bootstrap_step']['SHUFFLE_ITERATIONS']
        save_bootstrap_data = config['bootstrap_step'].get('SAVE_BOOTSTRAP_DATA', True)
        
        # Run bootstrap step
        shuffled_dfs = bootstrap_step(experiment_name, bootstrap_input_dir, bootstrap_output_base_dir, 
                                      fields_to_keep, shuffle_iterations, save_bootstrap_data)
        
        if shuffled_dfs is None:
            shuffled_dfs = []

    # Disease score step configuration
    if 'disease_score_step' in config:
        original_data_dir = os.path.join(config['filter_step']['OUTPUT_PATH'], experiment_name, "filtered_data")
        output_base_dir = config['disease_score_step']['OUTPUT_BASE_DIR']
        
        # Run connection matrices step with original and shuffled DataFrames
        if shuffled_dfs:
            connection_matrices_step(original_data_dir, shuffled_dfs, output_base_dir, experiment_name, shuffle_iterations)
        else:
            print("  ⚠ Warning: No shuffled DataFrames available. Skipping connection matrices step.")

