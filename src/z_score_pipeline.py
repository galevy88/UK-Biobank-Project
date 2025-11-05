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
    if 'bootstrap_step' in config:
        # Fix the input directory to use the correct experiment name from filter_step
        # This ensures bootstrap step reads from the same filtered_data directory that filter_step created
        bootstrap_input_dir = os.path.join(config['filter_step']['OUTPUT_PATH'], experiment_name, "filtered_data")
        bootstrap_output_base_dir = config['bootstrap_step']['OUTPUT_BASE_DIR']
        fields_to_keep = config['bootstrap_step']['FIELDS_TO_KEEP']
        shuffle_iterations = config['bootstrap_step']['SHUFFLE_ITERATIONS']
        save_bootstrap_data = config['bootstrap_step'].get('SAVE_BOOTSTRAP_DATA', True)
        
        # Run bootstrap step - it will automatically detect all CSV files in the input directory
        bootstrap_step(experiment_name, bootstrap_input_dir, bootstrap_output_base_dir, 
                      fields_to_keep, shuffle_iterations, save_bootstrap_data)

    # Disease score step configuration
    if 'disease_score_step' in config:
        original_data_dir = os.path.join(config['filter_step']['OUTPUT_PATH'], experiment_name, "filtered_data")
        bootstrap_data_dir = os.path.join(config['bootstrap_step']['OUTPUT_BASE_DIR'], experiment_name, "bootstraped_hesin_data")
        output_base_dir = config['disease_score_step']['OUTPUT_BASE_DIR']
        
        # Run connection matrices step with original and bootstrap directories
        connection_matrices_step(original_data_dir, bootstrap_data_dir, output_base_dir, experiment_name)

