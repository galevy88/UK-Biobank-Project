import unittest
import pandas as pd
import os
import tempfile
import shutil
from src.utils.get_data import merge_csv_files
from pandas.testing import assert_frame_equal


class TestMergeCsvFiles(unittest.TestCase):
    def setUp(self):
        # Define the path to the existing dummy_csvs directory
        self.dummy_csvs_dir = os.path.abspath('tests/dummy_csvs')
        # Create a temporary directory for tests
        self.temp_dir = tempfile.mkdtemp()
        # Copy existing CSV files to the temporary directory
        for filename in ['file1.csv', 'file2.csv', 'file3.csv']:
            src = os.path.join(self.dummy_csvs_dir, filename)
            dst = os.path.join(self.temp_dir, filename)
            if os.path.exists(src):
                shutil.copy(src, dst)

    def tearDown(self):
        # Clean up the temporary directory
        shutil.rmtree(self.temp_dir)

    def test_merge_csv_files_success(self):
        # Test merging the three CSV files
        result = merge_csv_files(self.temp_dir, merge_column='participant_id')
        # Expected output DataFrame based on the provided CSV files
        expected = pd.DataFrame({
            'participant_id': [1, 2, 3, 4, 5],
            'age': [25.0, 30.0, 35.0, None, None],
            'weight': [None, 70.0, 80.0, 75.0, None],
            'blood_pressure': [120.0, None, 130.0, None, 125.0]
        })
        assert_frame_equal(result, expected, check_dtype=False)

    def test_merge_csv_files_empty_directory(self):
        # Test with an empty directory
        empty_dir = tempfile.mkdtemp()
        with self.assertRaises(ValueError) as context:
            merge_csv_files(empty_dir)
        self.assertEqual(str(context.exception), "No CSV files found in the directory")
        shutil.rmtree(empty_dir)

    def test_merge_csv_files_invalid_column(self):
        # Test with a non-existent merge column
        with self.assertRaises(KeyError):
            merge_csv_files(self.temp_dir, merge_column='invalid_column')


if __name__ == '__main__':
    unittest.main()