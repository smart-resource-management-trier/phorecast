import os.path
import unittest
from collections import namedtuple
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from src.utils.general import  plot_history, plot_predictions

History = namedtuple('History', ['history'])




class TestPlotHistory(unittest.TestCase):
    def setUp(self):
        # Mock a history object
        self.mock_history = History(history={
            'accuracy': np.random.random(10).tolist(),
            'val_accuracy': np.random.random(10).tolist(),
            'loss': np.random.random(10).tolist(),
            'val_loss': np.random.random(10).tolist(),
        })

    def test_output_type(self):
        html = plot_history(self.mock_history)
        self.assertIsInstance(html, str, "Output should be a string")

    def test_output_content(self):
        html = plot_history(self.mock_history)
        self.assertTrue(html.startswith('<img class="img-fluid" src=\'data:image/png;base64,'),
                        "Output should start with the correct HTML img tag")

    def test_invalid_input(self):
        with self.assertRaises(Exception):
            plot_history(None)

class TestPlotAndSave(unittest.TestCase):
    def setUp(self):
        """Create a sample DataFrame for testing."""
        self.idx = pd.date_range('2023-01-01', periods=10, freq='D')
        self.data = pd.DataFrame({
            'label': range(10),
            'prediction': range(10, 20),
            'reference': range(20, 30)
        }, index=self.idx)
        self.path = 'test_plot.png'

    def test_plot_and_save_correct_columns(self):
        """Test the function with correct DataFrame structure."""
        try:
            plot_predictions(self.data, self.path)
            self.assertTrue(Path(self.path).is_file(), "File should be saved.")
        finally:
            # Clean up: Remove the created file after the test
            if os.path.exists(self.path):
                os.remove(self.path)

    def test_plot_and_save_missing_columns(self):
        """Test the function with missing columns in the DataFrame."""
        # Remove one required column
        data_with_missing_column = self.data.drop(columns=['reference'])
        with self.assertRaises(ValueError):
            plot_predictions(data_with_missing_column, self.path)
if __name__ == '__main__':
    unittest.main()
