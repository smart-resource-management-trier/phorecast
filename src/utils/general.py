"""
This module contains general utility functions that are used across the project.
"""

import base64
from io import BytesIO

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sqlalchemy.orm import DeclarativeBase

from src.utils.logging import get_default_logger

logger = get_default_logger(__name__)


class Base(DeclarativeBase):
    """
    Base class for all SQLAlchemy models in the project.
    """


def plot_predictions(df: pd.DataFrame, path: str = None, reference_name: str = 'Reference'):
    """
    Plots a DataFrame with a datetime index and three columns: 'label', 'prediction', and
    'reference'. 'label' and 'prediction' are plotted on the primary y-axis, 'reference' on the
    secondary y-axis. The plot is then saved to the specified path.

    :param df: pandas.DataFrame with datetime index and columns ['label', 'prediction', 'reference']
    :param path: str, path to save the plot image
    :param reference_name:
    """
    # Check if DataFrame has the required columns
    required_columns = ['label', 'prediction', 'reference']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"DataFrame must contain the columns: {required_columns}")

    # Create a plot figure and axis
    _, ax1 = plt.subplots()

    # Plot 'label' and 'prediction' on the primary y-axis
    color = 'tab:blue'
    df[['label', 'prediction']].plot(ax=ax1, color=['tab:blue', 'tab:green'])
    ax1.set_ylabel('Label and Prediction', color=color)
    ax1.tick_params(axis='y', labelcolor=color)

    # Create a secondary y-axis for 'reference'
    ax2 = ax1.twinx()
    color = 'tab:red'
    df['reference'].plot(ax=ax2, color=color, linestyle='--')
    ax2.set_ylabel(reference_name, color=color)
    ax2.tick_params(axis='y', labelcolor=color)

    # Title and labels
    plt.title(f'Comparison of Label, Prediction, and {reference_name}')
    ax1.set_xlabel('Date')

    # Save the plot
    plt.savefig(path, format='jpeg', dpi=300)
    plt.close()  # Close the plot to free up memory


def plot_history(history, path: str = None) -> str | None:
    """
    Plots the history of a training run, all parameters in the history dict will be plotted
    :param history: keras history object
    :param path: path to save the plot if None return html string
    :return: html string with base 64 encoded image
    """
    # Create a figure and subplots for different measures

    sns.set(style="whitegrid")
    sns.set_context("paper", font_scale=1.5)
    # List of measures to plot

    measures = [x for x in history.history.keys() if
                not (x.startswith('val_') or x == "loss" or "lr" in x)]
    fig, axes = plt.subplots(nrows=len(measures), ncols=1, figsize=(12, 4 * len(measures)))

    lowest_val = min(history.history['val_loss'])
    lowest_index = history.history['val_loss'].index(lowest_val)

    for idx, measure in enumerate(measures):
        ax = axes[idx] if len(measures) > 1 else axes  # Use single axis if only one measure

        sns.lineplot(data=history.history[measure], ax=ax, label='Train', linewidth=2)
        sns.lineplot(data=history.history['val_' + measure], ax=ax, label='Validation', linewidth=2)

        ax.set_title(f"Metric: {measure.replace('_', ' ')}")
        ax.set_xlabel('Epochs')
        ax.set_ylabel(measure)

        ax.legend()

    fig.suptitle(
        f'Metrics of the training run lowest validation loss: {lowest_val:.4f} on Epoch: '
        f'{lowest_index}')
    plt.tight_layout()

    if path is not None:
        plt.savefig(path, format='jpeg', dpi=300)
        return None

    tmp_file = BytesIO()
    plt.savefig(tmp_file, format='png', dpi=300)
    encoded = base64.b64encode(tmp_file.getvalue()).decode('utf-8')
    html = f'<img class="img-fluid" src=\'data:image/png;base64,{encoded}\'>'
    return html


def plot_windows(train: [pd.DataFrame], test: [pd.DataFrame], path: str = None):
    """
    Plots intervals of the given dataframes
    :param path: path to save the plot if not set, it will not be saved
    :param train: train list
    :param test: test list
    """

    plt.figure(figsize=(25, 3))
    for df in train:
        plt.hlines(y=1, xmin=df.index.min(), xmax=df.index.max(), color='blue', alpha=0.5,
                   linewidth=5)
    for df in test:
        plt.hlines(y=2, xmin=df.index.min(), xmax=df.index.max(), color='red', alpha=0.5,
                   linewidth=5)
    # Set labels and title
    plt.yticks([1, 2], ['train', 'test'])
    plt.xlabel('Date')
    plt.title('Staggered Time Intervals of DataFrames')

    # Add a custom legend
    plt.legend(['Train', 'Test'])
    plt.tight_layout()
    # Show plot
    if path is not None:
        plt.savefig(path, format='jpeg', dpi=300)
        return

    plt.show()
