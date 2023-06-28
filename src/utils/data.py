import os
import shutil

import pandas as pd

from src.utils.misc import get_project_root

DATA_DIR = get_project_root() / "data"
LOG_DIR = get_project_root() / "data/parser"

def create_datasets(start_year=1946, end_year=1991):
    output_file = LOG_DIR / "download_images_log_updated.csv.gz"
    df = pd.read_csv(output_file, compression='gzip')

    datasets = ['train', 'val', 'test']
    # Create directories for the datasets
    for dataset in datasets:
        os.makedirs(DATA_DIR / dataset, exist_ok=True)

    # For each year from 1946 to 1991
    for year in range(start_year, end_year + 1):
        # Filter out issues of the current year with exactly 4 pages
        filtered_df = df[(df['year'] == year) & df['page'].between(1, 4)]

        # Group the data by date (YYYY_MM_DD) and keep only those dates with exactly 4 entries (pages)
        issues_with_4_pages = filtered_df['filename'].str.slice(0, 10).value_counts() == 4

        # Filter the DataFrame to include only issues with exactly 4 pages
        filtered_df = filtered_df[filtered_df['filename'].str.slice(0, 10).isin(issues_with_4_pages[issues_with_4_pages].index)]

        # Check if there are at least 3 issues to select for the datasets
        if len(filtered_df['filename'].str.slice(0, 10).unique()) < 3:
            print(f"Year {year} does not have enough issues with 4 pages. Skipping this year.")
            continue

        # Randomly sample three different issues from the filtered DataFrame
        selected_issues = filtered_df['filename'].str.slice(0, 10).sample(n=3).values

        # Copy the 4 pages of each selected issue to each respective dataset directory
        for dataset, issue in zip(datasets, selected_issues):
            for _, row in filtered_df[filtered_df['filename'].str.startswith(issue)].iterrows():
                src = os.path.join(DATA_DIR / "raw_data", row['filename'])
                dst = os.path.join(DATA_DIR / dataset, row['filename'])
                shutil.copy(src, dst)


if __name__ == "__main__":
    create_datasets(start_year=1946, end_year=1991)