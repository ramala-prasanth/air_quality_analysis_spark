import os
import pandas as pd

# Folder where all your CSV files are
folder_path = '/workspaces/air_quality_analysis_spark/ingestion/data/processed'

# List to hold all DataFrames
dfs = []

# Loop through all files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        df = pd.read_csv(file_path)
        dfs.append(df)

# Concatenate all DataFrames
merged_df = pd.concat(dfs, ignore_index=True)

# Save to a single CSV file
merged_df.to_csv('merged_file.csv', index=False)

print("All CSV files have been merged successfully!")
