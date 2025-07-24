import pandas as pd
import os
import glob

# Get all CSV files in the data folder
data_folder = "data"
csv_files = glob.glob(os.path.join(data_folder, "*.csv"))

print(f"Found CSV files in {data_folder}:")
for file in csv_files:
    print(f"  - {file}")

# List to store all queries from all files
all_queries = []

# Iterate through each CSV file
for csv_file in csv_files:
    print(f"\nProcessing {csv_file}...")
    
    try:
        # Read only the 'query' column from each CSV file
        df = pd.read_csv(csv_file, usecols=['query'])
        
        print(f"  - Found {len(df)} queries")
        
        # Add queries to the master list
        all_queries.extend(df['query'].tolist())
        
    except Exception as e:
        print(f"  - Error reading {csv_file}: {e}")

# Create a DataFrame with all queries
print(f"\nCombining all queries...")
combined_df = pd.DataFrame({'query': all_queries})

print(f"Total queries from all files: {len(combined_df)}")
print(f"Duplicate queries: {combined_df['query'].duplicated().sum()}")

# Remove duplicates
unique_df = combined_df.drop_duplicates(subset=['query'])

print(f"Unique queries after removing duplicates: {len(unique_df)}")
print(f"Removed {len(combined_df) - len(unique_df)} duplicate queries")

# Save to nostros_query.csv
output_file = "data/nostros_query.csv"
unique_df.to_csv(output_file, index=False)

print(f"\nSaved {len(unique_df)} unique queries to '{output_file}'")

# Display first few unique queries
print(f"\nFirst 5 unique queries:")
print(unique_df['query'].head())