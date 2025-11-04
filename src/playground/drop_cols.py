import pandas as pd

# Read the CSV file
df = pd.read_csv('data/sanity_check/input/ukb_participant.csv')

# Keep only the specified columns
df = df[['eid', 'Age at recruitment', 'Sex']]

# Save the result to a new CSV file (optional)
df.to_csv('filtered_file.csv', index=False)

# Display the resulting DataFrame
print(df.shape)