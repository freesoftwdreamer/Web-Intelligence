import pandas as pd

# Read the CSV file
df = pd.read_csv('input.csv')

# Define the country-specific output files
output_files = {
    'AT': 'ATinput.csv',
    'DE': 'DEinput.csv',
    'NL': 'NLinput.csv',
    'PL': 'PLinput.csv'
}

# Iterate over each row in the DataFrame
for index, row in df.iterrows():
    # Determine the output file based on the country
    output_file = output_files.get(row['country'])

    # If an output file was found, write the row to it
    if output_file:
        # Convert the Series to a DataFrame and append it to the corresponding CSV
        row.to_frame().T.to_csv(output_file, mode='a', header=not pd.io.common.file_exists(output_file), index=False)
