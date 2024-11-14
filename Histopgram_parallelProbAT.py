#ecommerce_probabilities_parallel_ATDATASET.csv

import pandas as pd
import matplotlib.pyplot as plt

# Step 1: Load the CSV Data
# Assuming your CSV file is named 'data.csv'
df = pd.read_csv('ecommerce_probabilities_parallel_ATDATASET.csv')

# Step 2: Check for non-numeric values in the 'Probability (%)' column
if not pd.to_numeric(df['Probability (%)'], errors='coerce').notnull().all():
    raise ValueError("The 'Probability (%)' column contains non-numeric values.")

# Step 3: Preprocess the Data
# Convert the 'Probability (%)' column to numeric
df['Probability (%)'] = pd.to_numeric(df['Probability (%)'], errors='coerce')

# Round the probabilities to the nearest integer
df['Rounded_Probability'] = df['Probability (%)'].round().astype(int)

# Count the frequencies of the rounded values
rounded_counts = df['Rounded_Probability'].value_counts().sort_index()

# Calculate the percentage of each rounded value
total_count = len(df)
rounded_percentages = (rounded_counts / total_count) * 100

# Step 4: Plot the Histogram
plt.bar(rounded_percentages.index, rounded_percentages.values, width=0.8, align='center')
plt.xlabel('Rounded Probability (%)')
plt.ylabel('Frequency (%)')
plt.title('Histogram of Rounded Probability Frequencies')
plt.xticks(rounded_percentages.index)

# Step 5: Compute the percentage of rounded probabilities >= 20
rounded_probabilities_ge_20 = df[df['Rounded_Probability'] >= 20]
percentage_ge_20 = (len(rounded_probabilities_ge_20) / total_count) * 100

# Step 6: Compute the percentage of rounded probabilities < 20
rounded_probabilities_lt_20 = df[df['Rounded_Probability'] < 20]
percentage_lt_20 = (len(rounded_probabilities_lt_20) / total_count) * 100

# Step 7: Verify the sum of both percentages
total_percentage = percentage_ge_20 + percentage_lt_20

print(f"Percentage of rounded probabilities >= 20: {percentage_ge_20:.2f}%")
print(f"Percentage of rounded probabilities < 20: {percentage_lt_20:.2f}%")
print(f"Total percentage: {total_percentage:.2f}%")

# Check if the total percentage is 100%
if total_percentage == 100.0:
    print("The sum of both percentages equals 100%.")
else:
    print("The sum of both percentages does not equal 100%.")

# Step 8: Annotate the bars with the number of observations over the total for the ones >= 20%
for index, value in rounded_percentages.items():
    if index >= 20:
        plt.text(index, value, f'{value:.2f}%', ha='center', va='bottom')

plt.show()
