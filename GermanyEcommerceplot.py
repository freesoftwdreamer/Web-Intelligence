import pandas as pd
import matplotlib.pyplot as plt

# Read the CSV file
df = pd.read_csv('NEWDEinput.csv')

# Count the frequencies of the 'ecommerce' field
ecommerce_counts = df['ecommerce'].value_counts()

# Plot the histogram
plt.bar(ecommerce_counts.index, ecommerce_counts.values, width=0.8, align='center')
plt.xlabel('E-commerce Indicator')
plt.ylabel('Frequency')
plt.title('Histogram of E-commerce Indicator Frequencies')
plt.xticks(ecommerce_counts.index)
plt.show()
