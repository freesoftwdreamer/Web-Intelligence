import pandas as pd
import requests

# Read the CSV file
df = pd.read_csv('DEinput.csv')
name_url = 1
# Specify the number of URLs to print
num_urls = 999

# Assume the URLs are in the name_url variable
urls = df.iloc[:num_urls, name_url]

# Keywords to look for
keywords_german = [
    'Kundenerkennung', 'Individuelle Preisgestaltung', 'Sitzungsmanagement',
    'cart', 'online', 'shop', 'Online-Shop', 'leistungen',
    'Leinstungen', 'service', 'Service', 'Reservierung',
    'Barbershop', 'bookings', 'versicherung', 'Mitgliedschaft',
    'datenshutz'
]

# List to store results
results = []

for url in urls:
    try:
        # Send a GET request to the URL
        response = requests.get(url)

        # Initialize counters for keyword matches
        keyword_matches = 0

        # Check if the response contains "Barbershop"
        if "Barbershop" in response.text:
            print(f"{url} is likely not an e-commerce site as it contains 'Barbershop' in its HTML.")
            probability = 0.0  # Assign zero probability for Barbershop URLs
        else:
            # Check for e-commerce keywords
            for keyword in keywords_german:
                if keyword in response.text:
                    keyword_matches += 1  # Increment match counter

            # Calculate probability (as a percentage)
            total_keywords = len(keywords_german)
            probability = (keyword_matches / total_keywords) * 100 if total_keywords > 0 else 0

        # Append the URL and its probability to results list
        results.append({'URL': url, 'Probability (%)': probability})

    except requests.exceptions.RequestException as e:
        print(f'Skipped {url} due to an error: {e}')
        # Append skipped URL with NaN probability
        results.append({'URL': url, 'Probability (%)': None})

# Create a DataFrame from results and save to CSV
results_df = pd.DataFrame(results)
results_df.to_csv('ecommerce_probabilities.csv', index=False)

print("Probabilities have been saved to 'ecommerce_probabilities.csv'.")
