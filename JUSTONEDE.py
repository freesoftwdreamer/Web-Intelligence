import pandas as pd
import requests
import logging
from bs4 import BeautifulSoup  # For parsing HTML

# Set up logging configuration
logging.basicConfig(
    filename='ecommerce_processing_DE.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def calculate_probability(url):
    # Keywords to look for with base weights
    keywords_german = {
        'Kundenerkennung': 1,
        'Individuelle Preisgestaltung': 1,
        'Sitzungsmanagement': 1,
        'cart': 3,
        'online': 3,
        'shop': 3,
        'Online-Shop': 3,
        'leistungen': 1,
        'Leinstungen': 1,
        'service': 1,
        'Service': 1,
        'Reservierung': 1,
        'versicherung': 2,
        'Mitgliedschaft': 2,
        'datenshutz': 1,
        'BUCHEN': 10,
        'Book': 4,
        'Booking': 4,
        'rentcar': 4,
        'rentacar': 4,
        'rent': 4,
        'reservation': 5,  # New keyword added
        'checkout': 5,     # New keyword added
        'payment': 5,       # New keyword added
        'Transporter': 5,
        'Fahrzeuge':5
    }

    try:
        # Send a GET request to the URL
        response = requests.get(url)
        keyword_matches = 0
        print(response.text)
        found_keywords = {keyword: False for keyword in keywords_german.keys()}
        print(found_keywords)
        # Check for e-commerce keywords and record which ones are found
        for keyword in keywords_german.keys():
            if keyword in response.text:
                found_keywords[keyword] = True

        # Analyze HTML content with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        print(soup.prettify())
        # Check for forms and buttons indicating e-commerce functionality
        forms = soup.find_all('form')
        buttons = soup.find_all('button')

        # Increase matches if certain types of forms/buttons are found
        if any('checkout' in form.get('action', '') for form in forms):
            keyword_matches += 5

        if any('buy' in button.text.lower() or 'add to cart' in button.text.lower() or 'Fahrzeuge' in button.text.lower() for button in buttons):
            keyword_matches += 5

        # Calculate base matches based on found keywords
        for keyword, weight in keywords_german.items():
            if found_keywords[keyword]:
                keyword_matches += weight
                print(f"Keyword: {keyword}, Keyword Matches: {keyword_matches}")
        total_weight = sum(keywords_german.values())
        probability = (keyword_matches / total_weight) * 100 if total_weight > 0 else 0

        logging.info(f"{url} has an estimated probability of being an e-commerce site: {probability:.2f}%")
        return {'URL': url, 'Probability (%)': probability}

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return {'URL': url, 'Probability (%)': 0}

def main():
    # Read the CSV file
    df = pd.read_csv('ecommerce_detection_results_DE.csv')
    # Filter only the ones that have field in second column = 1 (ecommerce indicator)
# Select the URL at index 1500
    #url = df.iloc[1500, 0] if len(df) > 1500 else None
    url="https://www.hertz.de/rentacar/reservation/"
    if url:
        result = calculate_probability(url)
        print(result)
    else:
        print("No URLs to process.")

if __name__ == "__main__":
    main()
