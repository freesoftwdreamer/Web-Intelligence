from mpi4py import MPI
import pandas as pd
import requests
import logging
from bs4 import BeautifulSoup  # For parsing HTML
from tqdm import tqdm  # Import tqdm for progress bar

# Set up logging configuration
logging.basicConfig(
    filename='ecommerce_processing_AT.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def calculate_probability(urls):
    # Keywords to look for with base weights
    keywords_german = {
        'Kundenerkennung': 1,
        'Individuelle Preisgestaltung': 1,
        'Sitzungsmanagement': 1,
        'cart': 10,
        'online': 3,
        'shop': 3,
        'Online-Shop': 10,
        'leistungen': 10,
        'Leinstungen': 10,
        'service': 1,
        'Service': 1,
        'familien':1,
        'Reservierung': 10,
        'versicherung': 10,
        'Mitgliedschaft': 10,
        'datenshutz': 1,
        'BUCHEN': 10,
        'Buchen':10,
        'Book': 10,
        'BOOK': 10,
        'Booking': 10,
        'booking':10,
        'rentcar': 4,
        'rentacar': 4,
        'rent': 4,
        'Rental': 6,
        'reservation': 5,  # New keyword added
        'checkout': 10,     # New keyword added
        'payment': 5,      # New keyword added
        'pay':5,
        'Transporter Mieten': 5,
        'Buchung':10,
        'buchung':10,
        'buchen':10,
        'BUCHEN':10,
        'DIRECTBUCHUNG':10,
        'Kostenlose Rücksendung':10,
        'been blocked by bot':10,
        'robots':5,
        'Vorteilswelt':5,
        'Zimmer':5,
        'ZIMMER':5,
        'Einkaufswagen':10,  # Shopping card
        'wellness':5,
        'suiten':5,
        'SUITEN':5,
        'ANFRAGE':5,
        'Anfrage':5,
        'delivery':10
    }

    results = []
    zero_probability_urls = []

    for url in tqdm(urls, desc="Processing URLs"):
        try:
            # Send a GET request to the URL with allow_redirects=True to follow redirects
            response = requests.get(url)
            keyword_matches = 0
            found_keywords = {keyword: False for keyword in keywords_german.keys()}

            # Check for e-commerce keywords and record which ones are found
            for keyword in keywords_german.keys():
                if keyword in response.text:
                    found_keywords[keyword] = True

            # Analyze HTML content with BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
            # Check for forms and buttons indicating e-commerce functionality
            forms = soup.find_all('form')
            buttons = soup.find_all('button')
            # Check if the URL contains the string "hotel"
            if "pension" in url:
               keyword_matches += 15
            if "hotel" in url:
               keyword_matches += 15
            if "cityhotel" in url:
               keyword_matches += 15
            # Increase matches if certain types of forms/buttons are found
            if any('checkout' in form.get('action', '') for form in forms):
                keyword_matches += 5

            if any('buy' in button.text.lower() or 'add to cart' in button.text.lower() for button in buttons):
                keyword_matches += 5

            # Calculate base matches based on found keywords
            for keyword, weight in keywords_german.items():
                if found_keywords[keyword]:
                    keyword_matches += weight

            total_weight = sum(keywords_german.values())
            probability = (keyword_matches / total_weight) * 100 if total_weight > 0 else 0

            logging.info(f"{url} has an estimated probability of being an e-commerce site: {probability:.2f}%")
            results.append({'URL': url, 'Probability (%)': probability})

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {url}: {e}")
            zero_probability_urls.append(url)

    return results, zero_probability_urls

def main():
    # Initialize MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Read the CSV file only on the root process
    if rank == 0:
        df = pd.read_csv('ecommerce_detection_results_AT.csv')
        # Filter only the ones that have field in second column = 1 (ecommerce indicator)
        df_filtered = df[df.iloc[:, 1] == 1]
        urls = df_filtered.iloc[:, 0].tolist()  # Assuming URLs are in the first column
        num_urls = len(urls)

        # Log the number of URLs processed
        logging.info(f"Total URLs to process: {num_urls}")

        # Split URLs among processes
        chunks = [urls[i::size] for i in range(size)]
    else:
        chunks = None

    # Scatter the URL chunks to all processes
    urls_chunk = comm.scatter(chunks, root=0)

    # Each process calculates probabilities for its chunk of URLs
    results_chunk, zero_probability_urls_chunk = calculate_probability(urls_chunk)

    # Gather results from all processes
    all_results = comm.gather(results_chunk, root=0)
    all_zero_probability_urls = comm.gather(zero_probability_urls_chunk, root=0)

    if rank == 0:
        # Flatten the list of results
        final_results = [item for sublist in all_results for item in sublist]
        final_zero_probability_urls = [url for sublist in all_zero_probability_urls for url in sublist]

        # Create a DataFrame from results and save to CSV
        results_df = pd.DataFrame(final_results)
        results_df.to_csv('ecommerce_probabilities_parallel_ATDATASET.csv', index=False)

        # Filter URLs with zero probability from final_results
        zero_probability_urls = [item['URL'] for item in final_results if item['Probability (%)'] == 0]

        # Save URLs with zero probability to a separate CSV
        if zero_probability_urls:
            zero_probability_df = pd.DataFrame(zero_probability_urls, columns=['URL'])
            zero_probability_df.to_csv('zero_probability_urls_ATDATASET.csv', index=False)
            logging.info("URLs with zero probability have been saved to 'zero_probability_urls_ATDATASET.csv'.")
        else:
            logging.info("No URLs with zero probability found.")

        logging.info("Probabilities have been saved to 'ecommerce_probabilities_parallel_ATDATASET.csv'.")

if __name__ == "__main__":
    main()
