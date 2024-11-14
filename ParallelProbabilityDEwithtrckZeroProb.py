from mpi4py import MPI
import pandas as pd
import requests
import logging

# Set up logging configuration
logging.basicConfig(
    filename='ecommerce_processing.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def calculate_probability(urls):
    # Keywords to look for .. there may be english words as well
    keywords_german = [
        'Kundenerkennung', 'Individuelle Preisgestaltung', 'Sitzungsmanagement',
        'cart', 'online', 'shop', 'Online-Shop', 'leistungen',
        'Leinstungen', 'service', 'Service', 'Reservierung',
        'Barbershop', 'bookings', 'versicherung', 'Mitgliedschaft',
        'datenshutz', 'protection'
    ]

    results = []

    for url in urls:
        try:
            # Send a GET request to the URL
            response = requests.get(url)
            keyword_matches = 0

            if "Barbershop" in response.text:
                probability = 0.0  # Assign zero probability for Barbershop URLs
                logging.info(f"{url} is likely not an e-commerce site as it contains 'Barbershop'.")
            else:
                for keyword in keywords_german:
                    if keyword in response.text:
                        keyword_matches += 1  # Increment match counter

                total_keywords = len(keywords_german)
                probability = (keyword_matches / total_keywords) * 100 if total_keywords > 0 else 0
                logging.info(f"{url} has an estimated probability of being an e-commerce site: {probability:.2f}%")

            results.append({'URL': url, 'Probability (%)': probability})

        except requests.exceptions.RequestException as e:
            logging.error(f'Skipped {url} due to an error: {e}')
            results.append({'URL': url, 'Probability (%)': None})

    return results

def main():
    # Initialize MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Read the CSV file only on the root process
    if rank == 0:
        df = pd.read_csv('DEinput.csv')
        urls = df.iloc[:, 1].tolist()  # Assuming URLs are in the second column
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
    results_chunk = calculate_probability(urls_chunk)

    # Gather results from all processes
    all_results = comm.gather(results_chunk, root=0)

    if rank == 0:
        # Flatten the list of results
        final_results = [item for sublist in all_results for item in sublist]

        # Create a DataFrame from results and save to CSV
        results_df = pd.DataFrame(final_results)
        results_df.to_csv('ecommerce_probabilities_parallel.csv', index=False)

        logging.info("Probabilities have been saved to 'ecommerce_probabilities_parallel.csv'.")

if __name__ == "__main__":
    main()
