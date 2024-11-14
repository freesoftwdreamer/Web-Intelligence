from mpi4py import MPI
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm  # Import tqdm for progress bar
import logging

# Set up logging configuration
logging.basicConfig(
    filename='ecommerce_detection.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def is_ecommerce_site(url):
    try:
        # Fetch the website content
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Check for keywords indicating non-e-commerce
        if any(keyword in response.text for keyword in ["Barbershop", "Notarfachangestellte", "Friseursalon"]):
            return 0, url  # Not an e-commerce site, return URL for saving

        return 1, None  # Likely an e-commerce site, no URL to save

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return None, None  # Error fetching the URL

def main():
    # Initialize MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        df = pd.read_csv('DEinput.csv')
        urls = df.iloc[:, 1].tolist()  # Assuming URLs are in the second column
        num_urls = len(urls)

        # Create an array of indices and split it among processes
        chunks = [urls[i::size] for i in range(size)]
    else:
        chunks = None

    # Scatter the URL chunks to all processes
    urls_chunk = comm.scatter(chunks, root=0)

    # Each process checks if its URLs are e-commerce sites with a progress bar
    results_chunk = []
    non_ecommerce_urls = set()  # Use a set to store non-e-commerce URLs
    original_indices = []  # List to store original indices of non-e-commerce URLs

    for i, url in enumerate(tqdm(urls_chunk, desc=f"Processing on rank {rank}")):
        result, non_ecommerce_url = is_ecommerce_site(url)

        if non_ecommerce_url:
            non_ecommerce_urls.add(non_ecommerce_url)  # Add to set of non-e-commerce URLs
            original_indices.append(i)  # Store the original index

        results_chunk.append((url, result))

    # Gather results from all processes
    all_results = comm.gather(results_chunk, root=0)
    all_non_ecommerce_urls = comm.gather(non_ecommerce_urls, root=0)
    all_original_indices = comm.gather(original_indices, root=0)

    if rank == 0:
        # Flatten the list of results and create a DataFrame for e-commerce checks
        final_results = [(url, result) for sublist in all_results for url, result in sublist]

        results_df = pd.DataFrame(final_results, columns=['URL', 'E-commerce Indicator'])

        # Save to CSV with specified structure for e-commerce checks
        results_df.to_csv('ecommerce_detection_results.csv', index=False)

        # Combine all sets of non-e-commerce URLs from different processes into one set
        final_non_ecommerce_urls_set = set()

        for sublist in all_non_ecommerce_urls:
            final_non_ecommerce_urls_set.update(sublist)  # Add items from each process's set

        final_non_ecommerce_urls = list(final_non_ecommerce_urls_set)  # Convert back to list

        non_ecommerce_df = pd.DataFrame(final_non_ecommerce_urls, columns=['URL'])

        # Save to CSV with specified structure for non-e-commerce URLs
        non_ecommerce_df.to_csv('non_ecommerce_urls.csv', index=False)
        # Please add the code to place NaN in the field ecommerce in the input File DEinput.csv as default value
        # Please put a zero  according to the list non_ecommerce_df


if __name__ == "__main__":
    main()
