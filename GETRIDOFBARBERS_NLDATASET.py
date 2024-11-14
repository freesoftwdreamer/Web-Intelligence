from mpi4py import MPI
import pandas as pd
import requests
from tqdm import tqdm  # Import tqdm for progress bar
import logging

# Set up logging configuration
logging.basicConfig(
    filename='ecommerce_detection_NL.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def is_ecommerce_site(url):
    #try:
        # Fetch the website content
        #response = requests.get(url)
        #response.raise_for_status()  # Raise an error for bad responses

        # Extract the domain part of the URL
        domain = url.split('//')[-1].split('/')[0]

        # Check if the domain contains the string "barb"
        if "barb" in domain:
            return 0, url  # Not an e-commerce site, return URL for saving
        if "dent" in domain:
            return 0, url
        if "orthodont" in domain:
            return 0, url
        if "law" in domain:
            return 0, url

        return 1, None  # Likely an e-commerce site, no URL to save

    #except requests.exceptions.RequestException as e:
        #logging.error(f"Error fetching {url}: {e}")
        #return -99, None  # Error fetching the URL, return -99

def main():
    # Initialize MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        df = pd.read_csv('NLinput.csv')
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
    logging.info(f"Rank {rank} is gathering results.")
    all_results = comm.gather(results_chunk, root=0)
    logging.info(f"Rank {rank} is gathering non-ecommerce URLs.")
    all_non_ecommerce_urls = comm.gather(non_ecommerce_urls, root=0)
    logging.info(f"Rank {rank} is gathering original indices.")
    all_original_indices = comm.gather(original_indices, root=0)

    if rank == 0:
        # Flatten the list of results and create a DataFrame for e-commerce checks
        logging.info("Flattening results.")
        final_results = [(url, result) for sublist in all_results for url, result in sublist]

        # Ensure unique results by using a set
        logging.info("Ensuring unique results.")
        unique_results = set(final_results)

        results_df = pd.DataFrame(unique_results, columns=['URL', 'E-commerce Indicator'])

        # Save to CSV with specified structure for e-commerce checks
        logging.info("Saving e-commerce detection results.")
        results_df.to_csv('ecommerce_detection_results_NL.csv', index=False)

        # Combine all sets of non-e-commerce URLs from different processes into one set
        logging.info("Combining non-e-commerce URLs.")
        final_non_ecommerce_urls_set = set()

        for sublist in all_non_ecommerce_urls:
            final_non_ecommerce_urls_set.update(sublist)  # Add items from each process's set

        final_non_ecommerce_urls = list(final_non_ecommerce_urls_set)  # Convert back to list

        non_ecommerce_df = pd.DataFrame(final_non_ecommerce_urls, columns=['URL'])

        # Save to CSV with specified structure for non-e-commerce URLs
        logging.info("Saving non-e-commerce URLs.")
        non_ecommerce_df.to_csv('non_ecommerce_urls_nl.csv', index=False)

        # Add a new column 'E-commerce Indicator' with default values (NaN)
        logging.info("Adding E-commerce Indicator column.")
        df['E-commerce Indicator'] = float('nan')

        # Update the 'E-commerce Indicator' to 0 for non-e-commerce URLs
        logging.info("Updating E-commerce Indicator for non-e-commerce URLs.")
        df.loc[df.iloc[:, 1].isin(final_non_ecommerce_urls), 'E-commerce Indicator'] = 0

        # Update the 'E-commerce Indicator' to -99 for URLs that resulted in an error
        logging.info("Updating E-commerce Indicator for error URLs.")
        error_urls = results_df[results_df['E-commerce Indicator'] == -99]['URL'].tolist()
        df.loc[df.iloc[:, 1].isin(error_urls), 'E-commerce Indicator'] = -99

        # Save the updated DataFrame back to ATinput.csv
        #logging.info("Saving updated input.")
        #df.to_csv('NLinput.csv', index=False)

        #logging.info("Results saved to 'ecommerce_detection_results_NL.csv'.")
        logging.info("Non-e-commerce URLs saved to 'non_ecommerce_urls_nl.csv'.")
        #logging.info("Updated input saved to 'NLinput.csv'.")

if __name__ == "__main__":
    main()
