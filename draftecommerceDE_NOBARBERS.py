from mpi4py import MPI
import pandas as pd
import requests
from tqdm import tqdm  # Import tqdm for progress bar
import logging

# Set up logging configuration
logging.basicConfig(
    filename='ecommerce_detection.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def check_url(url, keywords):
    try:
        # Send a GET request to the URL
        response = requests.get(url)

        # Check if the response contains the Set-Cookie header
        uses_cookies = 'Set-Cookie' in response.headers

        # Check if the response contains "rent"
        contains_rent = "rent" in response.text

        # Check for keywords in the response text
        contains_keywords = [keyword for keyword in keywords if keyword in response.text]

        return uses_cookies, contains_rent, contains_keywords

    except requests.exceptions.RequestException as e:
        logging.error(f'Skipped {url} due to an error: {e}')
        return None, None, None

def main():
    # Initialize MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        # Read the CSV file
        df = pd.read_csv('ecommerce_detection_results_DE.csv')
        name_url = 0
        # Specify the number of URLs to process
        num_urls = 2000

        # Filter the DataFrame to include only records where 'E-commerce Indicator' is 1
        df_filtered = df[df['E-commerce Indicator'] == 1]

        # Assume the URLs are in the name_url variable
        urls = df_filtered.iloc[:num_urls, name_url].tolist()

        # Create an array of indices and split it among processes
        chunks = [urls[i::size] for i in range(size)]
    else:
        chunks = None

    # Scatter the URL chunks to all processes
    urls_chunk = comm.scatter(chunks, root=0)

    # Keywords to look for
    keywords_german = [
        'cart', 'online', 'shop', 'Online-Shop', 'leistungen',
        'Leinstungen', 'service', 'Service', 'Reservierung',
        'bookings', 'versicherung', 'Mitgliedschaft','rent'
        'datenshutz'
    ]

    keywords = keywords_german
    rent_counter = 0

    # Each process checks if its URLs are e-commerce sites with a progress bar
    results_chunk = []

    for url in tqdm(urls_chunk, desc=f"Processing on rank {rank}"):
        uses_cookies, contains_rent, contains_keywords = check_url(url, keywords)

        if uses_cookies is not None:
            if uses_cookies:
                print(f'{url} uses cookies')
            else:
                print(f'{url} does not use cookies')

            if contains_rent:
                print(f"{url} is likely an e-commerce site as it contains 'rent' in its HTML please investigate for renting online option ...")
                rent_counter += 1
            else:
                for keyword in contains_keywords:
                    print(f'{url} contains the keyword "{keyword}" in the HTML.')

        results_chunk.append((url, uses_cookies, contains_rent, contains_keywords))

    # Gather results from all processes
    all_results = comm.gather(results_chunk, root=0)

    if rank == 0:
        # Flatten the list of results
        final_results = [(url, uses_cookies, contains_rent, contains_keywords) for sublist in all_results for url, uses_cookies, contains_rent, contains_keywords in sublist]

        # Print the final results
        for url, uses_cookies, contains_rent, contains_keywords in final_results:
            if uses_cookies is not None:
                if uses_cookies:
                    print(f'{url} uses cookies')
                else:
                    print(f'{url} does not use cookies')

                if contains_rent:
                    print(f"{url} is likely an e-commerce site as it contains 'rent' in its HTML please investigate for renting online option ...")
                    rent_counter += 1
                else:
                    for keyword in contains_keywords:
                        print(f'{url} contains the keyword "{keyword}" in the HTML.')

        print(f"The number of URLs containing 'rent' in their HTML is {rent_counter}.")

if __name__ == "__main__":
    main()
