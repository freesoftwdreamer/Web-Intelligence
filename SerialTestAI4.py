import asyncio
import crawl4ai  # Assuming Crawl4AI is imported as crawl4ai
import csv  # For reading and writing CSV files
import re  # For regex-based searching
import logging  # For logging
import requests  # For sending HTTP requests to llama-server
import subprocess  # For running shell commands
from crawl4ai import AsyncWebCrawler
from requests.exceptions import RequestException
from tqdm import tqdm  # Import tqdm for progress bar

# Configure logging
log_filename = "script_log.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# Define translations for eCommerce terms in different languages
translations = {
    "cart": {"German": "Warenkorb", "Dutch": "winkelwagen", "Polish": "koszyk"},
    "checkout": {"German": "Kasse", "Dutch": "afrekenen", "Polish": "kasa"},
    "book": {"German": "Buch", "Dutch": "boek", "Polish": "książka"},
    "product": {"German": "Produkt", "Dutch": "product", "Polish": "produkt"},
    "shop": {"German": "Geschäft", "Dutch": "winkel", "Polish": "sklep"},
    "online shop": {"German": "Onlineshop", "Dutch": "webwinkel", "Polish": "sklep internetowy"},
    "booking": {"German": "Buchung", "Dutch": "boeking", "Polish": "rezerwacja"},
    "room": {"German": "Zimmer", "Dutch": "kamer", "Polish": "pokój"},
    "ticket": {"German": "Ticket", "Dutch": "ticket", "Polish": "bilet"},
    "insurance": {"German": "Versicherung", "Dutch": "verzekering", "Polish": "ubezpieczenie"}
}

# Load URLs from a CSV file
def load_urls_from_csv(filename):
    urls = []
    try:
        with open(filename, mode='r', newline='') as file:
            reader = csv.reader(file)
            for row in reader:
                if row and len(row) > 1:  # Ensure the row is not empty and has at least two columns
                    urls.append(row[1])  # Assuming each row has one URL in the second column
    except FileNotFoundError:
        logging.error(f"Error: The file {filename} was not found.")
    except Exception as e:
        logging.error(f"An error occurred while reading the file: {e}")
    return urls

# Analyze the output for eCommerce and social media presence with handles
def analyze_output(text):
    # Check if it's an eCommerce site by searching for keywords in multiple languages
    is_ecommerce = any(
        term in text.lower() for word in translations.keys()
        for term in [word] + [translations[word][lang].lower() for lang in translations[word]]
    )

    # Check for social media links and extract handles if available
    social_media = {
        platform: re.search(pattern, text, re.IGNORECASE)
        for platform, pattern in {
            'Twitter': r'twitter\.com/([A-Za-z0-9_]+)',
            'YouTube': r'youtube\.com/(channel|user)/([A-Za-z0-9_-]+)',
            'TikTok': r'tiktok\.com/@([A-Za-z0-9_.]+)',
            'LinkedIn': r'linkedin\.com/in/([A-Za-z0-9_-]+)'
        }.items()
    }

    # Convert results to Yes/No and extract value if found
    social_media_presence = {platform: ("Yes" if match else "No") for platform, match in social_media.items()}
    social_media_handles = {f"{platform}_value": (match.group(0) if match else "") for platform, match in social_media.items()}

    return is_ecommerce, social_media_presence, social_media_handles

# Function to clean and compress text
def clean_and_compress_text(text):
    # Remove multiple spaces and newlines
    cleaned_text = re.sub(r'\s+', ' ', text).strip()
    return cleaned_text

# Check if llama-server is running
def check_llama_server(server_url):
    try:
        response = requests.get(server_url)
        if response.status_code == 200:
            return True
    except requests.exceptions.RequestException:
        return False

# Check GPU usage
def check_gpu_usage():
    try:
        memory_used = subprocess.check_output(["nvidia-smi", "--query-gpu=memory.used", "--format=csv,noheader,nounits"]).decode("utf-8").strip().split('\n')
        gpu_utilization = subprocess.check_output(["nvidia-smi", "--query-gpu=utilization.gpu", "--format=csv,noheader,nounits"]).decode("utf-8").strip().split('\n')
        for i, (mem, util) in enumerate(zip(memory_used, gpu_utilization)):
            logging.info(f"GPU {i}: Memory used: {mem} MB | GPU Utilization: {util} %")
        return memory_used, gpu_utilization
    except subprocess.CalledProcessError as e:
        logging.error(f"Error checking GPU usage: {e}")
        return None, None

# Main processing function for each URL
async def process_url(url, server_url):
    try:
        async with AsyncWebCrawler(verbose=True) as crawler:
            crawler_output = await crawler.arun(url=url)

        # Extract relevant text from the CrawlResult object
        text_content = crawler_output.markdown

        if not text_content:
            logging.warning(f"No valid text found for URL: {url}")
            return "", False, {}, {}

        # Clean and compress the text
        compressed_text = clean_and_compress_text(text_content)

        # Analyze output
        is_ecommerce, social_media_presence, social_media_handles = analyze_output(compressed_text)

        # Prepare input for Llama.cpp
        llama_input = f"""
        Here is the content of the webpage:
        {compressed_text}

        Based on this content, please answer the following:

        1. Is this an eCommerce site? Answer with "Yes" or "No".
        2. Does this website have a Twitter account? Answer with "Yes" or "No" and provide the Twitter handle if available.
        3. Does this website have a YouTube channel? Answer with "Yes" or "No" and provide the YouTube handle if available.
        4. Does this website have a TikTok account? Answer with "Yes" or "No" and provide the TikTok handle if available.
        5. Does this website have a LinkedIn account? Answer with "Yes" or "No" and provide the LinkedIn handle if available.

        Please provide the answers in a clear and concise format.
        """

        # Send request to llama-server with retry logic
        max_retries = 5
        retry_delay = 2  # seconds
        for attempt in range(max_retries):
            try:
                response = requests.post(server_url, json={"prompt": llama_input})
                if response.status_code == 200:
                    llama_response = response.json().get("response", "").strip()
                    return llama_response, is_ecommerce, social_media_presence, social_media_handles
                else:
                    logging.warning(f"Failed to get response from llama-server for URL {url}: {response.status_code}. Retrying...")
            except RequestException as e:
                logging.warning(f"Request to llama-server failed for URL {url}: {e}. Retrying...")

            # Exponential backoff
            await asyncio.sleep(retry_delay * (2 ** attempt))

        logging.error(f"Failed to get response from llama-server for URL {url} after {max_retries} attempts.")
        return "", False, {}, {}

    except Exception as e:
        logging.error(f"An error occurred while processing URL {url}: {e}")
        return "", False, {}, {}

# Save results to CSV
def save_results_to_csv(filename, results):
    fieldnames = ["URL",
                  "eCommerce",
                  "Twitter",
                  "Twitter_value",
                  "YouTube",
                  "YouTube_value",
                  "TikTok",
                  "TikTok_value",
                  "LinkedIn",
                  "LinkedIn_value",
                  "llama_response"]

    try:
        with open(filename, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)  # Write all results at once
    except Exception as e:
        logging.error(f"An error occurred while saving results to {filename}: {e}")

# Process all URLs from CSV and save results
async def process_urls_from_csv(input_csv, output_csv, server_url, urls):
    results = []

    # Create a progress bar
    with tqdm(total=len(urls), desc="Processing URLs") as pbar:
        for url in urls:
            logging.info(f"Processing URL: {url}")
            llama_response, is_ecommerce, social_media_presence, social_media_handles = await process_url(url, server_url)

            if not llama_response:
                logging.warning(f"Skipping URL {url} due to processing error.")
                continue

            result = {
                'URL': url,
                'eCommerce': 'Yes' if is_ecommerce else 'No',
                'Twitter': social_media_presence.get('Twitter', 'No'),
                'Twitter_value': social_media_handles.get('Twitter_value', ''),
                'YouTube': social_media_presence.get('YouTube', 'No'),
                'YouTube_value': social_media_handles.get('YouTube_value', ''),
                'TikTok': social_media_presence.get('TikTok', 'No'),
                'TikTok_value': social_media_handles.get('TikTok_value', ''),
                'LinkedIn': social_media_presence.get('LinkedIn', 'No'),
                'LinkedIn_value': social_media_handles.get('LinkedIn_value', ''),
                'llama_response': llama_response,
            }

            results.append(result)
            pbar.update(1)  # Update the progress bar

    save_results_to_csv(output_csv, results)
    logging.info(f"Results saved to {output_csv}")

# Run the processing and save results
if __name__ == "__main__":
    server_url = "http://localhost:8080/completion"  # Adjust the URL as per your llama-server setup

    # Ask the user for the nation
    print("Please select the nation:")
    print("1) DE for Germany")
    print("2) AT for Austria")
    print("3) NL for Netherlands")
    print("4) PL for Poland")
    nation = input("Enter the number corresponding to the nation: ")

    if nation == "1":
        nation = "DE"
        input_csv = "NEWDEinput.csv"
        output_csv = "output_results_DE.csv"
    elif nation == "2":
        nation = "AT"
        input_csv = "NEWATinput.csv"
        output_csv = "output_results_AT.csv"
    elif nation == "3":
        nation = "NL"
        input_csv = "NEWNLinput.csv"
        output_csv = "output_results_NL.csv"
    elif nation == "4":
        nation = "PL"
        input_csv = "NEWPLinput.csv"
        output_csv = "output_results_PL.csv"
    else:
        print("Invalid selection. Exiting.")
        exit(1)

    ## Check if llama-server is running
    #if not check_llama_server(server_url):
        #print("The llama-server is not running.")
        #print("Please install and run the llama-server using the following command:")
        #print("git clone https://github.com/ggerganov/llama.cpp")
        #print("cd llama.cpp")
        #print("make")
        #print("CUDA_VISIBLE_DEVICES=0 ./server -m /path/to/your/model.gguf -ngl 64 -cnv")
        #print("Make sure to replace '/path/to/your/model.gguf' with the path to your model file.")
        #exit(1)

    # Load URLs from CSV
    urls = load_urls_from_csv(input_csv)

    # Check GPU usage before processing
    memory_used, gpu_utilization = check_gpu_usage()
    if memory_used is None:
        print("The llama-server is not running.")
        print("Please install and run the llama-server using the following command:")
        print("git clone https://github.com/ggerganov/llama.cpp")
        print("cd llama.cpp")
        print("make")
        print("CUDA_VISIBLE_DEVICES=0 ./server -m /path/to/your/model.gguf -ngl 64 -cnv")
        print("Make sure to replace '/path/to/your/model.gguf' with the path to your model file.")
        exit(1)
    else:
        logging.info(f"GPU usage before processing: Memory used: {memory_used} MB | GPU Utilization: {gpu_utilization} %")

    # Process URLs sequentially
    asyncio.run(process_urls_from_csv(input_csv, output_csv, server_url, urls))
