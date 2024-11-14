import requests
from bs4 import BeautifulSoup
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

        # Check for e-commerce keywords in the text (including German keywords)
        keywords = [
            'cart', 'checkout', 'buy', 'order', 'payment',
            'Einloggen', 'Reservierung', 'Buchung','buchung', 'Buchen',
            'Warenkorb', 'Kaufen', 'Bestellen', 'Zahlung','reservation','rentacar','robots'
        ]

        page_text = soup.get_text().lower()
        print(page_text)
        if any(keyword.lower() in page_text for keyword in keywords):
            logging.info(f"{url} likely has e-commerce functionality due to keywords.")
            return True

        # Check for forms that may indicate e-commerce functionality
        forms = soup.find_all('form')
        if any('checkout' in form.get('action', '').lower() for form in forms):
            logging.info(f"{url} has a checkout form.")
            return True

        # Check for buttons indicating purchasing options
        buttons = soup.find_all('button')
        if any('buy' in button.text.lower() or
               'add to cart' in button.text.lower() or
               'kaufen' in button.text.lower() or
               'in den warenkorb' in button.text.lower() for button in buttons):
            logging.info(f"{url} has buttons indicating purchasing options.")
            return True

        # Check footer for e-commerce platform indicators
        footer = soup.find('footer')
        if footer and ('powered by' in footer.text.lower() or
                       'shop' in footer.text.lower() or
                       'e-commerce' in footer.text.lower()):
            logging.info(f"{url} footer indicates it may be powered by an e-commerce platform.")
            return True

        logging.info(f"{url} does not appear to be an e-commerce site.")
        return False

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return False

if __name__ == "__main__":
    url_to_check = "https://www.bohotel.com/"
    #url_to_check = "https://www.notebooksbilliger.de/"
    if is_ecommerce_site(url_to_check):
        print(f"{url_to_check} is likely an e-commerce site.")
    else:
        print(f"{url_to_check} is not an e-commerce site.")
