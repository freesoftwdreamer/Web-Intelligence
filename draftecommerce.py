import pandas as pd
import requests

# Read the CSV file
df = pd.read_csv('input.csv')
name_url = 1
name_country =
# Specify the number of URLs to print
num_urls = 15

# Assume the URLs are in the name_url variable
urls = df.iloc[:num_urls, name_url]

# Keywords to look for
keywords_german = ['Kundenerkennung', 'Individuelle Preisgestaltung', 'Sitzungsmanagement','cart','online','shop','Online-Shop','leistungen', 'Leinstungen','service','Service','Reservierung','Barbershop','bookings','versicherung','Mitgliedschaft','datenshutz']
keywords_english = ['customer recognition', 'individual pricing', 'session management','card','online','shop', 'Online-Shop','service','Service','Barbershop', 'bookings','insurance','Membership','Data protection']
barbershop_counter =0;
for url in urls:
    # Send a GET request to the URL
    response = requests.get(url)


for url in urls:
    # Send a GET request to the URL
    response = requests.get(url)
    # Check if the URL contains '.de'
    if url.find('.de') != -1:
        keywords = keywords_german
    else:
        keywords = keywords_english

    # Check if the response contains the Set-Cookie header
    if 'Set-Cookie' in response.headers:
        print(f'{url} uses cookies')
    else:
        print(f'{url} does not use cookies')
    # Check if the response contains "Barbershop"
    if "Barbershop" in response.text:
        print(f"{url} is likely not an e-commerce site as it contains 'Barbershop' in its HTML.")
        barbershop_counter += 1
    else:
        for keyword in keywords:
            if keyword in response.text:
                print(f'{url} contains the keyword "{keyword}" in the HTML.')




print(f"The number of URLs containing 'Barbershop' in their HTML is {barbershop_counter}.")
