import csv
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

# Function to extract categories and keywords from a Wikipedia page
def extract_categories_and_keywords(url):
    categories_and_keywords = set()
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        catlinks = soup.find(id="mw-normal-catlinks")
        if catlinks:
            categories = catlinks.find_all('a')
            for category in categories:
                categories_and_keywords.add(category.get_text().lower())
    except Exception as e:
        print(f"Error extracting categories and keywords from page {url}: {e}")
    return categories_and_keywords

# Function to assign colors based on categories and keywords
def assign_color(categories_and_keywords):
    if 'music' in categories_and_keywords:
        return 'red'
    # Add more color assignments based on other categories or keywords here
    return 'black'  # Default color

# Read the input CSV file
input_csv_file = 'wikipedia_data.csv'
output_csv_file = 'output_data_with_color.csv'

with open(input_csv_file, 'r', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    rows = list(reader)

# Create a new CSV file with additional color column
with open(output_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['From Page', 'To Page', 'Color']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    # Use ThreadPoolExecutor to parallelize the extraction of categories and keywords
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for row in rows:
            from_page = row['From Page']
            future = executor.submit(extract_categories_and_keywords, 'https://en.wikipedia.org' + from_page)
            futures.append((row, future))

        for row, future in futures:
            categories_and_keywords = future.result()
            color = assign_color(categories_and_keywords)
            writer.writerow({'From Page': row['From Page'], 'To Page': row['To Page'], 'Color': color})

print("Color assigned and data saved to", output_csv_file)
