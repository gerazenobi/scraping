import csv
import math
import re
import threading
import time
from queue import Queue
import statistics

import numpy
import requests
from bs4 import BeautifulSoup

AGENCIES_URL_TO_SCRAPE = "https://inmuebles.mercadolibre.com.ar/departamentos/alquiler/1-dormitorio/cordoba/cordoba/inmobiliaria/nueva-cordoba/_Desde_"
OWNERS_URL_TO_SCRAPE = "https://inmuebles.mercadolibre.com.ar/departamentos/alquiler/1-dormitorio/cordoba/cordoba/dueno-directo/nueva-cordoba/_Desde_"
pages_to_fetch_queue = Queue()
waiting_for_request_queue = Queue()
page_contents_queue = Queue()
apartments_list = []
csv_file_name = "1-bedroom-rents-nueva-cordoba-mercado-libre.csv"


class PageFetcher(threading.Thread):
    def __init__(self, pages_to_fetch_queue, page_contents_queue, session):
        super().__init__()
        self.pages_to_fetch_queue = pages_to_fetch_queue
        self.page_contents_queue = page_contents_queue
        self.session = session

    def run(self):
        while True:
            page = self.pages_to_fetch_queue.get()
            waiting_for_request_queue.put(page)
            response = self.session.get(page)
            waiting_for_request_queue.get()
            waiting_for_request_queue.task_done()
            self.page_contents_queue.put(response.text)
            self.pages_to_fetch_queue.task_done()


class PageConsumer(threading.Thread):
    def __init__(self, pages_contents_queue, apartments_list):
        super().__init__()
        self.pages_contents_queue = pages_contents_queue
        self.apartments_list = apartments_list

    def run(self):
        while True:
            content = self.pages_contents_queue.get()
            soup = BeautifulSoup(content, "html.parser")
            apartments = soup.find_all("li", attrs={"class": "results-item highlighted article grid"})
            for apartment in apartments:
                listing = parse_listing(apartment, is_owner="dueño directo" in soup.title.text)
                if listing:
                    self.apartments_list.append(listing)
            self.pages_contents_queue.task_done()


class UIProgress(threading.Thread):
    def __init__(self, pages_to_fetch_queue, page_contents_queue):
        super().__init__()
        self.pages_to_fetch_queue = pages_to_fetch_queue
        self.page_contents_queue = page_contents_queue

    def run(self):
        while True:
            print(
                "\r"
                + f"Pages to fetch: [{self.pages_to_fetch_queue.qsize()}] - "
                + f"Content to process: [{self.page_contents_queue.qsize()}] - "
                + f"Pending requests: [{waiting_for_request_queue.qsize()}]",
                end="",
            )
            time.sleep(0.1)


def scrape():
    pages = []
    start_time = time.time()

    ui_thread = UIProgress(pages_to_fetch_queue, page_contents_queue)
    ui_thread.setDaemon(True)
    ui_thread.start()

    total_for_agencies, page_size_for_agencies = get_total_apartments_and_page_size(AGENCIES_URL_TO_SCRAPE)
    total_for_owners, page_size_for_owners = get_total_apartments_and_page_size(OWNERS_URL_TO_SCRAPE)
    increment = 1
    number_of_pages_agencies = math.ceil(total_for_agencies / page_size_for_agencies)
    for _ in range(number_of_pages_agencies):
        pages.append(AGENCIES_URL_TO_SCRAPE + str(increment))
        increment += page_size_for_agencies

    number_of_owners = math.ceil(total_for_owners / page_size_for_owners)
    increment = 1
    for _ in range(number_of_owners):
        pages.append(OWNERS_URL_TO_SCRAPE + str(increment))
        increment += page_size_for_agencies
    sessions = []

    for i in range(5):
        session = requests.session()
        sessions.append(session)
        fetcher = PageFetcher(pages_to_fetch_queue, page_contents_queue, session)
        fetcher.setDaemon(True)
        fetcher.start()

    for url in pages:
        pages_to_fetch_queue.put(url)

    consumer = PageConsumer(page_contents_queue, apartments_list)
    consumer.setDaemon(True)
    consumer.start()

    pages_to_fetch_queue.join()
    page_contents_queue.join()

    for session in sessions:
        session.close()

    time_elapsed = (time.time() - start_time) / 60
    print(f"Finished in {time_elapsed}")
    print(f"Apartments: {len(apartments_list)}")

    print_results_and_generate_csv(apartments_list, total_for_agencies + total_for_owners, time_elapsed)


def parse_listing(apartment, is_owner):
    try:
        listing = {}
        price = int(apartment.find("span", attrs={"class": "price__fraction"}).text.replace(".", ""))
        url = apartment.find("a").attrs["href"]
        if price < 5000 or price > 30000:  # acceptable limits ?
            print(f"price not acceptable for listing that passed validation: {url}")  # TODO: debug logging
            return None
        listing["title"] = apartment.find("div", attrs={"class": "item__title"}).text
        listing["price"] = price
        listing["is_owner"] = is_owner
        listing["url"] = url
        return listing
    except Exception as identifier:
        raise identifier


def get_total_apartments_and_page_size(url):
    """
    Returns (total_number_of_aparments, page_size)
    """
    page = requests.get(url + "1").text
    soup = BeautifulSoup(page, "html.parser")
    results_text = soup.find("div", attrs={"class": "quantity-results"}).text
    results_number_regex = re.compile(" (\d+) .+")
    total_number_of_apartments = int(results_number_regex.match(results_text).group(1))
    apartments = soup.find_all("li", attrs={"class": "results-item highlighted article grid"})
    return total_number_of_apartments, len(apartments)


def print_results_and_generate_csv(results, total_number_of_apartments, elapsed_time_in_minutes):
    # TODO: refactor
    prices = [listing["price"] for listing in results]
    published_by_owner_prices = [listing["price"] for listing in results if listing["is_owner"]]
    published_by_agency_prices = [listing["price"] for listing in results if not listing["is_owner"]]
    print("\n===============================================")
    print(f"Finished scraping mercadolibre listings {elapsed_time_in_minutes} minutes")
    print(f"Total scraped apartments: {total_number_of_apartments}")
    print(f"Relevant apartments processed: {len(results)}")
    print(f"Ignored apartments: {total_number_of_apartments - len(results)}")
    print("===============================================")
    print("All apartments:")
    print(f"Total: {len(prices)}")
    print(f"Max price: {max(prices)}")
    print(f"Min price: {min(prices)}")
    print(f"AVG price: {sum(prices) / len(prices)}")
    print(f"MEDIAN price: {statistics.median(prices)}")

    print("===============================================")
    print("Owners:")
    print(f"Total: {len(published_by_owner_prices)}")
    print(f"Max price: {max(published_by_owner_prices)}")
    print(f"Min price: {min(published_by_owner_prices)}")
    print(f"AVG price: {sum(published_by_owner_prices) / len(published_by_owner_prices)}")
    print(f"MEDIAN price: {statistics.median(published_by_owner_prices)}")
    print("===============================================")
    print("Agencies:")
    print(f"Total: {len(published_by_agency_prices)}")
    print(f"Max price: {max(published_by_agency_prices)}")
    print(f"Min price: {min(published_by_agency_prices)}")
    print(f"AVG price: {sum(published_by_agency_prices) / len(published_by_agency_prices)}")
    print(f"MEDIAN price: {statistics.median(published_by_agency_prices)}")
    print("===============================================")

    print("\nExporting to CSV... ")
    with open(csv_file_name, "w") as csvfile:
        fieldnames = ["id", "price", "published_by_owner", "url"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for listing in results:
            listing_id = listing["title"]
            writer.writerow(
                {
                    "id": listing_id,
                    "price": listing["price"],
                    "published_by_owner": "true" if listing["is_owner"] else "false",
                    "url": listing["url"],
                }
            )
    # calculate histogram values to see distribution of prices
    last_bucket = round(max(prices) / 1000) * 1000
    bins = [value for value in range(0, last_bucket + 1000, 1000)]
    values, buckets = numpy.histogram(prices, bins=bins)
    with open(csv_file_name, "a") as csvfile:
        fieldnames = ["alquiler", "departamentos"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        print("\nPrices distribution:")
        print("========================")
        for index, value in enumerate(values):
            writer.writerow({"alquiler": str(buckets[index]) + " - " + str(buckets[index + 1]), "departamentos": value})
            print(f"{str(buckets[index])} - {str(buckets[index + 1])} => {str(value)}")
    print("========================\n")


if __name__ == "__main__":
    scrape()
