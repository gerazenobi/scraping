import argparse
import csv
import re
import threading
import time
from queue import Queue
import statistics

import numpy
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

URL_TO_SCRAPE_AGENCIES = "https://clasificados.lavoz.com.ar/inmuebles/departamentos/nueva-cordoba/1-dormitorio?ciudad=cordoba&provincia=cordoba&operacion=alquileres&cantidad-de-dormitorios[1]=1-dormitorio&tipo-de-vendedor=inmobiliaria"
URL_TO_SCRAPE_OWNERS = "https://clasificados.lavoz.com.ar/inmuebles/departamentos/nueva-cordoba/1-dormitorio?ciudad=cordoba&provincia=cordoba&operacion=alquileres&cantidad-de-dormitorios[1]=1-dormitorio&tipo-de-vendedor=particular"
browser_driver_path = None
pages_to_fetch_queue = Queue()
waiting_for_request_queue = Queue()
page_contents_queue = Queue()
apartments_list = []
csv_file_name = "1-bedroom-rents-nueva-cordoba.csv"


class PageFetcher(threading.Thread):
    def __init__(self, pages_to_fetch_queue, page_contents_queue, browser):
        super().__init__()
        self.pages_to_fetch_queue = pages_to_fetch_queue
        self.page_contents_queue = page_contents_queue
        self.browser = browser

    def run(self):
        while True:
            page = self.pages_to_fetch_queue.get()
            waiting_for_request_queue.put(page)
            self.browser.get(page)
            waiting_for_request_queue.get()
            waiting_for_request_queue.task_done()
            self.page_contents_queue.put(self.browser.page_source)
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
            apartments = soup.find_all(
                "div", attrs={"class": "col-6 flex flex-wrap content-start sm-col-3 md-col-3 align-top"}
            )
            filters = [
                e.text.replace(" ×", "").strip()
                for e in soup.find_all("a", attrs={"class": "inline-flex btn btn-outline-main m0 p03"})
            ]
            is_owner = "Particular" in filters
            for apartment in apartments:
                listing = parse_listing(apartment, is_owner)
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


def get_browser():
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--headless")
    if browser_driver_path:
        return webdriver.Chrome(chrome_options=chrome_options, executable_path=browser_driver_path)
    return webdriver.Chrome(chrome_options=chrome_options)


def scrape():
    pages = []
    start_time = time.time()

    ui_thread = UIProgress(pages_to_fetch_queue, page_contents_queue)
    ui_thread.setDaemon(True)
    ui_thread.start()

    total_agencies, _, last_page = get_total_apartments_and_page_size(URL_TO_SCRAPE_AGENCIES)
    for i in range(1, last_page + 1):
        pages.append(URL_TO_SCRAPE_AGENCIES + f"&page={str(i)}")

    total_owners, _, last_page = get_total_apartments_and_page_size(URL_TO_SCRAPE_OWNERS)
    for i in range(1, last_page + 1):
        pages.append(URL_TO_SCRAPE_OWNERS + f"&page={str(i)}")
    browsers = []

    for i in range(8):
        browser = get_browser()
        browsers.append(browser)
        fetcher = PageFetcher(pages_to_fetch_queue, page_contents_queue, browser)
        fetcher.setDaemon(True)
        fetcher.start()

    for host in pages:
        pages_to_fetch_queue.put(host)

    consumer = PageConsumer(page_contents_queue, apartments_list)
    consumer.setDaemon(True)
    consumer.start()

    pages_to_fetch_queue.join()
    page_contents_queue.join()

    for browser in browsers:
        browser.quit()

    time_elapsed = (time.time() - start_time) / 60
    print(f"finished in {time_elapsed}")
    print(f"Apartments: {len(apartments_list)}")

    print_results_and_generate_csv(apartments_list, total_agencies + total_owners, time_elapsed)


def parse_listing(apartment, is_owner):
    try:
        listing = {}
        price = apartment.find("span", attrs={"class": "price"}).text
        title = apartment.find("h2").text.lower()
        url = apartment.find("a").attrs["href"]
        if "consultar" in price or "U$S" in price:
            return None
        if re.search(
            r"inversiones|inversión|amueblado|amoblado|amoblados|venta|vendo|temporal|temporario|temporada",
            title,
        ):
            return None
        price = float(price.replace("$", "").replace(".", "").replace(",", ".").strip())
        if price < 10000 or price > 30000:  # acceptable limits ?
            # print(f"price not acceptable for listing that passed validation: {url}") # TODO: debug logging
            return None

        listing["price"] = price
        listing["is_owner"] = is_owner
        listing["url"] = url
        return listing
    except Exception as identifier:
        raise identifier


def get_total_apartments_and_page_size(url):
    """
    Returns (total_number_of_apartments, page_size)
    """
    browser = get_browser()
    browser.get(url)
    content = browser.page_source
    soup = BeautifulSoup(content, "html.parser")

    results_text = soup.find("span", attrs={"class": "h4"}).text
    results_number_regex = re.compile("[\w\s]*:\s(\d+\.?\d+)")
    total_number_of_apartments = int(results_number_regex.match(results_text).group(1).replace(".", ""))
    last_page = int(soup.find_all("a", attrs={"class": "page-link h4"})[-1].text)
    per_page = len(
        soup.find_all("div", attrs={"class": "col-6 flex flex-wrap content-start sm-col-3 md-col-3 align-top"})
    )
    browser.quit()
    return total_number_of_apartments, per_page, last_page


def print_results_and_generate_csv(results, total_number_of_apartments, elapsed_time_in_minutes):
    # TODO: refactor
    prices = [listing["price"] for listing in results]
    published_by_owner_prices = [listing["price"] for listing in results if listing["is_owner"]]
    published_by_agency_prices = [listing["price"] for listing in results if not listing["is_owner"]]
    print("\n===============================================")
    print(f"Finished scraping la voz listings in {elapsed_time_in_minutes} minutes")
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
            (listing_id,) = re.search(r"departamentos/(\d+)/", listing["url"]).groups()
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--chrome-driver-path", default=None)
    args, _ = parser.parse_known_args()
    browser_driver_path = args.chrome_driver_path
    scrape()
