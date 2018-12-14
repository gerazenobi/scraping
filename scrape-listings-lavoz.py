import argparse
import csv
import os
import re
import threading
import time
from os.path import dirname, realpath
from queue import Queue

import numpy
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

URL_TO_SCRAPE = "https://clasificados.lavoz.com.ar/search/apachesolr_search/?f[0]=im_taxonomy_vid_34:6330&f[1]=im_taxonomy_vid_34:6334&f[2]=ss_operacion:Alquileres&f[3]=ss_cantidad_dormitorios:1%20Dormitorio&f[4]=im_taxonomy_vid_5:5034&page="
PROJECT_DIR = dirname(dirname(realpath(__file__)))
DEFUALT_CHROME_DRIVER_PATH = os.path.join(PROJECT_DIR, "scraping", "chromedriver")

browser_driver_path = ""
pages_to_fetch_queue = Queue()
page_contents_queue = Queue()
apartments_list = []


class PageFetcher(threading.Thread):
    def __init__(self, pages_to_fetch_queue, page_contents_queue, browser):
        super().__init__()
        self.pages_to_fetch_queue = pages_to_fetch_queue
        self.page_contents_queue = page_contents_queue
        self.browser = browser

    def run(self):
        while True:
            page = self.pages_to_fetch_queue.get()
            self.browser.get(page)
            content = self.browser.page_source
            print(f"got results from page: {page}")
            self.page_contents_queue.put(content)
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
            regex_for_listing = re.compile("BoxResultado Borde Espacio")
            apartments = soup.find_all("div", attrs={"class": regex_for_listing})
            for apartment in apartments:
                listing = process_listing(apartment)
                if listing:
                    self.apartments_list.append(listing)
            print("done processing page")
            self.pages_contents_queue.task_done()


def get_browser():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1024x1400")
    return webdriver.Chrome(chrome_options=chrome_options, executable_path=browser_driver_path)


def scrape():
    pages = []
    start_time = time.time()
    total, page_size = get_total_apartments_and_page_size()
    for i in range(round(total / page_size)):
        pages.append(URL_TO_SCRAPE + str(i))
    browsers = []

    for i in range(8):
        browser = get_browser()
        browsers.append(browser)
        t = PageFetcher(pages_to_fetch_queue, page_contents_queue, browser)
        t.setDaemon(True)
        t.start()

    for host in pages:
        pages_to_fetch_queue.put(host)

    for i in range(2):
        dt = PageConsumer(page_contents_queue, apartments_list)
        dt.setDaemon(True)
        dt.start()

    pages_to_fetch_queue.join()
    page_contents_queue.join()

    for browser in browsers:
        browser.quit()

    time_elapsed = (time.time() - start_time) / 60
    print(f"finished in {time_elapsed}")
    print(f"Apartments: {len(apartments_list)}")

    print_results_and_generate_csv(apartments_list, total, time_elapsed)


def process_listing(apartment):
    try:
        listing = {}
        is_owner = "particular" in apartment.find("div", attrs={"class": "avatar"}).text.lower()
        price = apartment.find("div", attrs={"class": "cifra"}).text
        description = apartment.find("div", attrs={"class": "Descripcion"}).text.lower()
        title = apartment.find("h4").text.lower()
        sub_title = apartment.find("h2") or apartment.find("h3") or apartment.find("h5")
        sub_title = sub_title.text.lower()
        url = apartment.find("a").attrs["href"]
        titles_and_desc = sub_title + " " + title + " " + description
        if "consultar" in price or "U$S" in price:
            return None
        if re.search(
            r"inversiones|inversión|amueblado|amoblado|amoblados|venta|vendo|temporal|temporario|temporada",
            titles_and_desc,
        ):
            return None
        price = float(price.replace("$", "").replace(".", "").replace(",", ".").strip())
        if price < 5000 or price > 30000:  # acceptable limits ?
            print(f"price not acceptable for listing that passed validation: {url}")
            return None

        listing["price"] = price
        listing["is_owner"] = is_owner
        listing["url"] = url
        return listing
    except Exception as identifier:
        raise identifier


def get_total_apartments_and_page_size():
    """
    Returns (total_number_of_aparments, page_size)
    """
    browser = get_browser()
    browser.get(URL_TO_SCRAPE + str(0))
    content = browser.page_source
    soup = BeautifulSoup(content, "html.parser")
    results_text = soup.find("p", attrs={"class": "cantidadResultados"}).text
    results_number_regex = re.compile(".+\((\d+) .+\)")
    total_number_of_apartments = int(results_number_regex.match(results_text).group(1))
    regex_for_listing = re.compile("BoxResultado Borde Espacio")
    apartments = soup.find_all("div", attrs={"class": regex_for_listing})
    browser.quit()
    return total_number_of_apartments, len(apartments)


def print_results_and_generate_csv(results, total_number_of_apartments, elapsed_time_in_minutes):
    prices = [listing["price"] for listing in results]
    published_by_owner_prices = [listing["price"] for listing in results if listing["is_owner"]]
    published_by_agency_prices = [listing["price"] for listing in results if not listing["is_owner"]]
    print("\n===============================================")
    print(f"Finished scraping clasificados la voz in {elapsed_time_in_minutes} minutes")
    print(f"Total scraped apartments: {total_number_of_apartments}")
    print(f"Relevant apartments processed: {len(results)}")
    print(f"Ignored apartments: {total_number_of_apartments - len(results)}")
    print("===============================================")
    print("All apartments:")
    print(f"Total: {len(prices)}")
    print(f"Max price: {max(prices)}")
    print(f"Min price: {min(prices)}")
    print(f"AVG price: {sum(prices) / len(prices)}")
    print("===============================================")
    print("Owners:")
    print(f"Total: {len(published_by_owner_prices)}")
    print(f"Max price: {max(published_by_owner_prices)}")
    print(f"Min price: {min(published_by_owner_prices)}")
    print(f"AVG price: {sum(published_by_owner_prices) / len(published_by_owner_prices)}")
    print("===============================================")
    print("Agencies:")
    print(f"Total: {len(published_by_agency_prices)}")
    print(f"Max price: {max(published_by_agency_prices)}")
    print(f"Min price: {min(published_by_agency_prices)}")
    print(f"AVG price: {sum(published_by_agency_prices) / len(published_by_agency_prices)}")
    print("===============================================")

    print("\nExporting to CSV... ")
    with open("1-bedroom-rents-nueva-cordoba.csv", "w") as csvfile:
        fieldnames = ["id", "price", "published_by_owner", "url"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for listing in results:
            listing_id, = re.search(r"departamentos/(\d+)/", listing["url"]).groups()
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
    with open("1-bedroom-rents-nueva-cordoba.csv", "a") as csvfile:
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
    parser.add_argument("--chrome-driver-path", default=DEFUALT_CHROME_DRIVER_PATH)
    args, _ = parser.parse_known_args()
    browser_driver_path = args.chrome_driver_path
    scrape()
