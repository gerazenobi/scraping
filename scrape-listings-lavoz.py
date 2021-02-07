import asyncio
import csv
import math
import re
import statistics
import time
import aiohttp


import numpy
from bs4 import BeautifulSoup
from prettytable import PrettyTable

URL_TO_SCRAPE_AGENCIES = "https://clasificados.lavoz.com.ar/inmuebles/departamentos/nueva-cordoba/1-dormitorio?ciudad=cordoba&provincia=cordoba&operacion=alquileres&cantidad-de-dormitorios[1]=1-dormitorio&tipo-de-vendedor=inmobiliaria"
URL_TO_SCRAPE_OWNERS = "https://clasificados.lavoz.com.ar/inmuebles/departamentos/nueva-cordoba/1-dormitorio?ciudad=cordoba&provincia=cordoba&operacion=alquileres&cantidad-de-dormitorios[1]=1-dormitorio&tipo-de-vendedor=particular"
CSV_FILE_NAME = "1-bedroom-rents-nueva-cordoba.csv"
MIN_PRICE_TO_CONSIDER = 12000
MAX_PRICE_TO_CONSIDER = 30000
apartment_listings = []


async def consumer(name: str, queue: asyncio.Queue, handler: callable):
    while True:
        content = await queue.get()
        print(f"{name}::queue::got item")
        try:
            handler(content)
            print(f"{name}::item processed")
        except Exception as e:
            print(f"{name}:: {e}")
        finally:
            queue.task_done()
        await asyncio.sleep(0.1)


async def html_fetcher(name: str, session: aiohttp.ClientSession, url: str, queue: asyncio.Queue):
    async with session.get(url) as response:
        html_as_string = await response.text()
        await queue.put(html_as_string)
        print(f"[{name}] fetch done")


async def get_pages_and_total_items_expected(session: aiohttp.ClientSession):
    pages = []
    initial_tasks = [
        _get_total_apartments_and_page_size(session, URL_TO_SCRAPE_AGENCIES),
        _get_total_apartments_and_page_size(session, URL_TO_SCRAPE_OWNERS),
    ]
    seed_results = await asyncio.gather(*initial_tasks, return_exceptions=True)
    total_agencies, last_page = seed_results[0]
    for i in range(1, last_page + 1):
        pages.append(URL_TO_SCRAPE_AGENCIES + f"&page={str(i)}")
    total_owners, last_page = seed_results[1]
    for i in range(1, last_page + 1):
        pages.append(URL_TO_SCRAPE_OWNERS + f"&page={str(i)}")
    return pages, total_agencies + total_owners


async def main():
    start_time = time.time()
    async with aiohttp.ClientSession() as session:
        print("getting pages to scrape")
        pages, total_items_to_parse = await get_pages_and_total_items_expected(session)

        content_queue = asyncio.Queue()
        consumers = [
            asyncio.create_task(consumer(name=str(i), queue=content_queue, handler=parse_page_content))
            for i in range(1)
        ]
        print(f"{len(consumers)} page content consumer/s started")

        tasks = []
        for i, url in enumerate(pages):
            tasks.append(html_fetcher(str(i), session, url, content_queue))
        print(f"starting {len(tasks)} html fetchers")
        await asyncio.gather(*tasks, return_exceptions=True)
        print("fetching content")
    await content_queue.join()
    for c in consumers:
        c.cancel()
    print(f"finishing processing {len(pages)} pages")
    time_elapsed = (time.time() - start_time) / 60
    print(f"scraping finished in {round(time_elapsed,2)} minutes")
    print_results_and_generate_csv(total_items_to_parse, apartment_listings)


def parse_page_content(content: str):
    soup = BeautifulSoup(content, "html.parser")
    apartments = soup.find_all("div", attrs={"class": "col-6 flex flex-wrap content-start sm-col-3 md-col-3 align-top"})
    filters = [
        e.text.replace(" ×", "").strip()
        for e in soup.find_all("a", attrs={"class": "inline-flex btn btn-outline-main m0 p03"})
    ]
    is_owner = "Particular" in filters
    listings = [parse_listing(apartment, is_owner) for apartment in apartments]
    apartment_listings.extend([listing for listing in listings if listing is not None])


def parse_listing(apartment: dict, is_owner: bool):
    try:
        listing = {}
        price = apartment.find("span", attrs={"class": "price"}).text
        title = apartment.find("h2").text.lower()
        url = apartment.find("a").attrs["href"]
        if "consultar" in price or "U$S" in price:
            print(f"not valid: {url}")
            return None
        if re.search(
            r"inversiones|inversión|amueblado|amoblado|amoblados|venta|vendo|temporal|temporario|temporada",
            title,
        ):
            print(f"not valid: {url}")
            return None
        price = float(price.replace("$", "").replace(".", "").replace(",", ".").strip())
        if price < MIN_PRICE_TO_CONSIDER or price > MAX_PRICE_TO_CONSIDER:
            print(f"price not valid: {url}")
            return None
        listing["price"] = price
        listing["is_owner"] = is_owner
        listing["url"] = url
        return listing
    except Exception as identifier:
        print(str(identifier))
        raise identifier


async def _get_total_apartments_and_page_size(session: aiohttp.ClientSession, url: str):
    """
    Returns (total_number_of_apartments, last_page)
    """
    async with session.get(url) as response:
        content = await response.text()
    soup = BeautifulSoup(content, "html.parser")
    results_text = soup.find("span", attrs={"class": "h4"}).text
    results_number_regex = re.compile("[\w\s]*:\s(\d+\.?\d+)")
    total_number_of_apartments = int(results_number_regex.match(results_text).group(1).replace(".", ""))
    last_page = int(soup.find_all("a", attrs={"class": "page-link h4"})[-1].text)
    return total_number_of_apartments, last_page


def print_results_and_generate_csv(total_parsed: int, results: list):
    # TODO: refactor
    prices = [listing["price"] for listing in results]
    published_by_owner_prices = [listing["price"] for listing in results if listing["is_owner"]]
    published_by_agency_prices = [listing["price"] for listing in results if not listing["is_owner"]]

    print("\n===============================================")
    print(f"Total scraped apartments: {total_parsed}")
    print(f"Ignored apartments: {total_parsed - len(results)}")

    stats_table = PrettyTable()
    stats_table.field_names = ["stat", "value"]
    stats_table.align["value"] = "r"
    stats_table.add_row(["all apartments", len(prices)])
    stats_table.add_row(["max price", max(prices)])
    stats_table.add_row(["min price", min(prices)])
    stats_table.add_row(["avg price", round(sum(prices) / len(prices))])
    stats_table.add_row(["median price", statistics.median(prices)])
    stats_table.add_row([" - - - ", " - - - "])

    stats_table.add_row(["owners", len(published_by_owner_prices)])
    stats_table.add_row(["max price", max(published_by_owner_prices)])
    stats_table.add_row(["min price", min(published_by_owner_prices)])
    stats_table.add_row(["avg price", round(sum(published_by_owner_prices) / len(published_by_owner_prices))])
    stats_table.add_row(["median price", statistics.median(published_by_owner_prices)])
    stats_table.add_row([" - - - ", " - - - "])

    stats_table.add_row(["agencies", len(published_by_agency_prices)])
    stats_table.add_row(["max price", max(published_by_agency_prices)])
    stats_table.add_row(["min price", min(published_by_agency_prices)])
    stats_table.add_row(["avg price", round(sum(published_by_agency_prices) / len(published_by_agency_prices))])
    stats_table.add_row(["median price", statistics.median(published_by_agency_prices)])

    print(stats_table)

    print("\nExporting to CSV... ")
    with open(CSV_FILE_NAME, "w") as csvfile:
        fieldnames = ["id", "price", "published_by_owner", "url"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for listing in results:
            try:
                listing_id = int(re.search(r"departamentos/(\d+)/?", listing["url"]).groups()[0])
            except Exception as e:
                print(f"error parsing listing id: {listing}, {e}")
                listing_id = None
            writer.writerow(
                {
                    "id": listing_id,
                    "price": listing["price"],
                    "published_by_owner": "true" if listing["is_owner"] else "false",
                    "url": listing["url"],
                }
            )
    # calculate histogram values to see distribution of prices
    first_bucket = math.floor(min(prices) / 1000) * 1000
    last_bucket = math.ceil(max(prices) / 1000) * 1000
    bins = [value for value in range(first_bucket, last_bucket + 1000, 1000)]
    values, buckets = numpy.histogram(prices, bins=bins)
    distribution_table = PrettyTable()
    distribution_table.field_names = ["rent", "apartments"]
    with open(CSV_FILE_NAME, "a") as csvfile:
        fieldnames = ["rent", "apartments"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        print("\nPrices distribution:")
        for index, value in enumerate(values):
            writer.writerow({"rent": str(buckets[index]) + " - " + str(buckets[index + 1]), "apartments": value})
            distribution_table.add_row([f"{str(buckets[index])} - {str(buckets[index + 1])}", value])
    print(distribution_table)


if __name__ == "__main__":
    asyncio.run(main())
