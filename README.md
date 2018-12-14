# Python Scraping playground

### Setup:
1. `virtualenv -p python env`
2. `source env/bin/activate`
3. `pip install -r requirements.txt`

### Run
- `python <scraper_name>.py --chrome-driver-path=/path/to/chrome/driver/for/selenium`

Note: chrome-driver-path defaults project workspace `./chromedriver` if none passed

### Available scrapers

- **scrape-listings-lavoz.py** : will scrape 1 dorm non seasonal apartments from clasificados.lavoz.com.ar and produce a csv file containing information about each apartment and a histogram for prices distribution.