# Python Scraping playground

### Setup:
1. `virtualenv -p python env`
2. `source env/bin/activate`
3. `pip install -r requirements.txt`

### Run
- `python <scraper_name>.py --chrome-driver-path=/path/to/driver`

Note: the scraper uses `webdriver.Chrome` in order to overcome getting bloqued by certain websites; that said you are welcome to change this and use your preferred browser and corresponding driver.
```python
from selenium.webdriver.chrome.options import Options

def get_browser():
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument("--headless")
    if browser_driver_path:
        return webdriver.Chrome(chrome_options=chrome_options, executable_path=browser_driver_path)
    return webdriver.Chrome(chrome_options=chrome_options)
```

If no driver path is provided it assumes the path to find the driver is present in `PATH`

### Available scrapers

- **scrape-listings-lavoz.py** : will scrape 1 dorm non seasonal apartments from clasificados.lavoz.com.ar and produce a csv file containing information about each apartment and a histogram for prices distribution.

### Running scrapers with docker
Alternatively you can choose to run the scraper in a ready to use image provided.

#### Build image and run scraper container:
`docker-compose build`

`docker-compose up -d`

Then inside the container ( `docker attach scraping_scraper_1` ) you can run a scraper by:

`python <scraper_name>.py`

The project directory is mounted as a volume for ease of use. Any files created by a scraper should be made visible to the host immediately.

