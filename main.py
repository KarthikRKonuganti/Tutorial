import logging
import json
import pandas as pd
import geopy
import xmltodict
import time
import boto3


from bs4 import BeautifulSoup
from datetime import datetime
from urllib.request import urlopen, Request
from seleniumwire.utils import decode as sw_decode
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from seleniumwire import webdriver


def timenow():
    return datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S")


# Example: Use selenium for clicking a button
def scraper1(url, driver):
    def fetch():
        print(f"fetching outages from {url}")

        driver.get(url)
        time.sleep(10)

        button = driver.find_elements("xpath", '//*[@id="OMS.Customers Summary"]')

        if button:
            wait = WebDriverWait(driver, 10)
            label = wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, '//*[@id="OMS.Customers Summary"]')
                )
            )
            label.click()
            time.sleep(5)
            page_source = {}
            select_elements = driver.find_elements(By.CLASS_NAME, "gwt-ListBox")
            menu = Select(select_elements[0])
            for idx, option in enumerate(menu.options):
                level = option.text
                menu.select_by_index(idx)
                time.sleep(3)
                page_source.update({f"per_{level}": driver.page_source})
        return page_source

    def parse():
        data = fetch()
        for level, pg in data.items():
            df = _parse(pg)
            data.update({level: df})
        return data

    def _parse(page_source):
        soup = BeautifulSoup(page_source, "html.parser")
        tables = soup.find_all("table")
        # separate rows
        rows = tables[1].find_all("tr")
        header_row = rows[0]
        data_rows = rows[1:]

        # Extract the table header cells
        header_cells = header_row.find_all("th")
        header = [cell.get_text().strip() for cell in header_cells]
        cols = [h for h in header if h != ""]

        # Extract the table data cells
        data = []
        for row in data_rows:
            cells = row.find_all("td")
            data.append([cell.get_text().strip() for cell in cells])

        # Print the table data as a list of dictionaries
        table = [dict(zip(header, row)) for row in data]
        df = pd.DataFrame(table)
        if len(df.columns) > 1:
            df = df[cols]
            df = df.dropna(axis=0)
            df["timestamp"] = timenow()
            # df = df[df["# Out"] != "0"]
        else:
            df = pd.DataFrame()
        # print("Storing info.csv ...")
        # df.to_csv("info.csv")
        return df

    return parse()


# TODO: Implement your scraper function here
# Input: url to scrape, chromedriver
# Output: A dictionary of dataframe, Ex: {"per_county": <pandas dataframe>, "per_zipcode": <pandas dataframe>, ...}
# Scraper 1 is an example
def scraper(url, driver):
    """
    General-purpose outage map scraper.

    Attempts multiple strategies to extract outage data:
      1. Intercept XHR/JSON network requests made by the page
      2. Parse HTML tables directly from the page source
      3. Look for dropdown menus to iterate over granularity levels
         (e.g., per County, per ZIP code, per City)

    Input:
        url    (str)            : URL of the outage map page
        driver (seleniumwire)   : Configured Selenium Wire Chrome driver

    Output:
        dict[str, pd.DataFrame]: Keys like "per_county", "per_zipcode", etc.
                                 Each value is a DataFrame with outage data
                                 and a "timestamp" column.
    """

    results = {}

    # ------------------------------------------------------------------ #
    # Helper: build a DataFrame from a list-of-dicts, adding a timestamp  #
    # ------------------------------------------------------------------ #
    def _to_df(records: list) -> pd.DataFrame:
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records)
        df.dropna(how="all", inplace=True)
        df["timestamp"] = timenow()
        return df

    # ------------------------------------------------------------------ #
    # Helper: parse every <table> on the current page source               #
    # Returns a dict  {"table_0": df, "table_1": df, ...}                 #
    # ------------------------------------------------------------------ #
    def _parse_html_tables(page_source: str) -> dict:
        soup = BeautifulSoup(page_source, "html.parser")
        tables = soup.find_all("table")
        table_data = {}
        for t_idx, table in enumerate(tables):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            # Header
            header_cells = rows[0].find_all(["th", "td"])
            header = [c.get_text(strip=True) for c in header_cells]
            header = [h if h else f"col_{i}" for i, h in enumerate(header)]

            # Data rows
            records = []
            for row in rows[1:]:
                cells = row.find_all("td")
                values = [c.get_text(strip=True) for c in cells]
                if len(values) == len(header):
                    records.append(dict(zip(header, values)))

            df = _to_df(records)
            if not df.empty:
                table_data[f"table_{t_idx}"] = df

        return table_data

    # ------------------------------------------------------------------ #
    # Helper: intercept JSON payloads from network requests               #
    # ------------------------------------------------------------------ #
    def _intercept_json_requests() -> dict:
        json_data = {}
        for request in driver.requests:
            if request.response is None:
                continue
            content_type = request.response.headers.get("Content-Type", "")
            if "json" not in content_type:
                continue
            try:
                body = sw_decode(
                    request.response.body,
                    request.response.headers.get("Content-Encoding", "identity"),
                )
                payload = json.loads(body)

                # Flatten top-level lists or dicts that look like outage records
                if isinstance(payload, list) and payload:
                    label = request.url.split("/")[-1].split("?")[0] or "json_list"
                    df = _to_df(payload)
                    if not df.empty:
                        json_data[f"per_{label}"] = df

                elif isinstance(payload, dict):
                    for key, val in payload.items():
                        if isinstance(val, list) and val:
                            df = _to_df(val)
                            if not df.empty:
                                json_data[f"per_{key}"] = df

            except Exception as exc:
                logging.debug("Could not decode request %s: %s", request.url, exc)

        return json_data

    # ------------------------------------------------------------------ #
    # Helper: iterate a <select> dropdown and scrape each option           #
    # ------------------------------------------------------------------ #
    def _scrape_dropdown_levels(select_elements) -> dict:
        dropdown_data = {}
        for sel_el in select_elements:
            try:
                menu = Select(sel_el)
                options = menu.options
                if len(options) < 2:
                    continue
                for idx, option in enumerate(options):
                    level = option.text.strip().replace(" ", "_").lower()
                    if not level:
                        level = f"level_{idx}"
                    menu.select_by_index(idx)
                    time.sleep(3)
                    tables = _parse_html_tables(driver.page_source)
                    if tables:
                        # Use the largest table found at this level
                        best = max(tables.values(), key=lambda d: len(d))
                        dropdown_data[f"per_{level}"] = best
            except Exception as exc:
                logging.warning("Error iterating dropdown: %s", exc)
        return dropdown_data

    # ================================================================== #
    # MAIN SCRAPE FLOW                                                     #
    # ================================================================== #
    print(f"[scraper] Navigating to {url}")
    driver.get(url)
    time.sleep(10)  # Allow JavaScript / map to fully load

    # --- Strategy 1: capture XHR/JSON network traffic -------------------
    json_results = _intercept_json_requests()
    if json_results:

        print(f"[scraper] Captured {len(json_results)} JSON dataset(s) from network.")
        results.update(json_results)

    # --- Strategy 2: look for a summary/table panel to click ------------
    summary_xpaths = [
        '//*[contains(@id, "Summary")]',
        '//*[contains(@class, "summary")]',
        '//*[contains(text(), "Outage Summary")]',
        '//*[contains(text(), "Customer Summary")]',
    ]
    for xpath in summary_xpaths:
        elements = driver.find_elements(By.XPATH, xpath)
        if elements:
            try:
                wait = WebDriverWait(driver, 10)
                el = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
                el.click()
                time.sleep(5)
                print(f"[scraper] Clicked summary panel: {xpath}")
            except Exception as exc:
                logging.debug("Could not click %s: %s", xpath, exc)
            break

    # --- Strategy 3: iterate dropdown menus if present ------------------
    select_elements = driver.find_elements(By.TAG_NAME, "select")
    if select_elements:
        dropdown_results = _scrape_dropdown_levels(select_elements)
        if dropdown_results:
            print(f"[scraper] Captured {len(dropdown_results)} level(s) from dropdowns.")
            results.update(dropdown_results)

    # --- Strategy 4: fallback — parse all HTML tables on the page -------
    if not results:
        print("[scraper] No structured data found via JSON/dropdown; parsing HTML tables.")
        html_results = _parse_html_tables(driver.page_source)
        results.update(html_results)

    if not results:
        logging.warning("[scraper] No outage data could be extracted from %s", url)

    return results

def handler(event, context):
    s3 = boto3.client("s3")
    bucket = "tutorialbucketing12342312"  # TODO: Modify it to your own s3 bucket

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--single-process")
    options.add_argument("--disable-dev-shm-usage")
    options.binary_location = "/opt/chrome/chrome"

    driver = webdriver.Chrome(
        executable_path="/opt/chromedriver", chrome_options=options
    )

    url = "https://webapps.jacksonemc.com/nisc/maps/MemberOutageMap/"

    data = scraper(url, driver)  # TODO: Modify it to your own scraper()

    driver.close()
    driver.quit()

    for key, df in data.items():
        current_time = timenow()
        filename = (
            f"Jackson_{key}_{current_time}.csv"  # TODO: Modify it to your filename
        )
        csv_buffer = pd.DataFrame(df).to_csv(index=False)
        s3.put_object(Bucket=bucket, Key=filename, Body=csv_buffer)

    return {
        "statusCode": 200,
        "body": "Successfully Scrap the Jakson EMC!",
    }  # TODO: Modify it to your own message
