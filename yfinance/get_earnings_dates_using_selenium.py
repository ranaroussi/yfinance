from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from dateutil import parser
from zoneinfo import ZoneInfo
import os
from typing import Optional


def get_earnings_dates_using_selenium(
    driver_path: str, limit: int = 100, ticker: str = "AAPL", headless: bool = True
) -> Optional[pd.DataFrame]:
    """
    Uses Chrome WebDriver to scrap earnings data from YahooFinance.
    Currently doesn't support other browsers : Edge, Safari, firefox etc.
    https://finance.yahoo.com/calendar/earnings

    Args:
        driver_path (str): Absolute path to the chromedriver executable
                           Raises an error if the path is invalid
        limit (int): Number of rows to extract
        ticker (str): Ticker to search for
        headless (bool): Use selenium in headless mode (Don't open the browser)

    Returns:
        pd.DataFrame in the following format.

                   EPS Estimate Reported EPS Surprise(%)
        Date
        2025-10-30         2.97            -           -
        2025-07-22         1.73         1.54      -10.88
        2025-05-06         2.63          2.7        2.57
        2025-02-06         2.09         2.42       16.06
        2024-10-31         1.92         1.55      -19.36
        ...                 ...          ...         ...
        2014-07-31         0.61         0.65        7.38
        2014-05-01         0.55         0.68       22.92
        2014-02-13         0.55         0.58        6.36
        2013-10-31         0.51         0.54        6.86
        2013-08-01         0.46          0.5        7.86

    Raises:
        ValueError: If driver_path is not a valid file path.
    """
    # -------------------------
    # Configure headless Chrome
    # -------------------------
    chrome_options = Options()
    # Don't wait for full page load (ADs take forever to load)
    chrome_options.page_load_strategy = "eager"
    if headless:
        # New headless mode for Chrome 109+
        chrome_options.add_argument("--headless=new")
        # Other optional args
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36")

    # Check if the path is a valid file
    if not os.path.isfile(driver_path):
        raise ValueError(
            f"Chromedriver path is not valid: '{driver_path}'. "
            "Please ensure the path is correct and the file exists."
        )
    else:
        service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)

    # Open the Yahoo earnings calendar page
    url = "https://finance.yahoo.com/calendar/earnings?symbol={}".format(ticker)
    driver.get(url)

    def extract_page(driver):
        # Function that extracts table
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        data = []
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            li_row = [col.text for col in cols]
            data.append(li_row)
        return data

    wait = WebDriverWait(driver, 10)
    li_data = []
    while True:
        # Stop if limit has been reached
        if len(li_data) >= limit:
            break

        # Get current first row (to detect staleness later)
        first_row = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
        )
        old_text = first_row.text

        # Wait until full table appear
        wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table tbody tr"))
        )

        # Extract data
        li_data += extract_page(driver)

        # Find the "next page" button
        next_button = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, 'button[data-testid="next-page-button"]')
            )
        )

        # Stop if disabled attribute exists (end of pagination)
        if next_button.get_attribute("disabled") is not None:
            # print("No more pages.")
            break

        # Click next
        next_button.click()

        # Wait until first row’s text is different
        wait.until(
            lambda d: d.find_element(By.CSS_SELECTOR, "table tbody tr").text != old_text
        )

    # Convert to pandas DataFrame
    cols = [
        "Symbol",
        "Company",
        "Earnings Date",
        "EPS Estimate",
        "Reported EPS",
        "Surprise(%)",
    ]
    df = pd.DataFrame(li_data, columns=cols)

    # praser.parse doesn't understand "EDT", "EST"
    tzinfos = {
        "EDT": ZoneInfo("America/New_York"),
        "EST": ZoneInfo("America/New_York"),
    }
    df.index = df["Earnings Date"].apply(
        lambda date_str: parser.parse(date_str, tzinfos=tzinfos).strftime("%Y-%m-%d")
    )
    df.index.name = "Date"
    # Remove "+" sign from Surprise(%)
    df["Surprise(%)"] = df["Surprise(%)"].apply(
        lambda x: str(x[1:]) if x[0] == "+" else str(x)
    )
    df = df.drop(["Earnings Date", "Company", "Symbol"], axis=1)

    # Close browser
    driver.quit()

    return df


if __name__ == "__main__":
    chromedriver = "YOUR_CHROMEDRIVER_PATH"
    df = get_earnings_dates_using_selenium(chromedriver, limit=9999, ticker="IQV")
    print(df)
