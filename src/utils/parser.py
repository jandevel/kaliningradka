import csv
import os
import random
import re
import sys
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from loguru import logger
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.utils.misc import get_project_root


def _set_logger(file_name=None):
    logger.remove()
    stdout_format = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
    logger.add(sys.stderr, format=stdout_format, level="DEBUG")
    project_path = get_project_root()
    if file_name:
        now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_format = "{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
        logger.add(project_path / f"logs/parser/{file_name}_{now}.log", format=file_format, level="DEBUG")
    return logger

def _get_chrome_driver():
    # Create a new instance of the Google Chrome driver
    service = Service()
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--headless')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--disable-setuid-sandbox")
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def get_image_links(image_links_file="image_links.txt", sleep_period=1, verbose=100):
    logger = _set_logger(file_name="get_image_links")
    driver = _get_chrome_driver()

    success_pages = 0
    no_images_failed_pages = 0
    timeout_failed_pages = 0

    # Iterate over the range of pages
    project_path = get_project_root()
    image_links_file = project_path / f"data/parser/{image_links_file}"
    with open(image_links_file, 'w') as f:
        for page_number in range(1, 12610):  # 12610 - max on 2023-06-18
            time.sleep(sleep_period)
            try:
                # Format the URL with the current page number
                url = f"https://kaliningradka.kantiana.ru/archive/newspapers/{page_number}/"
                driver.get(url)
                # Wait up to 10 seconds for the page to fully load
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'img')))
                html_content = driver.page_source

                # Parse the HTML and find all image elements
                soup = BeautifulSoup(html_content, 'html.parser')
                images = soup.find_all('img')

                # If at least one image link exists
                if images[0].get('src') is not None:
                    for img in images:
                        line = f"{page_number},1,{url},{img.get('src')}\n"
                        f.write(line)
                    success_pages += 1
                # No image links on the page
                else:
                    logger.warning(f"No images! Page {page_number} failed to load.")
                    line = f"{page_number},2,{url},failed\n"
                    f.write(line)
                    no_images_failed_pages += 1

            except TimeoutException:
                # If a timeout exception is thrown, print an error message and move on to the next page
                logger.warning(f"Timeout! Page {page_number} failed to load.\n")
                line = f"{page_number},3,{url},failed\n"
                f.write(line)
                timeout_failed_pages += 1
                continue

            if page_number % verbose == 0:
                logger.info(f"Processed {page_number} pages.")
    driver.quit()

    logger.info(f"SUCCESS: {success_pages} pages")
    logger.info(f"NO IMAGES FAIL: {no_images_failed_pages} pages")
    logger.info(f"TIMEOUT FAIL: {timeout_failed_pages} pages")


def download_images_from_txt(image_links_file, download_dir, csv_file, sleep_period=1, num_random_links=0, verbose=500):
    logger = _set_logger(file_name="download_images")
    if num_random_links > 0:
        numbers = random.sample(range(49716), num_random_links)
        logger.info(f"Test the following random links: {numbers}")
    else:
        numbers = []

    project_path = get_project_root()

    download_dir = project_path / f"data/{download_dir}"
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    image_links_file = project_path / f"data/parser/{image_links_file}"
    with open(image_links_file, 'r') as f:
        lines = f.readlines()

    success_pages = 0
    failed_code_pages = 0
    failed_format_pages = 0

    csv_file = project_path / f"data/parser/{csv_file}"
    with open(csv_file, 'w', newline='') as csvfile:
        fieldnames = ['line_number', 'date', 'number', 'page', 'status', 'link', 'filename', 'extension']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for line_number, line in enumerate(lines, start=1):
            if (line != "failed") and ((num_random_links == 0) or (num_random_links > 0 and line_number in numbers)):
                line = line.strip().split(',')
                line = line[-1]
                matches = re.search(r'^http.*%D0%9A%D0%9F/(\d+)/(\d+)-(\d+)-(\d+)-(\d+)/(\d+)(?:--|-|-%20|%20-|%20-%20|_)(\d+)(?:--|-|-%20|%20-|%20-%20|_)(\d+)(?:--|-|-%20|%20-|%20-%20|_)(\d+).(\w+|\.\w+)', line, re.I)
                if matches:
                    year = matches.group(1)
                    number = matches.group(2).zfill(3)
                    day = matches.group(3).zfill(2)
                    month = matches.group(4).zfill(2)
                    page = matches.group(9).zfill(2)
                    extension = matches.group(10).lower().lstrip('.')
                    date = f"{year}-{month}-{day}"

                    new_filename = f"{date}_{number}_{page}.{extension}"
                    new_filepath = os.path.join(download_dir, new_filename)

                    response = requests.get(line, stream=True)
                    if response.status_code == 200:
                        with open(new_filepath, 'wb') as out_file:
                            out_file.write(response.content)
                        status = 1
                        success_pages += 1
                    else:
                        logger.warning(f"Failed to download image from line: {line_number}. Response code is {response.status_code}")
                        status = 2
                        failed_code_pages += 1
                else:
                    failed_format_pages += 1
                    logger.warning(f"URL does not match expected format. Line {line_number}")
                    status = 3
                    new_filename = None
                    date = None
                    number = None
                    page = None
                    extension = None

                # write to csv
                writer.writerow({
                    'line_number': line_number,
                    'date': date,
                    'number': number,
                    'page': page,
                    'status': status,
                    'link': line,
                    'filename': new_filename,
                    'extension': extension,
                })
                if line_number % verbose == 0:
                    logger.info(f"PROCESSED {line_number} pages. SUCCESS: {success_pages}. FAIL CODE: {failed_code_pages}. FAIL FORMAT: {failed_format_pages}")
                time.sleep(sleep_period)

    logger.info(f"The file processed. Results:")
    logger.info(f"PROCESSED {line_number} pages. SUCCESS: {success_pages}. FAIL CODE: {failed_code_pages}. FAIL FORMAT: {failed_format_pages}")


def check_filenames(directory):
    # Constructing the pattern for file name using regular expressions
    # \d{4} - matches any four digits
    # ^(19[4-6][0-9]|197[0-9]|198[0-9]|199[0-1]) - matches years from 1946 to 1991
    # (0[1-9]|1[0-2]) - matches months from 01 to 12
    # (0[1-9]|[1-2][0-9]|3[0-1]) - matches days from 01 to 31
    # ([0-3][0-9][0-9]) - matches numbers from 001 to 399
    # (0[1-4]) - matches numbers from 01 to 04
    # (jpg|png) - matches either jpg or png
    logger = _set_logger()
    project_path = get_project_root()
    directory = project_path / f"data/{directory}"
    pattern = re.compile(r"^(19[4-6][0-9]|197[0-9]|198[0-9]|199[0-1])-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])_([0-3][0-9][0-9])_(0[1-4])\.(jpg|png)$")
    count = 0
    # Use os.listdir to get file names in the directory
    for filename in os.listdir(directory):
        # Use re.match to check if the filename matches the pattern
        if not pattern.match(filename):
            count += 1
            logger.warning(f"File '{filename}' does not match the required pattern.")
    logger.info(f"{count} files do not match the required pattern.")


if __name__ == "__main__":
    get_image_links(
        image_links_file="image_links.txt",
        sleep_period=1,
        verbose=100,
    )
    download_images_from_txt(
        image_links_file="image_links.txt",
        download_dir="raw_data",
        csv_file="download_log.csv",
        sleep_period=2,
        num_random_links=0,
        verbose=500,
    )
    check_filenames("raw_data")