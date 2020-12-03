from typing import List, Dict
import requests
from ratelimiter import RateLimiter
import json
import time
import math
import pickle
import pandas as pd


class Crawler:
    SESSION_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    }

    def __init__(self, backup_dir, verbose):
        self.backup_dir = backup_dir
        self.verbose = verbose

    @RateLimiter(max_calls=1, period=1)
    def _call_to_api(self, s, page):
        """
        Function extracting total number of records and JSON list from vivino API explore endpoint with rate limiting
        """
        response = s.get(page)
        json_str = response.content
        try:
            json_obj = json.loads(json_str)
            return json_obj
        except:
            print(response.content)

    def _parse_vintages(self, s, page_num: int, price_min: int, price_max: int):
        """
        Function that extracts records from a single page
        """
        page_template = 'https://www.vivino.com/api/explore/explore?country_code=GB&currency_code=GBP' \
                        '&grape_filter=varietal&min_rating=1&order_by=ratings_average&order=desc&page={}' \
                        '&per_page=100&price_range_min={}&price_range_max={}'
        page = page_template.format(page_num, price_min, price_max)
        json_obj = self._call_to_api(s, page)
        return json_obj['explore_vintage']['matches']

    def _parse_vintage_num(self, s, page_num: int, price_min: int, price_max: int):
        """
        Function that extracts total number of matches
        """
        page_template = 'https://www.vivino.com/api/explore/explore?country_code=GB&currency_code=GBP' \
                        '&grape_filter=varietal&min_rating=1&order_by=ratings_average&order=desc&page={}' \
                        '&per_page=100&price_range_min={}&price_range_max={}'
        page = page_template.format(page_num, price_min, price_max)
        json_obj = self._call_to_api(s, page)
        return json_obj['explore_vintage']['records_matched']

    def _parse_reviews(self, s, wine_id: int, year: str, page_num: int):
        """
        Function that returns reviews extracted for a particular wine ID and year, and a particular page
        """
        page_template = 'https://www.vivino.com/api/wines/{}/latest_reviews?year={}&per_page=50&page={}'
        page = page_template.format(wine_id, year, page_num)
        json_obj = self._call_to_api(s, page)
        return json_obj['reviews']

    def download_reviews(self, wines: pd.DataFrame, country, year) -> List[Dict]:
        """
        Function that returns all reviews extracted for a particular wine ID and year, and appends them to a given list.
        """
        s = requests.Session()
        s.headers.update(Crawler.SESSION_HEADERS)

        reviews = []
        timepoint_0 = time.time()

        for index, row in wines.iterrows():
            wine_id = row['wine_id']
            num = row['rating_count']

            max_pages = math.ceil(num / 50)

            # if self.verbose:
            #     print(
            #         f"The program will send up to {max_pages} requests to reviews API to extract up to {num} reviews for wine {wine_id}")

            for it in range(1, max_pages + 1):
                reviews_batch = self._parse_reviews(s, wine_id, year, it)
                if len(reviews_batch) == 0:
                    break
                else:
                    reviews += reviews_batch

            # if self.verbose and index % 100 == 0:
            #     print(
            #         f"Record no. {index} with id {wine_id} finished. Currently stores {len(set([review['id'] for review in reviews]))} records")

        if self.verbose:
            print(f"Program uploaded {len(set([review['id'] for review in reviews]))} reviews for {country} "
                  f"for the year {year}. It took app. {round((time.time() - timepoint_0) / 60, 2)} minutes to run")

        s.close()

        return reviews

    def download_all_wines(self, price_min=0, price_max=400, with_prices=True, inter_backup=True, final_backup=True):
        """
        Function that iterates over small price ranges to extract all the data within a given range between min price and max price.
        If necessary, the function may store intermediate backups and/or a final backup inside pickle files.
        """
        s = requests.Session()
        s.headers.update(Crawler.SESSION_HEADERS)

        if self.verbose:

            records_total = self._parse_vintage_num(s, 1, price_min, price_max)

            if price_min == 0 and price_max == 400 and with_prices is True: # remove wines with no prices
                # here, we remove all records with no prices. Those appear only when range (0, 400) is called
                records_total -= self._parse_vintage_num(s, 1, 0, 0)

            # elif price_min == 0 and price_max == 400 and with_prices is not True:
            #     # here, we remove all records with no prices. Those appear only when range (0, 400) is called
            #     records_total += self._parse_vintage_num(s, 1, 0, 0)

            print(
                f'''The program recognized app.{records_total} unique records with a price from {price_min} to {price_max} 
                and will run for app.{math.ceil(records_total / 100)} iterations in total''')

        full_wine_list = []
        timepoint_start = time.time()

        # define iteration variables
        timepoint_iter = timepoint_start
        cur_batch = []

        # load each price
        for i in range(price_min, price_max):
            if i == 0 and with_prices is not True:  # special case to cover wines with no prices
                cur_price_records_num = self._parse_vintage_num(s, 1, i,
                                                                i)  # TODO consider the case when iterations exceed 80
            else:
                cur_price_records_num = self._parse_vintage_num(s, 1, i, i + 1)
            # to make sure the necessary number of iterations to catch all of the records with a given price:
            iterations_required = math.ceil(cur_price_records_num / 100)
            # load each page of a given price
            for it in range(1, iterations_required + 1):
                if i == 0 and with_prices is not True:  # special case to cover wines with no prices
                    cur_batch += self._parse_vintages(s, 1, i, i)
                else:
                    cur_batch += self._parse_vintages(it, i, i + 1)
                # for every 10th price write intermediate backup, send the results to the main list and update the batch
                if i % 10 == 0 and it == iterations_required:
                    if inter_backup:
                        # piece of code necessary to write intermediate backups in the process
                        with open(self.backup_dir + f"match_list_{i - 1}", 'wb') as f:
                            pickle.dump(cur_batch, f)
                    # append the batch results to the main list
                    full_wine_list.extend(cur_batch)
                    cur_batch = []

                    if self.verbose:
                        time_batch = time.time() - timepoint_iter
                        print(
                            f'''Bunch of records with price below {i + 1} uploaded and took {round(time_batch / 60, 2)} minutes. 
                            Next, running prices from {i + 1}...''')
                        timepoint_iter = time.time()

            full_wine_list.extend(cur_batch)

        if final_backup:
            with open(self.backup_dir + "full_match_list", 'wb') as f:
                pickle.dump(full_wine_list, f)

        if self.verbose:
            total_time = time.time() - timepoint_start
            print(
                f'''Program finished and successfully uploaded {len(full_wine_list)} wine records with prices(need to check duplicates).
                The program took app. {round(total_time / 60, 2)} minutes to run''')

        s.close()

        return full_wine_list
