#!/usr/bin/env python3

import csv
import datetime
import logging
import os
import time

from json.decoder import JSONDecodeError
from requests.exceptions import ConnectionError

import gdax

from utils import connection_retry


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GDAXOrderBookScraper:
    """
    Run the GDAX order book scraper

    Attributes:
        products: The products being tracked by the trader
        order_book_data: The order book data being pulled for the next CSV
        current_time: The datetime of the current iteration
        csv_directory: The directory to store the CSV files in
    """

    # Environment variables required for authenticating with GDAX
    GDAX_KEY_ENV = 'GDAX_KEY'
    GDAX_SECRET_ENV = 'GDAX_SECRET'
    GDAX_PASSPHRASE_ENV = 'GDAX_PASSPHRASE'
    GDAX_API_URL_ENV = 'GDAX_API_URL'

    # GDAX rate limit of 3 requests per second with a little extra padding
    RATE_LIMIT = 1.0 / 3.0 + 0.5

    # Maximum number of retry attempts after a connection error
    MAX_RETRIES = 5

    # Frequency of order book scrapes in seconds
    FREQUENCY = 60

    # Number of order book scrapes per CSV file
    SCRAPES_PER_CSV = 5

    # Default directory to output CSV files to
    DEFAULT_OUTPUT_DIR = 'data'


    def __init__(self):
        self.products = []
        self.order_book_data = []

        self.current_time = datetime.datetime.now(datetime.timezone.utc)
        self.csv_directory = GDAXOrderBookScraper.DEFAULT_OUTPUT_DIR

        self.client = GDAXOrderBookScraper._get_client()


    def add_product(self, product):
        self.products.append(product)


    def run(self):
        """
        Start the GDAX order book scraper
        """

        running = True

        logger.info('Starting GDAX Order Book Scraper...')

        start_time = time.time()

        while running:
            success = self._run_iteration()

            if not success:
                logger.warning('Data unavailable, iteration skipped...')

            # Sleep to achieve the desired frequency
            frequency = GDAXOrderBookScraper.FREQUENCY
            elapsed_time = (time.time() - start_time) % frequency
            sleep_time = frequency - elapsed_time

            logger.info('Sleeping for {:.2f} seconds'.format(sleep_time))

            time.sleep(sleep_time)


    def _run_iteration(self):
        """
        Perform an iteration of the GDAX order book scraper
        """

        # Set current time of iteration
        self.current_time = datetime.datetime.now(datetime.timezone.utc)

        current_time_iso = self.current_time.isoformat()
        logger.info('Running next iteration at {}'.format(current_time_iso))

        order_book_data_all = {}

        # Get all product order book data
        for product in self.products:

            logger.info('Getting order book data for {}'.format(product))

            try:
                order_book_data = self._get_order_book_data(product),

            # Skip iteration if order book data is unavailable
            except (ConnectionError, JSONDecodeError):
                return False

            # For some reason the data is contained in a single element tuple
            try:
                order_book_data_all[product] = order_book_data[0]
            except TypeError:
                return False

        # Append order book data to order book history
        self.order_book_data.append(order_book_data_all)

        # Write new CSV file with the latest order book data
        if len(self.order_book_data) >= GDAXOrderBookScraper.SCRAPES_PER_CSV:
            self.write_order_book_csv()
            self.order_book_data = []

        return True


    def write_order_book_csv(self):
        """
        Write GDAX price history to CSV file

        :param history: GDAX price history
        """

        for order_book_data in self.order_book_data:

            for product in order_book_data:

                # Create output directories if they do not exist
                product_dir = os.path.join(self.csv_directory, product)
                os.makedirs(product_dir, exist_ok=True)

                # Set filename to the current time so the files can be ordered
                csv_filename = str(self.current_time.timestamp())
                csv_path = os.path.join(product_dir, csv_filename)

                logger.info('Writing CSV file to {}'.format(csv_path))

                with open(csv_path, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)

                    # The different order types
                    #   - The key is the column in the order book data
                    #   - The value is the flag for the `prepare` function
                    order_types = {
                        'bids': 'b',
                        'asks': 'a',
                    }

                    # Loop through each type of orders
                    for order_type in order_types:

                        # Loop through every order for an order type
                        for row in order_book_data[product][order_type]:

                            ot = order_types[order_type]
                            row = self.prepare_order_book_row(row, ot)

                            writer.writerow(row)


    def prepare_order_book_row(self, row, order_type='b'):
        """
        Prepare a single row from the order book for writing to CSV

        Row column format is as follows:
            - Date
            - Order type
            - Price
            - Size
            - Number of orders

        :param row: the row from the order book
        :param order_type: is the row from the bid or ask column.
            Use ``b`` for bid and ``a`` for ask.
        """

        datetime_iso = self.current_time.isoformat(' ')

        try:
            # Add the Order type column
            row.insert(0, order_type)

            # Add the Date column
            row.insert(0, datetime_iso)

        # Skip row if it is the wrong type
        except AttributeError as error:
            logger.warning('Skipping row: {}: {}'.format(error, row))
            return None

        return row


    @classmethod
    def _get_client(cls):
        """
        Get a public GDAX client

        :returns: a GDAX client
        """

        client = gdax.PublicClient()

        return client


    @connection_retry(MAX_RETRIES, RATE_LIMIT)
    def _get_order_book_data(self, product):
        """
        Get order book data for a product

        :param product: the GDAX product
        :returns: order book data
        """

        return self.client.get_product_order_book(product, level=2)


if __name__ == '__main__':
    scraper = GDAXOrderBookScraper()
    scraper.add_product('BTC-USD')

    scraper.run()
