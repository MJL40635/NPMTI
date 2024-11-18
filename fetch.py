import requests
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING
import time
import os
import sys
import logging
import yaml
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler('weather_data.log'),
        logging.StreamHandler(sys.stdout)
    ]
)


class WeatherDataFetcher:
    def __init__(self, config):
        # Load configuration parameters
        self.mongodb_uri = os.getenv('MONGODB_URI', config.get('mongodb_uri'))
        if not self.mongodb_uri:
            raise ValueError("MongoDB URI not provided. Set the 'MONGODB_URI' environment variable or 'mongodb_uri' in config.yaml")

        self.re_fetch_days = config.get('re_fetch_days', 7)
        self.test_mode = config.get('test_mode', False)
        self.test_counties = config.get('test_counties', [])
        self.parameters = config.get('parameters', [])
        self.max_workers = config.get('max_workers', 5)

        # Set up MongoDB client
        self.client = MongoClient(self.mongodb_uri)
        self.county_db = self.client['NPMTI']
        self.county_collection = self.county_db['county_data']
        self.weather_db = self.client['NPMTI']
        self.weather_collection = self.weather_db['weather_data']

        # Create indexes for optimization
        self.weather_collection.create_index([('fips_code', ASCENDING), ('date', ASCENDING)])

        # Set up a semaphore for limiting concurrent API calls
        self.semaphore = Semaphore(self.max_workers)

    def get_latest_date_for_county(self, county):
        county_filter = {
            'fips_code': county['fips_code']
        }
        latest_record = self.weather_collection.find_one(county_filter, sort=[('date', -1)])
        if latest_record:
            return latest_record['date']
        else:
            return None

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True
    )
    def fetch_api_data(self, api_url):
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()

    def fetch_and_store_data(self, county):
        with self.semaphore:
            county_name = county.get('county_name')
            state_name = county.get('state_name')
            fips_code = county.get('fips_code')
            latitude = county.get('latitude')
            longitude = county.get('longitude')

            # Ensure all required fields are present
            if not all([county_name, state_name, fips_code, latitude, longitude]):
                logging.warning(f"Missing data for county: {county}")
                return

            try:
                # Get the latest date for this county from MongoDB
                latest_date_in_db = self.get_latest_date_for_county(county)
                if latest_date_in_db:
                    # Re-fetch data for the past 're_fetch_days' days to check for updates
                    start_date_dt = (latest_date_in_db - timedelta(days=self.re_fetch_days)).date()
                    # Ensure start_date_dt is not before 5 years ago
                    five_years_ago = datetime.today().date() - timedelta(days=5*365)
                    start_date_dt = max(start_date_dt, five_years_ago)
                else:
                    # If no data in DB, start from 5 years ago
                    start_date_dt = datetime.today().date() - timedelta(days=5*365)

                # Set end_date to yesterday
                end_date_dt = datetime.today().date() - timedelta(days=1)

                # Ensure start_date_dt is not after end_date_dt
                if start_date_dt > end_date_dt:
                    logging.info(f"No new data to fetch for {county_name}, {state_name}")
                    return

                # Convert dates to strings in 'YYYYMMDD' format
                start_date_str = start_date_dt.strftime('%Y%m%d')
                end_date_str = end_date_dt.strftime('%Y%m%d')

                # Build API URL
                parameters_str = ','.join(self.parameters)
                api_url = (
                    f'https://power.larc.nasa.gov/api/temporal/daily/point?'
                    f'parameters={parameters_str}&community=AG&latitude={latitude}'
                    f'&longitude={longitude}&start={start_date_str}&end={end_date_str}&format=JSON'
                )

                data = self.fetch_api_data(api_url)

                # Check for data availability
                if 'properties' in data and 'parameter' in data['properties']:
                    parameters_data = data['properties']['parameter']
                    dates = parameters_data[next(iter(parameters_data))].keys()

                    for date_str in dates:
                        date_dt = datetime.strptime(date_str, '%Y%m%d')
                        record = {
                            'county_name': county_name,
                            'state_name': state_name,
                            'fips_code': fips_code,
                            'latitude': latitude,
                            'longitude': longitude,
                            'date': date_dt
                        }
                        # Add parameters to the record
                        for param in self.parameters:
                            param_data = parameters_data.get(param)
                            if param_data and date_str in param_data:
                                record[param.lower()] = param_data[date_str]
                            else:
                                record[param.lower()] = None

                        # Build a filter to check if record exists
                        filter_query = {
                            'fips_code': fips_code,
                            'date': date_dt
                        }
                        # Upsert the record
                        self.weather_collection.update_one(
                            filter_query,
                            {'$set': record},
                            upsert=True
                        )
                    logging.info(f"Updated data for {county_name}, {state_name}")
                else:
                    logging.warning(f"No data available for {county_name}, {state_name}")

            except requests.exceptions.HTTPError as http_err:
                logging.error(f"HTTP error occurred for {county_name}, {state_name}: {http_err}")
            except Exception as err:
                logging.exception(f"An unexpected error occurred for {county_name}, {state_name}")

    def run(self):
        try:
            if self.test_mode:
                counties_cursor = self.county_collection.find({'fips_code': {'$in': self.test_counties}})
            else:
                counties_cursor = self.county_collection.find({})

            counties = list(counties_cursor)
            logging.info(f"Number of counties to process: {len(counties)}")  # Add this line

            if not counties:
                logging.warning("No counties found to process. Exiting script.")
                return

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self.fetch_and_store_data, county) for county in counties]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logging.exception(f"Error processing county: {e}")
        finally:
            # Ensure MongoDB client is closed
            self.client.close()
            logging.info("MongoDB connection closed.")


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Weather Data Fetcher')
    parser.add_argument('--config', type=str, default='config.yaml', help='Path to the configuration file')
    parser.add_argument('--test', action='store_true', help='Run in test mode')
    parser.add_argument('--re-fetch-days', type=int, help='Number of days to re-fetch for updates')
    parser.add_argument('--max-workers', type=int, help='Maximum number of worker threads')
    args = parser.parse_args()

    # Load configuration from file
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)

    # Override config parameters with command-line arguments if provided
    if args.test:
        config['test_mode'] = True
    if args.re_fetch_days is not None:
        config['re_fetch_days'] = args.re_fetch_days
    if args.max_workers is not None:
        config['max_workers'] = args.max_workers

    # Initialize and run the fetcher
    fetcher = WeatherDataFetcher(config)
    fetcher.run()

if __name__ == '__main__':
    main()
