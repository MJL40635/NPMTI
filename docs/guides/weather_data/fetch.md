# Historical Weather Data Processing

## Overview
The Weather Data Fetcher is a Python script designed to fetch and update weather data from the NASA POWER API and store it in a MongoDB database. It intelligently updates existing records, checks for new data, and ensures that the most recent weather data is available. The script supports both full data processing and a test mode for processing a subset of locations.

## Features
- **Automated Data Fetching**: Retrieves weather data for specified parameters from the NASA POWER API.  

- **Incremental Updates**: Updates existing records and inserts new ones, avoiding redundant data pulls.  

- **Parallel Processing**: Utilizes multithreading to process multiple locations concurrently.  

- **Retry Mechanism**: Implements retry logic with exponential backoff to handle transient API errors.  

- **Configurable**: Allows customization through a config.yaml file and command-line arguments.  

- **Logging**: Provides detailed logs for monitoring and debugging.  

## Prerequisites
- **Python**: Version 3.6 or higher.
- **MongoDB**: Access to a MongoDB database (local or cloud-hosted).
- **Python Packages**:
```bash
requests
pymongo
tenacity
PyYAML
```

## Installation
1. Clone the Repository
  ```bash
  git clone https://github.com/yourusername/weather-data-fetcher.git
  cd weather-data-fetcher
  ```

2. Create a Virtual Environment (Optional but Recommended)  
  **On Unix/Linux/macOS**
  ```bash
  python3 -m venv venv
  source venv/bin/activate
  ```
  **On Windows**
  ```
  python -m venv venv
  venv\Scripts\activate
  ```

3. Install Required Packages
  ```bash
  pip install -r requirements.txt
  ```
  If requirements.txt does not exist, install packages manually:
  ```bash
  pip install requests pymongo tenacity PyYAML
  ```
4. Set Up MongoDB  
  **Local MongoDB:**  
    Install MongoDB Community Edition from the official website.  
    Start the MongoDB service  
  **Cloud MongoDB (e.g., MongoDB Atlas):**  
    Sign up for a free account on MongoDB Atlas.  
    Create a cluster and obtain the connection string (URI).

5. Configure Environment Variables  
  Set the MONGODB_URI environment variable with your MongoDB connection string.
  ```bash
  # On Unix/Linux/macOS
  export MONGODB_URI='your_mongodb_connection_string_here'

  # On Windows Command Prompt
  set MONGODB_URI='your_mongodb_connection_string_here'

  # On Windows PowerShell
  $env:MONGODB_URI='your_mongodb_connection_string_here'
  ```
6. Create config.yaml File  
  Create a config.yaml file in the project directory with the following content:
  ```yaml
    mongodb_uri: 'your_mongodb_connection_string_here'
    re_fetch_days: 7
    test_mode: false
    test_counties:
      - '01001'  # Autauga County, AL
      - '06037'  # Los Angeles County, CA
    parameters:
      - 'T2M'
      - 'WD2M'
      - 'WS2M'
      - 'PS'
      - 'GWETROOT'
      - 'TS'
      - 'CDD0'
      - 'CDD18_3'
      - 'CDD10'
      - 'CLOUD_AMT'
      - 'EVLAND'
      - 'ALLSKY_SFC_PAR_TOT'
      - 'PRECTOTCORR'
    max_workers: 5  # For parallel processing
  ```
    Replace 'your_mongodb_connection_string_here' with your actual MongoDB URI

    Adjust test_counties with the FIPS codes relevant to your test cases.

    Modify other parameters as needed.

7. Prepare the MongoDB Collections

    Ensure that your MongoDB database contains the necessary collections:

    - **county_data**: Contains county information with fields like county_name, state_name, fips_code, latitude, and longitude.

    - **weather_data**: Will be used by the script to store weather data.

    If you don't have the county_data collection, you need to populate it with the required county data.

## Usage
**Command-Line Arguments**

The script supports the following command-line arguments:
--config: Path to the configuration file (default: config.yaml).
--test: Run the script in test mode using counties specified in test_counties.
--re-fetch-days: Number of days to re-fetch for updates (overrides config.yaml).
--max-workers: Maximum number of worker threads (overrides config.yaml).

**Running the Script**

1. Standard Execution
To run the script with default settings:
```bash
python fetch.py
```
2. Test Mode Execution
To run the script in test mode:
```bash
python fetch.py --test
```
3. Custom Configuration File
If you have a custom configuration file:
```bash
python fetch.py --config path/to/your_config.yaml
```
4. Override Configuration Parameters
To override re_fetch_days and max_workers:
```bash
python fetch.py --re-fetch-days 10 --max-workers 3
```
5. Display Help
To see all available options:
```bash
python fetch.py --help
```

**Example Usage**

Run in Test Mode with Specific Counties
Update config.yaml:
```yaml
test_mode: true
test_counties:
  - '01001'  # Autauga County, AL
  - '06037'  # Los Angeles County, CA
```
Run the script:
```bash
python fetch.py
```
Run with Custom Parameters
```bash
python fetch.py --re-fetch-days 14 --max-workers 10
```

## Configuration Details

**config.yaml Parameters**

- **mongodb_uri**: MongoDB connection string (can also be set via the MONGODB_URI environment variable).

- **re_fetch_days**: Number of days to re-fetch data for updates (default: 7).

- **test_mode**: Set to true to run the script in test mode.

- **test_counties**: List of FIPS codes to process when in test mode.

- **parameters**: List of weather parameters to fetch from the NASA POWER API.

- **max_workers**: Maximum number of concurrent threads for processing (default: 5).

**Adjusting Parameters**

- Adding New Weather Parameters: Simply add the parameter name to the parameters list in config.yaml. The script will automatically handle the new parameter.

- Changing Fetch Range: Modify re_fetch_days to adjust how many days back the script should check for updates.

- Thread Management: Increase or decrease max_workers based on your system's capabilities and the NASA POWER API's rate limits.

**Logging**

- Log File: The script logs detailed information to weather_data.log.

- Logging Levels: Adjust the logging level in the script (logging.basicConfig) if you need more or less verbosity.

- Monitoring Execution: Check the log file to monitor progress and debug any issues.

## Troubleshooting

**Common Issues**

1. No Counties Found to Process

    If you see:
    ```txt
    No counties found to process. Exiting script.
    ```

    Ensure that test_counties in config.yaml contains valid FIPS codes present in your county_data_test collection.

    Verify that fips_code in your database matches the data type (string or integer) used in test_counties.

2. MongoDB Connection Error

    Confirm that the mongodb_uri is correct and accessible.

    Check network configurations and firewall settings.

    Ensure that your MongoDB service is running.

3. API Request Failures

    The NASA POWER API might be experiencing downtime or rate limiting.

    Check your internet connection.

    Review the log file for specific error messages.

**Getting More Debug Information**

Set the logging level to DEBUG for more detailed logs:
```python
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler('weather_data.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
```
Run the script and review weather_data.log for detailed information.

**Scheduling Runs**

Use a task scheduler like cron (Unix/Linux) or Task Scheduler (Windows) to run the script daily or at your desired frequency.

**Contributing**

If you wish to contribute:

Fork the Repository: Create your own fork of the project.

Create a Branch: Work on your feature or bug fix in a new branch.

Submit a Pull Request: Describe your changes and submit a pull request for review.

## Contact

For questions or support, please contact:

michael.lavelle@mesur.io