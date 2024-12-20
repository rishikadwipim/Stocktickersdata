import time
import functools
import requests
import json
from config import API_KEYS

BASE_URL = "https://api.polygon.io/v3/reference/tickers"
FETCH_LIMIT = 100000
ROWS_PER_REQUEST = 1000
DELAY_BETWEEN_REQUESTS = 5  #(5 seconds to avoid hitting rate limit)


# (Decorator_to_log_execution_time)
def log_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Function {func.__name__!r} executed in {end_time - start_time:.2f} seconds")
        return result
    return wrapper


# (Decorator_to_log_the_function_call_details)
def log_call(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print(f"Calling function {func.__name__} with arguments: {args}, {kwargs}")
        return func(*args, **kwargs)
    return wrapper


@log_time
@log_call
def fetch_data(api_key, limit, offset):
    url = f"{BASE_URL}?apiKey={api_key}&limit={limit}&offset={offset}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()["results"]
    elif response.status_code == 429:
        print("Rate limit exceeded. Waiting before retrying...")
        time.sleep(60)  #(waits for 1 minute before retrying)
        return fetch_data(api_key, limit, offset)
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None


@log_time
def collect_data():
    all_data = []
    total_rows = 0
    api_key_index = 0
    offset = 0

    while total_rows < FETCH_LIMIT:
        api_key = API_KEYS[api_key_index]
        print(f"Using API key {api_key_index + 1}/{len(API_KEYS)} to fetch data...")

        # (Fetching_data_for_this_batch)
        data = fetch_data(api_key, ROWS_PER_REQUEST, offset)

        if data:
            all_data.extend(data)
            total_rows += len(data)
            offset += ROWS_PER_REQUEST
            print(f"Fetched {total_rows} rows")
        else:
            print("Failed to fetch data, skipping to next key.")

        # (Using 25 API's for Switching_to_next_API_key if the current one reaches its limit)
        api_key_index = (api_key_index + 1) % len(API_KEYS)

        # (Sleeps_to_avoid_hitting_the_rate_limit)
        time.sleep(DELAY_BETWEEN_REQUESTS)

    return all_data


def save_data(data):
    with open("tickers_data.json", "w") as file:
        json.dump(data, file)
    print("Data saved to tickers_data.json")


if __name__ == "__main__":
    data = collect_data()
    save_data(data)
