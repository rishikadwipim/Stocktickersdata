import pandas as pd
import time

start_time = time.time()

# Load JSON data directly into a pandas DataFrame
df = pd.read_json("tickers_data.json")

# Optionally, you can convert it to CSV
df.to_csv("tickers_data.csv", index=False)

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Time taken to load and convert to CSV: {elapsed_time:.2f} seconds")
