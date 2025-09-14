#fetch data from a sample API and save to CSV

import requests
import pandas as pd
url = "https://api.sample.com/data"
response = requests.get(url)
data = response.json()
df = pd.DataFrame(data)

#save the data in the brozne layer

#processing and transformations
df = df.dropna()  # Example transformation: drop missing values
df['new_column'] = df['existing_column'] * 2  # Example transformation: create a new column

#store the data in the silver/gold layer as a parquet file 

df.to_csv("output.csv", index=False)
