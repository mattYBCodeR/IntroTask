import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def nasa_firms_api():
    NASA_KEY = os.getenv('NASA_KEY')
    data_url = f'https://firms.modaps.eosdis.nasa.gov/api/data_availability/csv/{NASA_KEY}/all'
    df = pd.read_csv(data_url)
    print(df.head())
    return df

