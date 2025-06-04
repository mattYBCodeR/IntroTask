from typing import Optional
import pandas as pd
import io
import requests
import os
from dotenv import load_dotenv

load_dotenv()
NASA_KEY: Optional[str] = os.getenv('NASA_KEY')

def nasa_firms_api() -> pd.DataFrame:
    data_url: str = f'https://firms.modaps.eosdis.nasa.gov/api/data_availability/csv/{NASA_KEY}/all'
    # Noa20_NRT_url: str = f'https://firms.modaps.eosdis.nasa.gov/api/data_availability/csv/{NASA_KEY}/VIIRs_NOAA20_NRT'
    file_Content: bytes = requests.get(data_url).content
    # Noa20_NRT_Content: bytes = requests.get(Noa20_NRT_url).content
    df: pd.DataFrame = pd.read_csv(io.StringIO(file_Content.decode('utf-8')))

    return df

def noaa20_data() -> pd.DataFrame:
    data_url: str = f'https://firms.modaps.eosdis.nasa.gov/api/data_availability/csv/{NASA_KEY}/VIIRS_NOAA20_NRT'
    file_Content: bytes = requests.get(data_url).content
    df: pd.DataFrame = pd.read_csv(io.StringIO(file_Content.decode('utf-8')))

    return df

