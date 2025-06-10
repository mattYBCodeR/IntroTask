from typing import Optional
import pandas as pd
import io
import requests
import os
import datetime
from dotenv import load_dotenv

load_dotenv()
NASA_KEY: Optional[str] = os.getenv('NASA_KEY')

def nasa_firms_api() -> pd.DataFrame:
    data_url: str = f'https://firms.modaps.eosdis.nasa.gov/api/data_availability/csv/{NASA_KEY}/all'
    file_Content: bytes = requests.get(data_url).content
    df: pd.DataFrame = pd.read_csv(io.StringIO(file_Content.decode('utf-8')))
    return df

# VARAIBLES FOR FUNCTIONS
filtered_df: pd.DataFrame = nasa_firms_api().iloc[[0,2]]
modis_NRT_name = filtered_df.iloc[0,0]
modis_NRT_mindate = filtered_df.iloc[0,1]
modis_NRT_maxdate = filtered_df.iloc[0,2]


viirs_NRT_name = filtered_df.iloc[1,0]
viirs_NRT_mindate = filtered_df.iloc[1,1]
viirs_NRT_maxdate = filtered_df.iloc[1,2]

AREA_COORDINATES: str = '-150,40,-49,79'  # Canada coordinates

def dataSensors_data() -> pd.DataFrame:

    # GETTING THE SOURCE
    data_url_MODIS: str = f'https://firms.modaps.eosdis.nasa.gov/api/data_availability/csv/{NASA_KEY}/{modis_NRT_name}'
    data_url_VIIRS: str = f'https://firms.modaps.eosdis.nasa.gov/api/data_availability/csv/{NASA_KEY}/{viirs_NRT_name}'

    file_Content_MODIS: bytes = requests.get(data_url_MODIS).content
    file_Content_VIIRS: bytes = requests.get(data_url_VIIRS).content

    modis_NRT_df: pd.DataFrame = pd.read_csv(io.StringIO(file_Content_MODIS.decode('utf-8')))
    viirs_NRT_df: pd.DataFrame = pd.read_csv(io.StringIO(file_Content_VIIRS.decode('utf-8')))
    combined_source_df = pd.concat([modis_NRT_df, viirs_NRT_df], ignore_index=True)
    print("Combined Data Sources:\n", combined_source_df)
    print()

    # GETTING data with AREA COORDINATES FOR CANADA ONLY 1 DAY
    data_area_url_MODIS: str = f'https://firms.modaps.eosdis.nasa.gov/api/area/csv/{NASA_KEY}/{modis_NRT_name}/{AREA_COORDINATES}/1/{modis_NRT_mindate}'
    data_area_url_VIIRS: str = f'https://firms.modaps.eosdis.nasa.gov/api/area/csv/{NASA_KEY}/{viirs_NRT_name}/{AREA_COORDINATES}/1/{viirs_NRT_mindate}'

    file_Content_area_MODIS: bytes = requests.get(data_area_url_MODIS).content
    file_Content_area_VIIRS: bytes = requests.get(data_area_url_VIIRS).content

    modis_area_NRT_df: pd.DataFrame = pd.read_csv(io.StringIO(file_Content_area_MODIS.decode('utf-8')))
    viirs_area_NRT_df: pd.DataFrame = pd.read_csv(io.StringIO(file_Content_area_VIIRS.decode('utf-8')))
    combined_area_df = pd.concat([modis_area_NRT_df, viirs_area_NRT_df], ignore_index=True)
    print("Getting Data with Area Coordinates for Canada for 1 day:")
    return combined_area_df

"""
Function to fetch all data for a given sensor and date range. Starts from a minimum date 
and fetches data in chunks of up to 10 days until the maximum date is reached.
Current_date gets incremented by the day_range, which is the minimum of 10 or the days left until max_date.
"""
def fetch_all_data(KEY=NASA_KEY, sensor_name =None, area_coords=AREA_COORDINATES, min_date_str=None, max_date_str=None):
    min_date = datetime.datetime.strptime(min_date_str, '%Y-%m-%d')
    max_date = datetime.datetime.strptime(max_date_str, '%Y-%m-%d')
    dfs = []
    current_date = min_date

    while current_date <= max_date:
        days_left = (max_date - current_date).days + 1
        day_range = min(10, days_left)
        start_date_str = current_date.strftime('%Y-%m-%d')
        full_url = f'https://firms.modaps.eosdis.nasa.gov/api/area/csv/{KEY}/{sensor_name}/{area_coords}/{day_range}/{start_date_str}'
        full_response = requests.get(full_url)
        df = pd.read_csv(io.StringIO(full_response.content.decode('utf-8')))

        if not df.empty:
            dfs.append(df)

        current_date += datetime.timedelta(days=day_range)

    return pd.concat(dfs, ignore_index=True)


def print_dfs():
    # Example usage
    modis_df = fetch_all_data(NASA_KEY, modis_NRT_name, AREA_COORDINATES, modis_NRT_mindate, modis_NRT_maxdate)
    viirs_df = fetch_all_data(NASA_KEY, viirs_NRT_name, AREA_COORDINATES, viirs_NRT_mindate, viirs_NRT_maxdate)


    print("MODIS Data:")
    print(modis_df)
    print()
    print()
    print("VIIRS Data:")
    print(viirs_df)

def df_to_JSON():
    modis_df = fetch_all_data(NASA_KEY, modis_NRT_name, AREA_COORDINATES, modis_NRT_mindate, modis_NRT_maxdate)
    viirs_df = fetch_all_data(NASA_KEY, viirs_NRT_name, AREA_COORDINATES, viirs_NRT_mindate, viirs_NRT_maxdate)
    # Save DataFrames to JSON files
    modis_df.to_json('modis_data.json', orient='records', lines=True)
    viirs_df.to_json('viirs_data.json', orient='records', lines=True)

