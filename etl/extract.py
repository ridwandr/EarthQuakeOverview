import pandas as pd
import requests
import logging
from datetime import datetime, timezone

def parse_geojson(data: dict) -> pd.DataFrame:
    """
    Parse a GeoJSON response from USGS API into a pandas DataFrame.

    Parameters:
        data (dict): JSON object returned by USGS GeoJSON API.

    Returns:
        pd.DataFrame: Flattened earthquake event data with relevant fields.
    """
    records = []

    for feature in data['features']:
        props = feature['properties']
        coords = feature['geometry']['coordinates']  # [lon, lat, depth]

        record = {
            "id": feature.get("id"),
            "place": props.get("place"),
            "mag": props.get("mag"),
            "time": pd.to_datetime(props.get("time"), unit='ms'),
            "updated": pd.to_datetime(props.get("updated"), unit='ms'),
            "tz": props.get("tz"),
            "felt": props.get("felt"),
            "cdi": props.get("cdi"),
            "mmi": props.get("mmi"),
            "alert": props.get("alert"),
            "status": props.get("status"),
            "tsunami": props.get("tsunami"),
            "sig": props.get("sig"),
            "net": props.get("net"),
            "code": props.get("code"),
            "ids": props.get("ids"),
            "sources": props.get("sources"),
            "types": props.get("types"),
            "longitude": coords[0],
            "latitude": coords[1],
            "depth": coords[2],
            "fetched_at": datetime.now(timezone.utc)
        }
        records.append(record)

    return pd.DataFrame(records)


def fetch_earthquake_all_day() -> pd.DataFrame:
    """
    Fetch real-time earthquake data from the past 24 hours.

    Returns:
        pd.DataFrame: Earthquake data from the /all_day.geojson feed.
    """
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
    try:
        response = requests.get(url)
        response.raise_for_status()
        logging.info("Fetched all_day data successfully.")
        return parse_geojson(response.json())
    except Exception as e:
        logging.error(f"Error fetching all_day data: {e}")
        return pd.DataFrame()
    
def fetch_earthquake_past_hour() -> pd.DataFrame:
    """
    Fetch real-time earthquake data from the past 1 hour.

    Returns:
        pd.DataFrame: Earthquake data from the /all_hour.geojson feed.
    """
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
    try:
        response = requests.get(url)
        response.raise_for_status()
        logging.info("Fetched all_hour data successfully.")
        return parse_geojson(response.json())
    except Exception as e:
        logging.error(f"Error fetching all_hour data: {e}")
        return pd.DataFrame()


def fetch_earthquake_historical_daily(start_date: str, end_date: str, min_magnitude: float = 0.0) -> pd.DataFrame:
    """
    Fetch historical earthquake data by full-day range using USGS query API.

    Parameters:
        start_date (str): Start date in YYYY-MM-DD format.
        end_date (str): End date in YYYY-MM-DD format.
        min_magnitude (float): Minimum magnitude filter (default 0.0).

    Returns:
        pd.DataFrame: Earthquake data from the specified date range.
    """
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": start_date,
        "endtime": end_date,
        "minmagnitude": min_magnitude,
        "orderby": "time",
        "limit": 20000
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        logging.info(f"Fetched historical daily data from {start_date} to {end_date}.")
        return parse_geojson(response.json())
    except Exception as e:
        logging.error(f"Error fetching historical daily data: {e}")
        return pd.DataFrame()
    
def fetch_earthquake_historical_hour(start_dt: datetime, end_dt: datetime, min_magnitude: float = 0.0) -> pd.DataFrame:
    """
    Fetch historical earthquake data by hourly range using USGS query API.

    Parameters:
        start_dt (datetime): Start timestamp.
        end_dt (datetime): End timestamp.
        min_magnitude (float): Minimum magnitude filter (default 0.0).

    Returns:
        pd.DataFrame: Earthquake data from the specified hourly range.
    """
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": start_dt.isoformat(),
        "endtime": end_dt.isoformat(),
        "minmagnitude": min_magnitude,
        "orderby": "time",
        "limit": 20000
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        logging.info(f"Fetched historical hourly data from {start_dt} to {end_dt}.")
        return parse_geojson(response.json())
    except Exception as e:
        logging.error(f"Error fetching historical hourly data: {e}")
        return pd.DataFrame()