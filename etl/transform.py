import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from pytz import timezone
import numpy as np

WIB = timezone('Asia/Jakarta')

def clean_earthquake_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform raw earthquake data:
    - Drop duplicates and nulls in essential columns
    - Standardize data types
    - Select only relevant columns

    Parameters:
        df (pd.DataFrame): Raw DataFrame from extract module

    Returns:
        pd.DataFrame: Cleaned DataFrame with selected columns
    """

    if df.empty:
        return df

    # Drop duplicate records
    df = df.drop_duplicates(subset=["id"])

    # Drop rows with null magnitude or location
    df = df.dropna(subset=["mag", "place", "latitude", "longitude", "time"])

    # Convert time columns to datetime if not already
    # Convert UTC time columns to datetime with Asia/Jakarta timezone
    df['time'] = pd.to_datetime(df['time'], errors='coerce').dt.tz_localize('UTC').dt.tz_convert(WIB)
    df['updated'] = pd.to_datetime(df['updated'], errors='coerce').dt.tz_localize('UTC').dt.tz_convert(WIB)
    df['fetched_at'] = pd.to_datetime(df['fetched_at'], errors='coerce').dt.tz_convert('UTC').dt.tz_convert(WIB)

    # Filter only valid magnitudes
    df = df[df['mag'] >= 0]

    # Select relevant columns
    selected_columns = [
        "id", "place", "mag", "time", "updated",
        "latitude", "longitude", "depth",
        "tsunami", "sig", "status", "alert", "types",
        "felt", "cdi", "mmi", "fetched_at"
    ]
    df = df[selected_columns]

    return df

def enrich_earthquake_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived columns to enhance the data:
    - mag_category: classify magnitude
    - is_significant: boolean for mag >= 5.5
    - day_of_week: for time-based trend
    - hour_of_day: time-based grouping

    Parameters:
        df (pd.DataFrame): Cleaned DataFrame

    Returns:
        pd.DataFrame: Enriched DataFrame
    """

    if df.empty:
        return df

    def classify_magnitude(mag):
        if mag < 2.0:
            return "Micro"
        elif mag < 4.0:
            return "Minor"
        elif mag < 5.5:
            return "Light"
        elif mag < 7.0:
            return "Moderate"
        elif mag < 8.0:
            return "Strong"
        else:
            return "Major"
    
    def get_address_detail(latitude, longitude):
        result = {}
        coder = Nominatim(user_agent="myGeocoder", timeout=5)
        rate_limit = RateLimiter(coder.reverse, 
                                 min_delay_seconds=1,
                                 max_retries=5,
                                 )
        
        address = rate_limit(f"{latitude}, {longitude}")

        result["city"] = address.raw.get("address").get("city") if address else np.nan
        result["state"] = address.raw.get("address").get("state") if address else np.nan
        result["country"] = address.raw.get("address").get("country") if address else np.nan
        return result

    # Add magnitude category
    df['mag_category'] = df['mag'].apply(classify_magnitude)
    df['is_significant'] = df['mag'] >= 5.5
    # Add address details
    address_detail = df.apply(lambda row: get_address_detail(row['latitude'], row['longitude']), axis=1, result_type='expand')
    df = pd.concat([df, address_detail], axis=1)
    # Add time-based columns
    df['day_of_week'] = df['time'].dt.day_name()
    df['hour_of_day'] = df['time'].dt.hour

    return df
