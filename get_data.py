import pandas as pd
import datetime as dt

import openmeteo_requests
import requests_cache
from retry_requests import retry

# https://open-meteo.com/en/docs/historical-weather-api#start_date=2000-01-01&end_date=2024-07-01&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation,rain,snowfall,snow_depth,weather_code,pressure_msl,surface_pressure,cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,et0_fao_evapotranspiration,vapour_pressure_deficit,wind_speed_10m,wind_speed_100m,wind_direction_10m,wind_direction_100m,wind_gusts_10m,soil_temperature_0_to_7cm,soil_temperature_7_to_28cm,soil_temperature_28_to_100cm,soil_temperature_100_to_255cm,soil_moisture_0_to_7cm,soil_moisture_7_to_28cm,soil_moisture_28_to_100cm,soil_moisture_100_to_255cm,is_day,sunshine_duration&timezone=auto

def get_historical_data(latitude, longtitude, start_date, end_date):
    
    hourly_variables = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature",
                        "precipitation", "rain", "snowfall", "snow_depth", "weather_code",
                        "pressure_msl", "surface_pressure", "cloud_cover", "cloud_cover_low",
                        "cloud_cover_mid", "cloud_cover_high", "et0_fao_evapotranspiration",
                        "vapour_pressure_deficit", "wind_speed_10m", "wind_speed_100m",
                        "wind_direction_10m", "wind_direction_100m", "wind_gusts_10m",
                        "soil_temperature_0_to_7cm", "soil_temperature_7_to_28cm", "soil_temperature_28_to_100cm",
                        "soil_temperature_100_to_255cm", "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm",
                        "soil_moisture_28_to_100cm", "soil_moisture_100_to_255cm", "is_day", "sunshine_duration"]


    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longtitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": hourly_variables,
        "timezone" : "auto"
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # Retrive the data
    hourly = response.Hourly()
    # Construct the dataframe with the datetime column
    hourly_data = {"datetime": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}
    hourly_dataframe = pd.DataFrame(data = hourly_data)

    for index, variable in enumerate(hourly_variables):
        hourly_dataframe[variable] = hourly.Variables(index).ValuesAsNumpy()

    hourly_dataframe.set_index('datetime', inplace=True)

    return hourly_dataframe


if __name__ == "__main__":
    latitude = 37.656
    longtitude = 21.3174
    start_date = "2000-01-01"
    end_date = "2024-07-01"
    data = get_historical_data(latitude, longtitude, start_date, end_date)
    # save data
    text_time = dt.datetime.now().strftime("%Y%m%d_%H-%M")
    path = r'./data/'
    filename = 'historical_data_' + text_time + '.csv'
    data.to_csv(path+filename, sep=';')
    print('data saved')