
import requests
import json
from requests.exceptions import HTTPError, RequestException
from config import config


class WeatherHelper:

    session = None

    def __init__(self):
        pass

    def get_current_weather_from_api(self, lat, lon):
        
        url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={config.OPEN_WEATHER_API_KEY}"

        try:
            response = requests.get(url=url, timeout=30)
            response_json = response.json()

            if response.status_code == 200:
                results = response_json
                return results

            else:
                message = f"Error when getting weather data from Open Weather API, status_code: {response.status_code}, error: {response.text}"
                raise HTTPError(message)
        except RequestException as e:
            message = f"Error when getting weather data from Open Weather API, error: {e}"
            raise RequestException(message)
