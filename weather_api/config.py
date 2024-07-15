# config.py

MONGODB_SETTINGS = {
    'host': 'localhost',
    'port': 27017,
}
DB_NAME = "genai_studio"
COLLECTION_NAME = "weather_data"
WEATHER_DATA_FILE = "/home/navvenramesh/DelhiWeatherAPI/data/Weather Data.csv"

RENAMED_COLUMNS = {
    'datetime_utc': 'datetime',
    '_conds': 'conditions',
    '_dewptm': 'dew_point_c',
    '_fog': 'fog',
    '_hail': 'hail',
    '_heatindexm': 'heat_index_c',
    '_hum': 'humidity',
    '_precipm': 'precipitation_mm',
    '_pressurem': 'pressure_mb',
    '_rain': 'rain',
    '_snow': 'snow',
    '_tempm': 'temperature_c',
    '_thunder': 'thunder',
    '_tornado': 'tornado',
    '_vism': 'visibility_km',
    '_wdird': 'wind_dir_degrees',
    '_wdire': 'wind_direction',
    '_wgustm': 'wind_gust_kph',
    '_windchillm': 'wind_chill_c',
    '_wspdm': 'wind_speed_kph'
}
