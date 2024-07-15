import unittest
import pandas as pd
from pymongo import MongoClient
from weather_api import config
import os
from io import StringIO
from contextlib import redirect_stdout
from weather_api.etl_process import extract_weather_data, transform_weather_data, load_into_mongodb


class TestWeatherDataProcessing(unittest.TestCase):

    def setUp(self):
        self.test_data = """
        datetime_utc,_conds,_dewptm,_fog,_hail,_heatindexm,_hum,_precipm,_pressurem,_rain,_snow,_tempm,_thunder,_tornado,_vism,_wdird,_wdire,_wgustm,_windchillm,_wspdm
        20230101-00:30,Fog,9.0,1,0,27.1,87.0,0.0,1015.0,0.0,0.0,17.0,0.0,0.0,1.2,0.0,SE,24.1,15.2,7.4
        20230101-01:30,Fog,8.0,1,0,27.2,88.0,0.0,1015.1,0.0,0.0,17.1,0.0,0.0,1.3,0.0,SSE,24.2,15.3,7.5
        """

        client = MongoClient(
            config.MONGODB_SETTINGS['host'], config.MONGODB_SETTINGS['port'])
        self.test_file_path = 'test_weather_data.csv'
        db = client[config.DB_NAME]
        self.collection = db['test_collection']
        self.collection.delete_many({})

        with open(self.test_file_path, 'w') as f:
            f.write(self.test_data)

    def tearDown(self):
        if os.path.exists(self.test_file_path):
            os.remove(self.test_file_path)

    def test_extract_weather_data(self):
        extracted_data = extract_weather_data(self.test_file_path)
        self.assertIsInstance(extracted_data, pd.DataFrame)
        self.assertEqual(len(extracted_data), 2)

        non_existent_file = 'non_existent_file.csv'
        extracted_data = extract_weather_data(non_existent_file)
        self.assertIsNone(extracted_data)

        empty_data_file = 'empty_data.csv'
        with open(empty_data_file, 'w') as f:
            pass
        extracted_data = extract_weather_data(empty_data_file)
        self.assertIsNone(extracted_data)

    def test_transform_weather_data(self):
        sample_data = pd.read_csv(StringIO(self.test_data))

        transformed_data = transform_weather_data(sample_data)
        self.assertIsInstance(transformed_data, pd.DataFrame)
        self.assertEqual(len(transformed_data), 2)

        sample_data_missing_cols = sample_data.drop(columns=['_pressurem'])
        transformed_data = transform_weather_data(sample_data_missing_cols)
        self.assertIsNone(transformed_data)

    def test_load_into_mongodb(self):
        sample_data = pd.read_csv(StringIO(self.test_data))
        transformed_data = transform_weather_data(sample_data)
        try:
            with redirect_stdout(open(os.devnull, 'w')):
                load_into_mongodb(transformed_data,self.collection, 'test_collection')

            count = self.collection.count_documents({})
            self.assertEqual(count, 2)
        except Exception as e:
            self.fail(f"MongoDB insertion failed: {str(e)}")
