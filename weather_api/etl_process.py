from pymongo import MongoClient
import pandas as pd
import numpy as np
import config


def extract_weather_data(file_path):
    try:
        weather_data = pd.read_csv(file_path)
        return weather_data
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None
    except pd.errors.EmptyDataError:
        print(f"Error: File '{file_path}' is empty.")
        return None
    except Exception as e:
        print(f"Error: Failed to load data from '{file_path}'.")
        print(str(e))
        return None


def transform_weather_data(weather_data):
    try:
        weather_data.columns = weather_data.columns.str.replace(' ', '')
        weather_data = weather_data.drop_duplicates()
        weather_data = weather_data.dropna(subset=["_tempm"])
        columns_to_replace = ['_pressurem', '_precipm',
                              '_heatindexm', '_wgustm', '_windchillm']
        for col in columns_to_replace:
            weather_data[col] = weather_data[col].replace(-9999.0, 0)

        weather_data = weather_data.rename(columns=config.RENAMED_COLUMNS)
        weather_data.datetime = weather_data.datetime.str.replace(' ', '')
        weather_data['datetime'] = pd.to_datetime(
            weather_data['datetime'], format='%Y%m%d-%H:%M')

        return weather_data
    except Exception as e:
        print("Error occurred during data transformation:")
        print(str(e))
        return None


def load_into_mongodb(data, collection, collection_name):

    data_dicts = data.to_dict(orient='records')
    for data_dict in data_dicts:
        query = {'datetime': data_dict['datetime']}
        existing_doc = collection.find_one(query)
        if existing_doc:
            print("Document already exists. Skipping insertion.")
        else:
            try:
                result = collection.insert_one(data_dict)
                print(f"Document inserted with _id: {result.inserted_id}")
            except Exception as e:
                print(f"Failed to insert document: {e}")
                print(
                    f"Successfully inserted {len(data_dict)} documents into MongoDB collection '{collection_name}'.")


def start_up():
    weather_data = extract_weather_data(config.WEATHER_DATA_FILE)

    if weather_data is not None:
        weather_data = transform_weather_data(weather_data)

        if weather_data is not None:
            client = MongoClient(
                config.MONGODB_SETTINGS['host'], config.MONGODB_SETTINGS['port'])
            db = client[config.DB_NAME]
            collection = db[config.COLLECTION_NAME]
            index_exists = False
            index_fields = "datetime"
            try:
                index_info = collection.index_information()
                for index in index_info:
                    if index_info[index]['key'][0][0] == index_fields:
                        index_exists = True
                        break
            except Exception as e:
                print("Due to permissions or connectivity issues")
                print(str(e))

            if not index_exists:
                try:
                    collection.create_index(
                        [(index_fields, 1)], unique=True, sparse=True)
                    print(f"Index {index_fields} created successfully.")
                except Exception as e:
                    print(f"Failed to create index: {e}")
            load_into_mongodb(weather_data, collection, config.COLLECTION_NAME)


start_up()
