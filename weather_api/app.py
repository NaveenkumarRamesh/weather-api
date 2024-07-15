from flask import Flask, jsonify, request
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from bson.json_util import dumps
import datetime
import config

app = Flask(__name__)


@app.route('/weather', methods=['GET'])
def get_weather_by_month_or_date():
    date_str = request.args.get('date')
    month_str = request.args.get('month')
    year_str = request.args.get('year')
    try:
        if date_str and month_str and year_str:
            weather_details = collection.find({"$expr": {"$and": [
                {"$eq": [{"$month": "$datetime"}, int(month_str)]},
                {"$eq": [{"$dayOfMonth": "$datetime"}, int(date_str)]},
                {"$eq": [{"$year": "$datetime"}, int(year_str)]},
            ]
            }
            },
                {
                '_id': 0
            }
            )
        elif date_str and month_str and not year_str:
            weather_details = collection.find({"$expr": {"$and": [
                {"$eq": [{"$month": "$datetime"}, int(month_str)]},
                {"$eq": [{"$dayOfMonth": "$datetime"}, int(date_str)]},
            ]
            }
            },
                {
                '_id': 0
            }
            )
        elif date_str and not month_str and not year_str:
            weather_details = collection.find({
                "$expr": {
                    "$eq": [{"$dayOfMonth": "$datetime"}, int(date_str)]

                }
            },
                {
                '_id': 0
            }
            )
        elif month_str and not date_str and not year_str:
            weather_details = collection.find({
                "$expr": {
                    "$eq": [{"$month": "$datetime"}, int(month_str)]

                }
            },
                {
                '_id': 0
            }
            )
        elif year_str and not date_str and not month_str:
            pipeline = [
                {
                    '$match': {
                        'datetime': {
                            '$gte': datetime.datetime(int(year_str), 1, 1, 0, 0, 0),
                            '$lt': datetime.datetime(int(year_str) + 1, 1, 1, 0, 0, 0)
                        },
                        'temperature_c': {'$exists': True, '$ne': None}
                    }
                },
                {
                    '$group': {
                        '_id': {
                            'year': {'$year': '$datetime'},
                            'month': {'$month': '$datetime'}
                        },
                        # '_id': '$datetime',
                        'high_temp': {'$max': '$temperature_c'},
                        'avg_temp': {'$avg': '$temperature_c'},
                        'min_temp': {'$min': '$temperature_c'}
                    }
                },
                {
                    '$project': {
                        '_id': 0,
                        'year': '$_id.year',
                                'month': '$_id.month',
                                'high_temp': 1,
                                'avg_temp': 1,
                                'min_temp': 1
                    }
                },
                {
                    '$sort': {
                        'month': 1
                    }
                }
            ]
            weather_details = collection.aggregate(pipeline)
        return dumps(list(weather_details))
    except ValueError as e:
        return jsonify({'error': "Please pass the valid parameters date&year&month"}), 400
    except OperationFailure as e:
        return jsonify({'error': f'MongoDB Operation Failure: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    client = MongoClient(
        config.MONGODB_SETTINGS['host'], config.MONGODB_SETTINGS['port'])
    db = client[config.DB_NAME]
    collection = db[config.COLLECTION_NAME]
    app.run()
