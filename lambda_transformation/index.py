import json
import base64
from datetime import datetime
import pytz

eastern = pytz.timezone('US/Eastern')


def handler(event, context):
    output = []

    for record in event['records']:
        payload = json.loads(base64.b64decode(record['data']))

        transformed_payload = {
            'ticker_symbol': payload['ticker_symbol'],
            'sector': payload['sector'],
            'change': payload['change'],
            'price': payload['price'],
            'trend': 'up' if payload['change'] > 0 else ('down' if payload['change'] < 0 else 'level'),
            'timestamp': round(datetime.now().astimezone(eastern).timestamp())
        }

        result = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(json.dumps(transformed_payload).encode()).decode("utf-8")
        }
        output.append(result)

    return {'records': output}