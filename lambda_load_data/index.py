import boto3
import base64
import json
import random
import string

def handler(event, context):
    client = boto3.client('firehose')

    sectors = ['FINANCIAL', 'TECHNOLOGY', 'RETAIL', 'ENERGY', 'HEALTHCARE']

    records = []

    for i in range(10):        
        payload = {
                'ticker_symbol': ''.join(random.choices(string.ascii_uppercase, k=3)),
                'sector': sectors[random.randint(0, len(sectors) - 1)],
                'change': round(random.uniform(-8, 8), 2),
                'price': round(random.uniform(1, 150), 2)
        }
        record = {
            'Data': json.dumps(payload).encode('utf-8')
        }
        records.append(record)

    response = client.put_record_batch(
        DeliveryStreamName='cf-infra-firehose-delivery-stream',
        Records=records
    )
    
    # best-effort
    return 'OK'        
