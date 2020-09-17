import time
import boto3
import json

DATABASE = 'test'
TABLE = 'ticker_transformed_output'

S3_OUTPUT = 's3://com.wicaksana.bigdata/athena_output'
RETRY_COUNT = 3

def handler(event, context):    
    start = float(event['params']['querystring']['from'])
    stop = float(event['params']['querystring']['to'])

    # create query
    query = f'SELECT * FROM {DATABASE}.{TABLE} WHERE timestamp >= {start} AND timestamp <= {stop}'

    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString = query,
        QueryExecutionContext = {
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': S3_OUTPUT,
        }
    )

    # get query execution id
    query_execution_id = response['QueryExecutionId']
    print(query_execution_id)

    # get execution status
    for i in range(1, 1 + RETRY_COUNT):
        # get query execution
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']

        if query_execution_status == 'SUCCEEDED':
            print("STATUS:" + query_execution_status)
            break
        
        if query_execution_status == 'FAILED':
            raise Exception("STATUS:" + query_execution_status)

        else:
            print("STATUS:" + query_execution_status)
            time.sleep(i)

    else:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        raise Exception('TIME OVER')

    # get query results
    result = client.get_query_results(QueryExecutionId=query_execution_id)
    
    returned_result = []
    query_results = result['ResultSet']['Rows']

    if len(query_results) > 1:
        for res in query_results[1:]:
            item = {
                'ticker_symbol': res['Data'][0]['VarCharValue'],
                'sector': res['Data'][1]['VarCharValue'],
                'change': float(res['Data'][2]['VarCharValue']),
                'price': float(res['Data'][3]['VarCharValue']),
                'trend': res['Data'][4]['VarCharValue'],
                'timestamp': float(res['Data'][5]['VarCharValue'])
            }
            returned_result.append(item)
    
    return returned_result
