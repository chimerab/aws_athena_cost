import boto3
import csv
import json
import math
import sys

def get_price(region_code: str, product_name: str) -> float:
    # pricing api only available at us-east-1
    client = boto3.client('pricing', region_name='us-east-1')
    filters = [
        {
            'Type': 'TERM_MATCH',
            'Field': 'ServiceCode',
            'Value': product_name
        },
        {
            'Type': 'TERM_MATCH',
            'Field': 'regionCode',
            'Value': region_code
        },
        {
            'Type': 'TERM_MATCH',
            'Field': 'termType',
            'Value': 'OnDemand'
        },
    ]

    resp = client.get_products(ServiceCode=product_name, Filters=filters, FormatVersion='aws_v1', MaxResults=1, )
    if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
        print(resp)
        return None
    # print(json.dumps(resp, indent=4))
    od_str = json.loads(resp['PriceList'][0])['terms']['OnDemand']
    od_keya = list(od_str)[0]
    od_keyb = list(od_str[od_keya]['priceDimensions'])[0]

    return float(od_str[od_keya]['priceDimensions'][od_keyb]['pricePerUnit']['USD'])


def get_query_history(client, workgroup='primary'):
    result = []
    resp = client.list_query_executions(MaxResults=50, WorkGroup=workgroup)
    if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
        print(resp)
        return None

    result.extend(resp['QueryExecutionIds'])
    while 'NextToken' in resp:
        next_token = resp['NextToken']
        resp = client.list_query_executions(MaxResults=50, WorkGroup=workgroup, NextToken=next_token)
        if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            print(resp)
            return None
        result.extend(resp['QueryExecutionIds'])

    return result


def main():
    """
    Athena keeps a query history for 45 days.
    :return:
    """
    if len(sys.argv) != 2:
        print('Please input region code as parameter. ex: ap-northeast-1')
        sys.exit(1)

    region = sys.argv[1]
    print(f'checking athena cost for region: {region}')
    athena = boto3.client('athena', region_name=region)
    result = get_query_history(athena)
    if result is None:
        print('Failed to get athena execution history, see error above. ')

    price = get_price(region, 'AmazonAthena')

    count = 0
    total_cost = 0
    with open('athena_cost_analysis.csv', 'w', newline='') as fp:
        writer = csv.writer(fp)
        writer.writerow(['QueryExecutionId', 'Query', 'WorkGroup', 'StatementType', 'Database', 'State',
                         'SubmissionDateTime', 'CompletionDateTime', 'DataScannedInBytes', 'TotalExecutionTimeInMillis',
                         'price($)'])
        for each in result:
            resp = athena.get_query_execution(QueryExecutionId=each)
            if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                print(resp)
                print(f'unable to get detail execution for {each}')
            query_type = resp['QueryExecution']['StatementType']
            query_status = resp['QueryExecution']['Status']['State']
            data_amount = resp['QueryExecution']['Statistics']['DataScannedInBytes'] if query_status != 'FAILED' else 0
            calculated_price = 0
            if query_type != 'DML' or query_status == 'FAILED':
                calculated_price = 0
            elif data_amount != 0:
                # You are charged for the number of bytes scanned by Amazon Athena,
                # rounded up to the nearest megabyte, with a 10MB minimum per query.
                # There are no charges for Data Definition Language (DDL) statements like CREATE/ALTER/DROP TABLE,
                # statements for managing partitions, or failed queries. Cancelled queries are charged based
                # on the amount of data scanned.
                # the price unit is 1 Terabytes
                roundup_price = int(math.ceil(data_amount / 10000000)) * 10000000
                calculated_price = roundup_price / 1000000000 * price

            record = [
                resp['QueryExecution']['QueryExecutionId'],
                resp['QueryExecution']['Query'],
                resp['QueryExecution']['WorkGroup'],
                query_type,
                resp['QueryExecution']['QueryExecutionContext']['Database'],
                # resp['QueryExecution']['QueryExecutionContext']['Catalog'],
                query_status,
                resp['QueryExecution']['Status']['SubmissionDateTime'].isoformat(),
                resp['QueryExecution']['Status']['CompletionDateTime'].isoformat(),
                # resp['QueryExecution']['Statistics']['EngineExecutionTimeInMillis'],
                data_amount,
                resp['QueryExecution']['Statistics']['TotalExecutionTimeInMillis'],
                # resp['QueryExecution']['Statistics']['QueryQueueTimeInMillis'],
                # resp['QueryExecution']['Statistics']['ServiceProcessingTimeInMillis']
                calculated_price
            ]
            print(record)

            writer.writerow(record)
            count += 1
            total_cost += calculated_price

    print(f'\n{count} athena query been found. the approximate cost is ${round(total_cost, 2)}')

if __name__ == '__main__':
    main()
