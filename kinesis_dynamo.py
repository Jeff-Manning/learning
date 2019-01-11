import boto3
import config
import time
import json
import decimal


def store_in_dynamodb(shard_it):
    while True: 
        out = kinesis_client.get_records(ShardIterator = shard_it, Limit = 100)

        for record in out['Records']:
            if 'entities' in json.loads(record['Data']):
                htags = json.loads(record['Data'])['entities']['hashtags']

                if htags: 
                    for ht in htags: 
                        htag = ht['text']

                        check_item_exists = table.get_item(
                            Key = {'hashtag' : htag}
                        )

                        if 'Item' in check_item_exists:
                            response = table.update_item(
                                Key = { 'hashtag' : htag },
                                UpdateExpression = 'set htCount = htCount + :val',
                                ConditionExpression = 'attribute_exists(hashtag)',
                                ExpressionAttributeValues = {
                                    ':val' : decimal.Decimal(1)
                                },
                                ReturnValues = 'UPDATED_NEW'
                            )
                        else:
                            response = table.update_item(
                                Key = { 'hashtag' : htag }, 
                                UpdateExpression = 'set htCount = :val', 
                                ExpressionAttributeValues = {
                                    ':val' : decimal.Decimal(1)
                                }, 
                                ReturnValues = 'UPDATED_NEW'
                            )
        shard_it = out['NextShardIterator']
        time.sleep(1.0)


if __name__ == '__main__': 
    session = boto3.Session(profile_name = config.profile)
    kinesis_client = session.client('kinesis')

    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table(config.table_name)

    # Only one shard
    shard_id = 'shardId-000000000000'
    shard_it = kinesis_client.get_shard_iterator(
        StreamName = config.stream_name, 
        ShardId = shard_id, 
        ShardIteratorType = 'LATEST'
    )['ShardIterator']

    store_in_dynamodb(shard_it)