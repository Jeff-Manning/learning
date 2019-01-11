import boto3
import config
import time
import json


if __name__ == '__main__':
    session = boto3.Session(profile_name = config.profile)
    kinesis_client = session.client('kinesis')


    # only one shard
    shard_id = 'shardId-000000000000'
    pre_shard_it = kinesis_client.get_shard_iterator(
        StreamName = config.stream_name,
        ShardId = shard_id,
        ShardIteratorType = 'LATEST'
    )

    shard_it = pre_shard_it['ShardIterator']


    while True: 
        out = kinesis_client.get_records(
            ShardIterator = shard_it, 
            Limit = 1
        )

        shard_it = out['NextShardIterator']
        print(out)
        time.sleep(1.0)