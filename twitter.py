import boto3
import config
import json

from TwitterAPI import TwitterAPI


def ingest_tweets(r):
    # We're only using a single shard for this demo. We use a 'filler' partition
    # key so that every tweet is assigned the same shard. We also batch
    # ingestion in sets of 100. 

    tweets = []
    tweet_count = 0

    for item in r: 
        json_item = json.dumps(item)

        tweets.append({
            'Data' : json_item, 
            'PartitionKey' : 'filler'
        })
        tweet_count += 1

        if tweet_count == 100:
            kinesis_client.put_records(
                StreamName = config.stream_name, 
                Records = tweets
            )
            tweet_count = 0
            tweets = []


if __name__ == '__main__':
    session = boto3.Session(profile_name = config.profile)
    kinesis_client = session.client('kinesis')

    api = TwitterAPI(
        config.consumer_key, 
        config.consumer_secret, 
        config.access_token_key, 
        config.access_token_secret
    )

    r = api.request('statuses/filter', {'locations': '-90,-90,90,90'})
    ingest_tweets(r)
