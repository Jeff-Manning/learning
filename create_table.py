import boto3 
import config


if __name__ == '__main__':
    session = boto3.Session(profile_name = config.profile)
    dynamodb = session.resource('dynamodb')

    hashtag_table = dynamodb.create_table(
        TableName = config.table_name, 
        
        KeySchema = [{
            'AttributeName' : 'hashtag', 
            'KeyType' : 'HASH'
        }], 
        
        AttributeDefinitions = [{
            'AttributeName' : 'hashtag',
            'AttributeType' : 'S'
        }],

        # Pricing determined by ProvisionedThroughput
        ProvisionedThroughput = {
            'ReadCapacityUnits' : 5, 
            'WriteCapacityUnits' : 5
        }
    )

    hashtag_table.meta.client.get_waiter('table_exists').wait(TableName = 'hashtags')