import boto3
import config


if __name__ == '__main__': 
	session = boto3.Session(profile_name = config.profile)
	dynamodb = session.resource('dynamodb')

	table = dynamodb.Table(config.table_name)
	table.delete()