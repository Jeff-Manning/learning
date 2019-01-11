import boto3 
import config


if __name__ == '__main__':
	session = boto3.Session(profile_name = config.profile)
	kinesis_client = session.client('kinesis')

	response = kinesis_client.delete_stream(
		StreamName = config.stream_name,
		EnforceConsumerDeletion = True
	)