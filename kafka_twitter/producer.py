import time
from kafka import KafkaProducer, KafkaClient 

# kafka = KafkaClient('localhost:9092')
producer = KafkaProducer(bootstrap_servers='localhost:1234')
topic = 'tweets'

api = TwitterAPI(
    config.consumer_key, 
    config.consumer_secret, 
    config.access_token_key, 
    config.access_token_secret
)


def tweet_emitter(tweet):
	print(' emitting.....')

	r = api.request('statuses/filter', {'locations': '-90,-90,90,90'})

	for item in r: 
		json_item = json.dumps(item)

		producer.send_message(topic, json_item.toBytes())

		time.sleep(0.2)
   
	print('done emitting')

if __name__ == '__main__':
	tweet_emitter(tweet)	

	# bin/kafka-console-producer.sh --broker-list localhost:9092 --topic tweets

