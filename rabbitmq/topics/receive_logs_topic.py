import pika
import sys 


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(
	exchange='topic_logs',
	exchange_type='topic'
)

results = channel.queue_declare(exclusive=True)
queue_name = results.method.queue 

binding_keys = sys.argv[1:]
if not binding_keys: 
	sys.stderr.write('Usage: %s [binding_key]...\b' % sys.argv[0])
	sys.exit(1)

for binding_key in binding_keys:
	channel.queue_bind(
		exchange='topic_logs',
		queue=queue_name, 
		routing_key=binding_key
	)

print(' [*] Waiting for logs. To exist press CTRL+C')


def callback(ch, method, prpoerties, body):
	print(' [x] %r:%r' %(method.routing_key, body))


channel.basic_consume(
	callback, 
	queue=queue_name, 
	no_ack=True
)

channel.start_consuming()
