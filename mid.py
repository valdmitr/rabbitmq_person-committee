import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'
))

channel = connection.channel()

channel.exchange_declare(exchange='direct_mid', exchange_type='direct')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

binding_key = 'committee_mid'

channel.queue_bind(exchange='direct_mid',
                   queue=queue_name,
                   routing_key=binding_key)

print(' [*] Waiting for a request from the Committee')

def callback(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body))

channel.basic_consume(callback, queue=queue_name, no_ack=True)

channel.start_consuming()