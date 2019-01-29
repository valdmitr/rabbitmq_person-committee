import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='direct_mid', exchange_type='direct')

result = channel.queue_declare(queue='approval')

routing_key = 'committee_mid'

def on_request(ch, method, props, body):
    r = 'ok'
    print(body)
    response = ("{} {} {}".format(props.correlation_id, body, r))

    ch.basic_publish(exchange='direct_mid',
                     routing_key=routing_key,
                     properties=pika.BasicProperties(correlation_id=
                                                     props.correlation_id),
                     body= response)
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(on_request, queue='approval')

print(" [x] Waiting for requests from people")
channel.start_consuming()