import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='direct_mid', exchange_type='direct')

channel.queue_declare(queue='approval')

routing_key = 'committee_mid_mvd'

for_mid = channel.queue_declare(queue='from_mid')

def on_request(ch, method, props, body):
    body = body.decode()
    r = 'ok'
    print(body)

    response = ("{} {}".format(body, r))

    list_of_body = body.split(' ')

    if list_of_body[0] == 'person':

        ch.basic_publish(exchange='direct_mid',
                         routing_key=routing_key,
                         properties=pika.BasicProperties(correlation_id=
                                                     props.correlation_id,
                                                         reply_to=props.reply_to),
                         body= str(response))


    ch.basic_ack(delivery_tag=method.delivery_tag)

def mid_request(ch, method, props, body):
    print("Hello", body.decode())
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=props.correlation_id),
                     body=body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(on_request, queue='approval')

channel.basic_consume(mid_request, queue='from_mid')

print(" [x] Waiting for requests from people")
channel.start_consuming()