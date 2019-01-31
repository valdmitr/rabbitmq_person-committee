import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'
))

channel = connection.channel()

channel.queue_declare(queue='internal_mvd')
channel.queue_declare(queue='external_mvd')

channel.exchange_declare(exchange='fanout_internal_external',
                         exchange_type='fanout')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue


binding_key = 'committee_mid_mvd'

channel.queue_bind(exchange='direct_mid',
                   queue=queue_name,
                   routing_key=binding_key)

print(' [*] Waiting for a request from the Committee')


def callback(ch, method, props, body):
    response = "{} {}".format("mvd", body.decode())
    print(response)
    ch.basic_publish(exchange='fanout_internal_external',
                          routing_key='',
                          properties=pika.BasicProperties(correlation_id=
                                                     props.correlation_id,
                                                     reply_to=props.reply_to),
                          body=body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)


def internal_request(ch, method, props, body):
    response = "Internal ok {}".format(body.decode())
    print(response)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def external_request(ch, method, props, body):
    response = "External ok {}".format(body.decode())
    print(response)
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(callback, queue=queue_name)
channel.basic_consume(internal_request, queue='internal_mvd')
channel.basic_consume(external_request, queue='external_mvd')

channel.start_consuming()