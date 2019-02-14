import pika
import uuid

import helper

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()

# создаем очередь для приема сообщений от комитета
channel.queue_declare(queue='committee_tax')

EVADER = ['b1af25d2-dd3d-41b9-b53c-7e8cc0abc145']


def callback(ch, method, props, body):
    """
    принимаем собщение от комитета,
    отправляем ответ обратно комитету
    """
    print(body.decode())

    if props.correlation_id not in EVADER:
        resp_tax = helper.pack_to_str({'taxpayer_id': str(uuid.uuid4())})

        ch.basic_publish(exchange='',
                         routing_key='from_tax',
                         properties=pika.BasicProperties(
                             correlation_id=props.correlation_id,
                             reply_to=props.reply_to),
                         body=resp_tax)
    else:
        message = helper.pack_to_str({
            'response': 'request failed, person {} is '
                        'tax evader'.format(props.correlation_id)
        })

        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(
                             correlation_id=props.correlation_id),
                         body=message)

    ch.basic_ack(delivery_tag=method.delivery_tag)


print('[*] Waiting for a request from the Committee')

channel.basic_consume(callback, queue='committee_tax')
channel.start_consuming()
