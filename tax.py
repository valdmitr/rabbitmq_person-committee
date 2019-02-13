import pika
import uuid

import helper

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()

# создаем очередь для приема сообщений от комитета
channel.queue_declare(queue='committee_tax')


def callback(ch, method, props, body):
    """
    принимаем собщение от комитета,
    отправляем ответ обратно комитету
    """
    print(body.decode())
    resp_tax = helper.pack_to_str({'taxpayer_id': str(uuid.uuid4())})

    ch.basic_publish(exchange='',
                     routing_key='from_tax',
                     properties=pika.BasicProperties(
                         correlation_id=props.correlation_id,
                         reply_to=props.reply_to),
                     body=resp_tax)

    ch.basic_ack(delivery_tag=method.delivery_tag)


print('[*] Waiting for a request from the Committee')

channel.basic_consume(callback, queue='committee_tax')
channel.start_consuming()
