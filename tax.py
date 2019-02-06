import pika
import json
import uuid

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='committee_tax') #создаем очередь для приема сообщений от комитета

def callback(ch, method, props, body):
    """
    принимаем собщение от комитета,
    отправляем ответ обратно комитету
    """
    print(body.decode())
    resp_tax = json.dumps({'taxpayer_id': str(uuid.uuid4())})

    ch.basic_publish(exchange='',
                     routing_key='from_tax',
                     properties=pika.BasicProperties(correlation_id=
                                                     props.correlation_id,
                                                     reply_to=props.reply_to),
                     body=resp_tax)

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(callback, queue='committee_tax')

print('[*] Waiting for a request from the Committee')

channel.start_consuming()