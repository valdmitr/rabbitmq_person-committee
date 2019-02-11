import pika

import helper

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'
))

channel = connection.channel()

# создаем очередь для приема сообщений от комитета
channel.queue_declare(queue='committee_social')

def callback(ch, method, props, body):
    """
    принимаем собщение от комитета,
    отправляем ответ обратно комитету
    """
    print(body.decode())
    response = helper.pack_dict_to_json(body.decode(), {'social security': 'ok'})

    ch.basic_publish(exchange='',
                     routing_key='from_social_sec',
                     properties=pika.BasicProperties(correlation_id=
                                                     props.correlation_id,
                                                     reply_to=props.reply_to),
                     body=response)

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(callback, queue='committee_social')

print('[*] Waiting for a request from the Committee')

channel.start_consuming()