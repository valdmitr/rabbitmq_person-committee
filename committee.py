import pika
import os
import json


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='direct_mid', exchange_type='direct') # создаем точку обмена для отправки сообщений в мид и мвд

channel.queue_declare(queue='approval') # создаем очередь для отправки запросов

routing_key = 'committee_mid_mvd' # routing key для точки обмена direct_mid

for_mid = channel.queue_declare(queue='from_mid_mvd')  # очередь для приема ответов от мид и мвд

def on_request(ch, method, props, body):
    """
    callback-функция для приема запрсов от людей,
    тут отправляем запрос в мид и мвд
    """
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
    """
    callback-функция для приема ответов от мид и мвд,
    отправляем ответ человеку на первоначальный запрос
    """
    if os.path.isfile('./response_from_mid.json') and os.path.isfile('./response_from_mvd.json'):
        with open("response_from_mvd.json", "r") as read_file:
            data_to_publish = json.load(read_file)
            body_to_publish = "Ok from mid and mvd {}".format(data_to_publish['body'])
            ch.basic_publish(exchange='',
                             routing_key=data_to_publish['reply_to'],
                             properties=pika.BasicProperties(correlation_id=
                                                             data_to_publish['correlation_id']),
                             body=body_to_publish)
        os.remove('./response_from_mid.json')
        os.remove('./response_from_mvd.json')
        # ch.basic_publish(exchange='',
        #              routing_key=props.reply_to,
        #              properties=pika.BasicProperties(correlation_id=props.correlation_id),
        #              body=body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(on_request, queue='approval')

channel.basic_consume(mid_request, queue='from_mid_mvd')

print("[x] Waiting for requests from people")
channel.start_consuming()