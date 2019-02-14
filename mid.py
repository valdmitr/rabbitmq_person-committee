import pika

import helper

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'
))

channel = connection.channel()

# создаем очередь для приема сообщений от комитета
result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

# binding key для приема сообщений от комитета
binding_key = 'committee_mid_mvd'

# создаем binding между точкой обмена direct_mid
# и очередью для приема сообщений от комитета
channel.queue_bind(exchange='direct_mid',
                   queue=queue_name,
                   routing_key=binding_key)


ILLEGAL_IMMIGRANT = ['7a6099e2-bcf8-4b89-8287-9662cc8adbe9',
                     '9348738a-c0c7-4d5f-a646-aa3b261d1ab2',
                     '9670c3b9-8c84-42da-84cd-306d775b9747']


def callback(ch, method, props, body):
    """
    принимаем собщение от комитета, проверяем является ли человек
    нелегальным иммигрантом, если является - кидаем ответ человеку
    сразу, что запрос не прошел. Если не является - отправляем
    ответ обратно комитету.
    """
    print(body.decode())

    if props.correlation_id not in ILLEGAL_IMMIGRANT:

        mid_dict = {'mid': 'ok',
                    'correlation_id': props.correlation_id,
                    'reply_to': props.reply_to,
                    }
        print(mid_dict)

        # записываем файл с данными от мид
        helper.update_file("response_from_mid_{}.json".format(props.correlation_id),
                           body.decode(), mid_dict)

        ch.basic_publish(exchange='',
                         routing_key='from_mid_mvd',
                         properties=pika.BasicProperties(
                             correlation_id=props.correlation_id),
                         body="response_from_mid_{}.json".format(props.correlation_id))
    else:
        message = helper.pack_to_str({
            'response': 'request failed, person {} is '
                        'illegal immigrant'.format(props.correlation_id)})

        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(
                             correlation_id=props.correlation_id),
                         body=message)

    ch.basic_ack(delivery_tag=method.delivery_tag)


print('[*] Waiting for a request from the Committee')

# принимаем запрос от комитета
channel.basic_consume(callback, queue=queue_name)
channel.start_consuming()
