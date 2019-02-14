import pika

import helper

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()

# очередь для приема сообщений от мвд
result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

# создаем связку между точкой обмена fanout и очередью для приема сообщ от мвд
channel.queue_bind(exchange='fanout_internal_external',
                   queue=queue_name)


BAD_PERSON_INTERNAL = {
    'c2772114-b159-402c-9e6c-ffdd35a7ad9e': 'kidnapping'
}


def callback(ch, method, props, body):
    """
    принимаем собщение от мвд, проверяем есть ли человек во
    внутренних базах. Если да, то кидаем отказ человеку,
    если нет, отправляем ответ обратно мвд.
    """
    if props.correlation_id not in BAD_PERSON_INTERNAL:
        print(body.decode())

        ch.basic_publish(exchange='',
                         routing_key='internal_mvd',
                         properties=pika.BasicProperties(
                             correlation_id=props.correlation_id,
                             reply_to=props.reply_to),
                         body=body.decode())
    else:
        message = helper.pack_to_str({'response': 'request failed, person {} is criminal (internal db)'.format(props.correlation_id)})

        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(
                             correlation_id=props.correlation_id),
                         body=message)


    ch.basic_ack(delivery_tag=method.delivery_tag)


print(' [*] Waiting for a request from mvd')

# принимаем запрос от мвд
channel.basic_consume(callback, queue=queue_name)
channel.start_consuming()
