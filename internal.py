import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()


result = channel.queue_declare(exclusive=True) # создаем очередь для приема сообщений от мвд
queue_name = result.method.queue

channel.queue_bind(exchange='fanout_internal_external',
                   queue=queue_name) # создаем binding между точкой доступа fanout и очередью для приема сообщений от мвд

print(' [*] Waiting for a request from mvd')

def callback(ch, method, props, body):
    """
    принимаем собщение от мвд,
    отправляем ответ обратно мвд
    """
    print(body.decode())
    ch.basic_publish(exchange='',
                     routing_key='internal_mvd',
                     properties=pika.BasicProperties(correlation_id=
                                                     props.correlation_id,
                                                     reply_to=props.reply_to),
                     body=body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(callback, queue=queue_name)

channel.start_consuming()