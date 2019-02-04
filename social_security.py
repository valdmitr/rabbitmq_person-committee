import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'
))

channel = connection.channel()

channel.queue_declare(queue='committee_social') #создаем очередь для приема сообщений от комитета

def callback(ch, method, props, body):
    """
    принимаем собщение от комитета,
    отправляем ответ обратно комитету
    """
    print(body.decode())
    response = "Ok from social security"

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