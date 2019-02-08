import pika
import json

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


def callback(ch, method, props, body):
    """
    принимаем собщение от комитета,
    отправляем ответ обратно комитету
    """
    response = "{} {}".format("mid", body.decode())
    print(response)

    mid_dict = {'body': body.decode(),
                'correlation_id': props.correlation_id,
                'reply_to': props.reply_to}
    print(mid_dict)

    # записываем файл с данными от мид
    with open("response_from_mid_{}.json".format(props.correlation_id), "w") as write_file:
        json.dump(mid_dict, write_file)

    ch.basic_publish(exchange='',
                     routing_key='from_mid_mvd',
                     properties=pika.BasicProperties(
                         correlation_id=props.correlation_id),
                     body="response_from_mid_{}.json".format(props.correlation_id))
    ch.basic_ack(delivery_tag=method.delivery_tag)


print('[*] Waiting for a request from the Committee')

# принимаем запрос от комитета
channel.basic_consume(callback, queue=queue_name)
channel.start_consuming()
