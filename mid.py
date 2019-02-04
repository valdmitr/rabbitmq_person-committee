import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'
))

channel = connection.channel()


result = channel.queue_declare(exclusive=True) #создаем очередь для приема сообщений от комитета
queue_name = result.method.queue #имя нашей созданной очереди записываем в переменную

binding_key = 'committee_mid_mvd'

channel.queue_bind(exchange='direct_mid',
                   queue=queue_name,
                   routing_key=binding_key)

print('[*] Waiting for a request from the Committee')


def callback(ch, method, props, body):
    """
    принимаем собщение от комитета,
    отправляем ответ обратно комитету
    """
    response = "{} {}".format("mid", body.decode())
    print(response)

    mid_dict = {'body': body.decode()}
    mid_dict['correlation_id'] = props.correlation_id
    mid_dict['reply_to'] = props.reply_to
    print(mid_dict)

    with open("response_from_mid.json", "w") as write_file:
        json.dump(mid_dict, write_file)

    ch.basic_publish(exchange='',
                     routing_key='from_mid_mvd',
                     body="response_from_mid.json")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(callback, queue=queue_name)

channel.start_consuming()