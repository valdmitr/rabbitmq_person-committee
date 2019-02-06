import pika
import json
import os


connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'
))

channel = connection.channel()

channel.queue_declare(queue='internal_mvd') # создаем очередь для приема сообщений от внутренней базы
channel.queue_declare(queue='external_mvd') # создаем очередь для приема сообщений от внешней базы

channel.exchange_declare(exchange='fanout_internal_external',
                         exchange_type='fanout') # создаем точку доступа для отправки сообщений одновременно во внутреннюю и во внешние базы

result = channel.queue_declare(exclusive=True) # создаем очередь для приема сообщений от комитета
queue_name = result.method.queue


binding_key = 'committee_mid_mvd'

channel.queue_bind(exchange='direct_mid',
                   queue=queue_name,
                   routing_key=binding_key) # создаем binding между точкой доступа direct_mid и очередью для приема сообщений от комитета

print('[*] Waiting for a request from the Committee')


def callback(ch, method, props, body):
    """
    принимаем сообщения от комитета,
    отправляем запрос одновременно во внутреннюю и во внешнюю базу
    """
    response = "{} {}".format("mvd", body.decode())
    print(response)
    ch.basic_publish(exchange='fanout_internal_external',
                          routing_key='',
                          properties=pika.BasicProperties(correlation_id=
                                                     props.correlation_id,
                                                     reply_to=props.reply_to),
                          body=body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)


def internal_request(ch, method, props, body):
    """
    принимаем сообщение от внутренней базы,
    загоняем ответ в файл in.json
    """
    response = "Internal ok {}".format(body.decode())
    print(response)

    internal_dict = {'body': body.decode(),
                     'correlation_id': props.correlation_id,
                     'reply_to': props.reply_to}

    print(internal_dict)
    # with open("{}_in.json".format(props.correlation_id), "w") as write_file:
    with open("in.json", "w") as write_file:
        json.dump(internal_dict, write_file)
    exist_file_in_out(ch)

    ch.basic_ack(delivery_tag=method.delivery_tag)


def external_request(ch, method, props, body):
    """
    принимаем сообщение от внешней базы,
    загоняем ответ в файл ex.json
    """
    response = "External ok {}".format(body.decode())
    print(response)

    external_dict = {'body': body.decode(),
                     'correlation_id': props.correlation_id,
                     'reply_to': props.reply_to}

    print(external_dict)
    with open("ex.json", "w") as write_file:
        json.dump(external_dict, write_file)
    exist_file_in_out(ch)

    ch.basic_ack(delivery_tag=method.delivery_tag)


def exist_file_in_out(ch):
    """
    функция проверяет наличие файлов от внутренней и внешней базы,
    отправляем итоговый ок комитету,
    после чего удаляем созданные файлы in.json, ex.json
    :param ch: канал, который передаем от callback-функции
    """
    if os.path.isfile('./in.json') and os.path.isfile('./ex.json'):
        os.rename('./in.json', './response_from_mvd.json')
        ch.basic_publish(exchange='',
                         routing_key='from_mid_mvd',
                         body="response_from_mvd.json")
        os.remove('./ex.json')


channel.basic_consume(callback, queue=queue_name)
channel.basic_consume(internal_request, queue='internal_mvd')
channel.basic_consume(external_request, queue='external_mvd')

channel.start_consuming()