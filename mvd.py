import pika
import json
import os


connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'
))

channel = connection.channel()

# создаем очередь для приема сообщений от внутренней базы
channel.queue_declare(queue='internal_mvd')

# создаем очередь для приема сообщений от внешней базы
channel.queue_declare(queue='external_mvd')

# создаем точку доступа для отправки сообщений
# одновременно во внутреннюю и во внешние базы
channel.exchange_declare(exchange='fanout_internal_external',
                         exchange_type='fanout')

# создаем очередь для приема сообщений от комитета
result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

# binding key для приема сообщений от комитета
binding_key = 'committee_mid_mvd'

# создаем binding между точкой доступа direct_mid
# и очередью для приема сообщений от комитета
channel.queue_bind(exchange='direct_mid',
                   queue=queue_name,
                   routing_key=binding_key)


def callback(ch, method, props, body):
    """
    принимаем сообщения от комитета, отправляем запрос
    одновременно во внутреннюю и во внешнюю базу
    """
    response = "{} {}".format("mvd", body.decode())
    print(response)
    ch.basic_publish(exchange='fanout_internal_external',
                     routing_key='',
                     properties=pika.BasicProperties(
                         correlation_id=props.correlation_id,
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

    # записываем в файл данные, которые пришли от внутренней базы мвд
    with open("in_{}.json".format(props.correlation_id), "w") as write_file:
        json.dump(internal_dict, write_file)
    exist_file_in_out(ch, props)

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

    # записываем в файл данные, которые пришли от внешней базы мвд
    with open("ex_{}.json".format(props.correlation_id), "w") as write_file:
        json.dump(external_dict, write_file)
    exist_file_in_out(ch, props)

    ch.basic_ack(delivery_tag=method.delivery_tag)


def exist_file_in_out(ch, props):
    """
    функция проверяет наличие файлов от внутренней и внешней базы,
    переименовываем файл от внутренней базы в файл с данными от мвд,
    отправляем итоговый ок комитету,
    после чего удаляем созданные файлы in.json, ex.json
    :param ch: канал, который передаем от callback-функции
    """
    if os.path.isfile('./in_{}.json'.format(props.correlation_id)) and \
            os.path.isfile('./ex_{}.json'.format(props.correlation_id)):
        os.rename('./in_{}.json'.format(props.correlation_id),
                  './response_from_mvd_{}.json'.format(props.correlation_id))
        ch.basic_publish(exchange='',
                         routing_key='from_mid_mvd',
                         properties=pika.BasicProperties(
                             correlation_id=props.correlation_id),
                         body='response_from_mvd_{}.json'.format(props.correlation_id))
        os.remove('./ex_{}.json'.format(props.correlation_id))


print('[*] Waiting for a request from the Committee')

# принимаем запрос от комитета
channel.basic_consume(callback, queue=queue_name)

# принимаем данные от внутренней базы
channel.basic_consume(internal_request, queue='internal_mvd')

# принимаем данные от внешней базы
channel.basic_consume(external_request, queue='external_mvd')
channel.start_consuming()
