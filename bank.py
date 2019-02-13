import pika
import uuid

import helper

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()

# создаем очередь для отправки запросов на оплату пошлины
channel.queue_declare(queue='bank')

# создаем точку обмена для отправки сообщений клиенту и в комитет
channel.exchange_declare(exchange='direct_bank', exchange_type='direct')

# routing key для отправки сообщений клиенту и комитету
routing_key = 'bank_person_committee'


def callback(ch, method, props, body):
    """
    callback-функция для приема платежей от людей,
    проверяем, что сумма 500, если все ок, присваиваем номер транзакции
    и отправляем человеку и комитету ответ о том, что человек
    успешно совершил оплату
    """
    print(body.decode())
    bank_dict = helper.unpack_str(body)

    if bank_dict['sum'] == 500:
        message = helper.pack_to_str({'transaction_id': str(uuid.uuid4()),
                                      'response': 'ok'})
        ch.basic_publish(exchange='direct_bank',
                         routing_key=routing_key,
                         properties=pika.BasicProperties(
                             correlation_id=props.correlation_id),
                         body=message)
    else:
        message = helper.pack_to_str({'response': 'payment failed'})
        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(
                             correlation_id=props.correlation_id),
                         body=message)

    ch.basic_ack(delivery_tag=method.delivery_tag)


print('[*] Waiting for fee from person')

channel.basic_consume(callback, queue='bank')
channel.start_consuming()
