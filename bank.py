import pika
import uuid
import json

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

    bank_dict = json.loads(body)
    print(bank_dict)

    if bank_dict['sum'] == 500:
        transaction_id = str(uuid.uuid4())
        ch.basic_publish(exchange='direct_bank',
                         routing_key=routing_key,
                         properties=pika.BasicProperties(
                             correlation_id=props.correlation_id),
                         body=transaction_id)

    ch.basic_ack(delivery_tag=method.delivery_tag)


print('[*] Waiting for fee from person')

channel.basic_consume(callback, queue='bank')
channel.start_consuming()
