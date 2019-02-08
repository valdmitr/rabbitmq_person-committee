import pika
import os
import json


connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()

# создаем точку обмена для отправки сообщений в мид и мвд
channel.exchange_declare(exchange='direct_mid', exchange_type='direct')

# создаем точку обмена для отправки сообщений от банка клиенту и в комитет
channel.exchange_declare(exchange='direct_bank', exchange_type='direct')

# routing key для точки обмена direct_mid
routing_key = 'committee_mid_mvd'

# binding key для приема сообщений от банка
binding_key = 'bank_person_committee'

# создаем очередь для отправки запросов на получение вида на жительство от людей
channel.queue_declare(queue='approval')

# очередь для приема ответов от мид и мвд
channel.queue_declare(queue='from_mid_mvd')

# очередь для приема ответов от министерства соцобеспечения
channel.queue_declare(queue='from_social_sec')

# очередь для приема ответов от людей, которые сдали экзамены и оплатили пошлину
channel.queue_declare(queue='from_person_fee')

# очередь для приема ответов от налоговой
channel.queue_declare(queue='from_tax')

# очередь для приема ответов от банка
for_bank = channel.queue_declare(exclusive=True)
callback_queue_bank = for_bank.method.queue


def on_request(ch, method, props, body):
    """
    callback-функция для приема запросов от людей,
    тут отправляем запрос в мид и мвд
    """
    body = body.decode()
    r = 'ok'
    print(body)

    response = ("{} {}".format(body, r))

    list_of_body = body.split(' ')

    if list_of_body[0] == 'person':
        ch.basic_publish(exchange='direct_mid',
                         routing_key=routing_key,
                         properties=pika.BasicProperties(
                             correlation_id=props.correlation_id,
                             reply_to=props.reply_to),
                         body=str(response))

    ch.basic_ack(delivery_tag=method.delivery_tag)


def mid_request(ch, method, props, body):
    """
    callback-функция для приема ответов от мид и мвд,
    отправляем запрос в министерство соцобеспечения
    """
    if os.path.isfile('./response_from_mid_{}.json'.format(props.correlation_id)) and \
            os.path.isfile('./response_from_mvd_{}.json'.format(props.correlation_id)):
        with open('response_from_mvd_{}.json'.format(props.correlation_id),
                  "r") as read_file:
            data_to_publish = json.load(read_file)
            body_to_publish = "Ok from mid and mvd {}".format(
                data_to_publish['body'])
            ch.basic_publish(exchange='',
                             routing_key='committee_social',
                             properties=pika.BasicProperties(
                                 correlation_id=data_to_publish['correlation_id'],
                                 reply_to=data_to_publish['reply_to']),
                             body=body_to_publish)
        os.remove('response_from_mid_{}.json'.format(props.correlation_id))
        os.remove('response_from_mvd_{}.json'.format(props.correlation_id))

    ch.basic_ack(delivery_tag=method.delivery_tag)


def social_request(ch, method, props, body):
    """
    callback-функция для приема ответов от министерства соцобеспечения,
    отправляем предварительный ок человеку
    """
    print(body.decode())
    response = "Pre-OK. You need to pass exam and pay fee"

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(
                         correlation_id=props.correlation_id),
                     body=response)

    ch.basic_ack(delivery_tag=method.delivery_tag)


def bank_request(ch, method, props, body):
    """
    callback-функция для приема ответов от банка,
    пакуем данные файл
    """
    with open("data_from_bank_{}.json".format(props.correlation_id), "w") as write_file:
        json.dump({'transaction_id': body.decode(),
                   'person_id': props.correlation_id}, write_file)

    ch.basic_ack(delivery_tag=method.delivery_tag)


def person_fee_request(ch, method, props, body):
    """
    callback-функция для приема ответов от человека (экзамены
    по языку сдал, пошлину оплатил), распаковываем данные из
    файла и сравниваем с полученными значениями. Отправляем запрос
    в налоговую.
    """
    if os.path.isfile('./data_from_bank_{}.json'.format(props.correlation_id)):
        with open("data_from_bank_{}.json".format(props.correlation_id), "r") as read_file:
            bank_dict = json.load(read_file)
            if bank_dict['transaction_id'] == body.decode()\
                    and bank_dict['person_id'] == props.correlation_id:
                print("Ok, Person {} payd fee".format(props.correlation_id))

                tax_message = "Person {} wants to get a residence " \
                              "permit".format(props.correlation_id)
                ch.basic_publish(exchange='',
                                 routing_key='committee_tax',
                                 properties=pika.BasicProperties(
                                     reply_to=props.reply_to,
                                     correlation_id=props.correlation_id),
                                 body=tax_message)
        os.remove('./data_from_bank_{}.json'.format(props.correlation_id))

    ch.basic_ack(delivery_tag=method.delivery_tag)


def tax_request(ch, method, props, body):
    """
    callback-функция для приема ответов от налоговой,
    отправляем итоговый ответ человеку
    """
    print(body.decode())
    tax_dict = json.loads(body.decode())
    response = "Congrats! Your taxpayer_id {}".format(tax_dict['taxpayer_id'])
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(
                         correlation_id=props.correlation_id),
                     body=response)

    ch.basic_ack(delivery_tag=method.delivery_tag)


# bind от точки обмена банка до очереди приема ответов от банка
channel.queue_bind(exchange='direct_bank',
                   queue=callback_queue_bank,
                   routing_key=binding_key)

print("[x] Waiting for requests from people")

channel.basic_qos(prefetch_count=1)

# принимаем запросы на вид на жительство от людей
channel.basic_consume(on_request, queue='approval')

# принимаем ответы от мид и мвд
channel.basic_consume(mid_request, queue='from_mid_mvd')

# принимаем ответы от министерства соцобепечения
channel.basic_consume(social_request, queue='from_social_sec')

# принимаем код транзакции от банка
channel.basic_consume(bank_request, queue=callback_queue_bank)

# принимаем ответов от людей, которые сдали экзамены и оплатили
channel.basic_consume(person_fee_request, queue='from_person_fee')

# принимаем ответы от налоговой
channel.basic_consume(tax_request, queue='from_tax')

channel.start_consuming()
