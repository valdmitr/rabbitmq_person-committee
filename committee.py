import pika
import os

import helper


connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()

# создаем точку обмена для отправки сообщений в мид и мвд
channel.exchange_declare(exchange='direct_mid', exchange_type='direct')

# создаем точку обмена для отправки сообщений от банка клиенту и в комитет
channel.exchange_declare(exchange='direct_bank', exchange_type='direct')

channel.exchange_declare(exchange='direct_mid_mvd', exchange_type='direct')

# routing key для точки обмена direct_mid
routing_key = 'committee_mid_mvd'

# binding key для приема сообщений от банка
binding_key_for_bank = 'bank_person_committee'

# binding key для приема сообщений от мид/мвд
binding_key_for_mid_mvd = 'mid_mvd_committee'

# создаем очередь для отправки запросов на получение вида на жительство от людей
channel.queue_declare(queue='approval')

# # очередь для приема ответов от мид и мвд
# channel.queue_declare(queue='from_mid_mvd')

# очередь для приема ответов от министерства соцобеспечения
channel.queue_declare(queue='from_social_sec')

# очередь для приема ответов от людей, которые сдали экзамены и оплатили пошлину
channel.queue_declare(queue='from_person_fee')

# очередь для приема ответов от налоговой
channel.queue_declare(queue='from_tax')

# очередь для приема ответов от банка
for_bank = channel.queue_declare(exclusive=True)
callback_queue_bank = for_bank.method.queue

# очередь для приема ответов от мид и мвд
for_mid_mvd = channel.queue_declare(exclusive=True)
callback_queue_mid_mvd = for_mid_mvd.method.queue


def on_request(ch, method, props, body):
    """
    callback-функция для приема запросов от людей,
    тут отправляем запрос в мид и мвд
    """
    body = body.decode()
    print(body)

    r = {'Committee':'ok'}
    response = helper.append_smth(body, r)

    ch.basic_publish(exchange='direct_mid',
                     routing_key=routing_key,
                     properties=pika.BasicProperties(
                         correlation_id=props.correlation_id,
                         reply_to=props.reply_to),
                     body=str(response))

    ch.basic_ack(delivery_tag=method.delivery_tag)


def mid_mvd_request(ch, method, props, body):
    """
    callback-функция для приема ответов от мид и мвд,
    при наличии файлов и от мид и от мвд
    отправляем запрос в министерство соцобеспечения
    """

    if os.path.isfile('./response_from_mid_{}.json'.format(props.correlation_id)) and \
            os.path.isfile('./response_from_mvd_{}.json'.format(props.correlation_id)):
        with open('response_from_mvd_{}.json'.format(props.correlation_id),
                  "r") as read_file:

            data_to_publish = helper.unpack_file(read_file)
            data_to_publish.update({'mid': 'ok', 'mvd': 'ok'})
            body_to_publish = helper.pack_to_str(data_to_publish)

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
    отправляем предварительный ок человеку. response ok показывает нам,
    что весь путь запрос прошел верно
    """
    print(body.decode())
    response = helper.pack_to_str({'response': 'ok',
                                   'Committee': 'Pre-OK. You need to pass exam '
                                                'and pay fee'})

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
    dict_bank_resp = helper.unpack_str(body)

    helper.write_file("data_from_bank_{}.json".format(props.correlation_id),
                      {'transaction_id': dict_bank_resp['transaction_id'],
                       'person_id': props.correlation_id})

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
            bank_dict = helper.unpack_file(read_file)
            if bank_dict['transaction_id'] == body.decode()\
                    and bank_dict['person_id'] == props.correlation_id:
                print("Ok, Person {} payd fee".format(props.correlation_id))

                tax_message = helper.pack_to_str({"Committee":
                                                      "Person {} wants to get a "
                                                      "residence permit".format(
                                                          props.correlation_id)})
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
    отправляем итоговый ответ человеку. response ok показывает нам,
    что весь путь запрос прошел верно
    """
    print(body.decode())
    tax_dict = helper.unpack_str(body.decode())
    response = helper.pack_to_str({"response": "ok",
                                   "Committee":"Congrats! Your taxpayer_id "
                                               "{}".format(tax_dict['taxpayer_id'])})
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(
                         correlation_id=props.correlation_id),
                     body=response)

    ch.basic_ack(delivery_tag=method.delivery_tag)


# bind от точки обмена банка до очереди приема ответов от банка
channel.queue_bind(exchange='direct_bank',
                   queue=callback_queue_bank,
                   routing_key=binding_key_for_bank)

# bind от точки обмена комитета до очереди приема ответов от мвд/мид
channel.queue_bind(exchange='direct_mid_mvd',
                   queue=callback_queue_mid_mvd,
                   routing_key=binding_key_for_mid_mvd)

print("[x] Waiting for requests from people")

channel.basic_qos(prefetch_count=1)

# принимаем запросы на вид на жительство от людей
channel.basic_consume(on_request, queue='approval')

# принимаем ответы от мид и мвд
channel.basic_consume(mid_mvd_request, queue=callback_queue_mid_mvd)

# принимаем ответы от министерства соцобепечения
channel.basic_consume(social_request, queue='from_social_sec')

# принимаем код транзакции от банка
channel.basic_consume(bank_request, queue=callback_queue_bank)

# принимаем ответов от людей, которые сдали экзамены и оплатили
channel.basic_consume(person_fee_request, queue='from_person_fee')

# принимаем ответы от налоговой
channel.basic_consume(tax_request, queue='from_tax')

channel.start_consuming()
