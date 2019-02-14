import pika

import helper

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()

# создаем очередь для отправки запросов для сдачи экзаменов
channel.queue_declare(queue='exams_center')


def callback(ch, method, props, body):
    """
    callback-функция для приема запрсов для сдачи экзаменов от людей,
    проверяем проходит ли человек установленный порог,
    отправляем ответ о том, что человек сдал экзамены,
    если не сдал - кидаем обратный ответ.
    """
    print(body.decode())
    exam_dict = helper.unpack_str(body)
    if exam_dict['exam_result'] >= 80:
        response = helper.append_smth(body.decode(),
                                      {'exams_center': 'Yep, you passed!',
                                       'response': 'ok'})

        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(
                             correlation_id=props.correlation_id),
                         body=response)
    else:
        response = helper.pack_to_str({'exams_center': 'No',
                                       'response': 'sorry, you did not pass the exam'})
        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(
                             correlation_id=props.correlation_id),
                         body=response)

    ch.basic_ack(delivery_tag=method.delivery_tag)


print('[*] Waiting for a request from person')

channel.basic_consume(callback, queue='exams_center')

channel.start_consuming()
