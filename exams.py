import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='exams_center') # создаем очередь для отправки запросов для сдачи экзаменов

def callback(ch, method, props, body):
    """
    callback-функция для приема запрсов для сдачи экзаменов от людей,
    отправляем ответ о том, что человек сдал экзамены
    """
    print(body.decode())
    response = "Yep, you passed!"

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=props.correlation_id),
                     body=response)
    ch.basic_ack(delivery_tag=method.delivery_tag)


print('[*] Waiting for a request from person')

channel.basic_consume(callback, queue='exams_center')

channel.start_consuming()