import pika
import os

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='direct_mid_mvd', exchange_type='direct')

# binding key для приема сообщений от мид/мвд
binding_key_for_mid_mvd = 'mid_mvd_committee'

# очередь для приема ответов от мид и мвд
for_mid_mvd = channel.queue_declare(exclusive=True)
queue_mid_mvd = for_mid_mvd.method.queue

# bind от точки обмена комитета до очереди приема ответов от мвд/мид
channel.queue_bind(exchange='direct_mid_mvd',
                   queue=queue_mid_mvd,
                   routing_key=binding_key_for_mid_mvd)


def security_on_request(ch, method, props, body):
    print("Security service {}".format(body.decode()))

    channel.basic_publish(exchange='',
                          routing_key='approval',
                          properties=pika.BasicProperties(
                              reply_to=props.reply_to,
                              correlation_id=props.correlation_id),
                          body=body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)


def mvd_request(ch, method, props, body):

    if os.path.isfile('./response_from_mvd_{}.json'.format(props.correlation_id)):
        print("I caught file from mvd")
        ch.basic_publish(exchange='',
                         routing_key='from_mid_mvd',
                         properties=pika.BasicProperties(
                             correlation_id=props.correlation_id),
                         body='response_from_mvd_{}.json'.format(
                             props.correlation_id))
    ch.basic_ack(delivery_tag=method.delivery_tag)


def pre_ok(ch, method, props, body):
    print(body.decode())

    ch.basic_publish(exchange='',
                     routing_key='from_social_sec',
                     properties=pika.BasicProperties(correlation_id=
                                                     props.correlation_id,
                                                     reply_to=props.reply_to),
                     body=body.decode())

    ch.basic_ack(delivery_tag=method.delivery_tag)


def final_ok(ch, method, props, body):
    print(body.decode())

    ch.basic_publish(exchange='',
                     routing_key='from_tax',
                     properties=pika.BasicProperties(
                         correlation_id=props.correlation_id,
                         reply_to=props.reply_to),
                     body=body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)



print("[x] Waiting for requests from people for security")

# принимаем запросы на вид на жительство от людей
channel.basic_consume(security_on_request, queue='approval')

# принимаем ответы от мид и мвд
channel.basic_consume(mvd_request, queue=queue_mid_mvd)

# принимаем ответы от министерства соцобепечения
channel.basic_consume(pre_ok, queue='from_social_sec')

# принимаем ответы от налоговой
channel.basic_consume(final_ok, queue='from_tax')

channel.start_consuming()
