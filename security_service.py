import pika
import os

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()

def security_on_request(ch, method, props, body):
    print("Security service {}".format(body.decode()))

    channel.basic_publish(exchange='',
                          routing_key='approval',
                          properties=pika.BasicProperties(
                              reply_to=props.reply_to,
                              correlation_id=props.correlation_id),
                          body=body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)


def mid_request(ch, method, props, body):
    if os.path.isfile(
        './response_from_mid_{}.json'.format(props.correlation_id)):
        ch.basic_publish(exchange='',
                         routing_key='from_mid_mvd',
                         properties=pika.BasicProperties(
                             correlation_id=props.correlation_id),
                         body="response_from_mid_{}.json".format(props.correlation_id))
    elif os.path.isfile('./response_from_mvd_{}.json'.format(props.correlation_id)):
        print("I caught file from mvd")
        ch.basic_publish(exchange='',
                         routing_key='from_mid_mvd',
                         properties=pika.BasicProperties(
                             correlation_id=props.correlation_id),
                         body='response_from_mvd_{}.json'.format(
                             props.correlation_id))
    ch.basic_ack(delivery_tag=method.delivery_tag)



print("[x] Waiting for requests from people for security")

# принимаем запросы на вид на жительство от людей
channel.basic_consume(security_on_request, queue='approval')

# принимаем ответы от мид и мвд
channel.basic_consume(mid_request, queue='from_mid_mvd')


# channel.basic_reject()
channel.start_consuming()
