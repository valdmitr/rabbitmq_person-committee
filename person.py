import pika
import uuid

class PersonRpcClient():
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(self.waiting_ok, no_ack=True,
                                   queue=self.callback_queue)




    def waiting_ok(self, ch, method, props, body):
        if self.person_id == props.correlation_id:
            self.response = body


    def call(self, message):
        self.response = None
        self.person_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='',
                                   routing_key='approval',
                                   properties=pika.BasicProperties(
                                       reply_to=self.callback_queue,
                                       correlation_id=self.person_id),
                                   body = message)
        while self.response is None:
            self.connection.process_data_events()
        return self.response

person = PersonRpcClient()

print('[x] I want to get a residence permit')
response = person.call('I want to get a residence permit')
print (" [.] It is %r" % (response,))