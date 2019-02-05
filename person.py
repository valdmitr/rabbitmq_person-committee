import pika
import uuid
import json

class PersonRpcClient():


    def __init__(self):
        """
        создаем подключение,
        подключаемся,
        создаем очередь reply_to,
        и принимаем по ней ответ на наш запрос
        """
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue # очередь для приема ответов от комитета

        result_exams = self.channel.queue_declare(exclusive=True)
        self.callback_queue_exams = result_exams.method.queue # очередь для приема ответов от exams

        self.channel.basic_consume(self.waiting_ok, no_ack=True,
                                   queue=self.callback_queue)

        self.channel.basic_consume(self.passed_exams,
                                   queue=self.callback_queue_exams)


    def waiting_ok(self, ch, method, props, body):
        """
        callback-функция для приема ответов на запрос,
        проверяем соответствие id и в response помещаем полученный body.
        Отправляем запрос на сдачу экзаменов.
        """
        request_exams = "I want to pass exams."


        if self.person_id == props.correlation_id:
            self.response = body


            ch.basic_publish(exchange='',
                                   routing_key='exams_center',
                                   properties=pika.BasicProperties(
                                       reply_to=self.callback_queue_exams,
                                       correlation_id=props.correlation_id),
                                   body = request_exams)



    def passed_exams(self, ch, method, props, body):
        """
        callback-функция для приема ответов на запрос,
        проверяем соответствие id и выводим ответ от центра сдачи экзаменов
        """

        if self.person_id == props.correlation_id:
            self.response_from_exams = body

            # req_bank = json.dumps({'type':'fee', 'sum':500})
            #
            # self.channel.basic_publish(exchange='',
            #                            routing_key='approval',
            #                            properties=pika.BasicProperties(
            #                                reply_to=self.callback_queue,
            #                                correlation_id=self.person_id),
            #                            body=req_bank)



        ch.basic_ack(delivery_tag=method.delivery_tag)


    def call(self, message):
        """
        отправляем запрос на получение вида на жительства
        :param message: сообщение запроса
        :return: ответ на запрос
        """
        my_message = "{} {}".format("person", message)
        self.response = None
        self.response_from_exams = None
        self.person_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='',
                                   routing_key='approval',
                                   properties=pika.BasicProperties(
                                       reply_to=self.callback_queue,
                                       correlation_id=self.person_id),
                                   body = my_message)
        """
        слушаем очереди, пока ловим self.response_from_exams и self.response.
        Когда поймали self.response_from_exams, выводим тело self.response, 
        затем выводим self.response_from_exams
        
        """
        while self.response is None:
            while self.response_from_exams is None:
                self.connection.process_data_events()
            print(self.response.decode())
            print("I want to pass exams.")
        return self.response_from_exams




person = PersonRpcClient()

print('[x] I want to get a residence permit')
response = person.call('I want to get a residence permit')
print (response.decode())
# print(response)