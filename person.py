import pika
import uuid
import json
import os

import helper


class PersonRpcClient:
    """
    класс, в котором прописываем rpc-клиента
    """

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

        # binding key для приема сообщений от банка
        self.binding_key = 'bank_person_committee'

        # очередь для приема ответов от комитета
        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        # очередь для приема ответов от exams
        result_exams = self.channel.queue_declare(exclusive=True)
        self.callback_queue_exams = result_exams.method.queue

        # очередь для приема ответов от банка
        result_bank = self.channel.queue_declare(exclusive=True)
        self.callback_queue_bank = result_bank.method.queue

        # очередь для приема итогового ответа от комитета
        final_result = self.channel.queue_declare(exclusive=True)
        self.callback_queue_final_result = final_result.method.queue

        # bind от точки обмена банка до очереди приема ответов от банка
        self.channel.queue_bind(exchange='direct_bank',
                                queue=self.callback_queue_bank,
                                routing_key=self.binding_key)

        # принимаем предварительный ок от комитета
        self.channel.basic_consume(self.waiting_ok, no_ack=True,
                                   queue=self.callback_queue)

        # принимаем результаты экзаменов
        self.channel.basic_consume(self.passed_exams,
                                   queue=self.callback_queue_exams)

        # принимаем код транзакции от банка
        self.channel.basic_consume(self.bank_request,
                                   queue=self.callback_queue_bank)

        # принимаем итоговый ок от комитета
        self.channel.basic_consume(self.final_ok, no_ack=True,
                                   queue=self.callback_queue_final_result)


    def waiting_ok(self, ch, method, props, body):
        """
        callback-функция для приема ответов на запрос,
        проверяем соответствие id и в response помещаем полученный body.
        Отправляем запрос на сдачу экзаменов.
        """
        if self.person_id == props.correlation_id:
            self.response = body

            request_exams = helper.simple_pack({"Person {}".format(
                props.correlation_id): "I want to pass exams"})

            ch.basic_publish(exchange='',
                             routing_key='exams_center',
                             properties=pika.BasicProperties(
                                 reply_to=self.callback_queue_exams,
                                 correlation_id=props.correlation_id),
                             body=request_exams)


    def passed_exams(self, ch, method, props, body):
        """
        callback-функция для приема предварительного одобрения на запрос,
        проверяем соответствие id и в response помещаем полученный ответ от
        центра сдачи экзаменов. Создаем файл с ответом от центра экзаменов.
        Отправляем пошлину в банк.
        """

        if self.person_id == props.correlation_id:
            self.response_from_exams = body

            with open("resp_exams_{}.json".format(props.correlation_id), "w") as write_file:
                json.dump({'exams': body.decode()}, write_file)

            fee_dict = {'Person {}'.format(props.correlation_id): 'I would like to pay', 'type': 'fee', 'sum': 500}
            req_bank = json.dumps(fee_dict)

            self.channel.basic_publish(exchange='',
                                       routing_key='bank',
                                       properties=pika.BasicProperties(
                                           correlation_id=props.correlation_id),
                                       body=req_bank)
            self.exist_file_exams_bank(ch, props)

        ch.basic_ack(delivery_tag=method.delivery_tag)


    def bank_request(self, ch, method, props, body):
        """
        callback-функция для приема ответов от банка,
        проверяем соответствие id и в response помещаем полученный код
        от банка. Создаем файл с кодом транзакции от банка и person_id.
        """
        if self.person_id == props.correlation_id:
            self.response_from_bank = 'transaction_id {}'.format(body.decode())

            with open("resp_bank_{}.json".format(props.correlation_id), "w") as write_file:
                json.dump({'transaction_id': body.decode(),
                           'person_id': props.correlation_id}, write_file)
            self.exist_file_exams_bank(ch, props)

        ch.basic_ack(delivery_tag=method.delivery_tag)


    def exist_file_exams_bank(self, ch, props):
        """
        проверяет наличие файлов от центра экзаменов и от банка,
        отправляем итоговый ок комитету с кодом транзакции и person_id,
        после чего удаляем созданные файлы
        :param ch: канал, который передаем от callback-функции
        """
        if os.path.isfile('./resp_exams_{}.json'.format(props.correlation_id)) and \
                os.path.isfile('./resp_bank_{}.json'.format(props.correlation_id)):
            with open("resp_bank_{}.json".format(props.correlation_id), "r") as read_file:
                data_to_publish = json.load(read_file)
                ch.basic_publish(exchange='',
                                 routing_key='from_person_fee',
                                 properties=pika.BasicProperties(
                                     reply_to=self.callback_queue_final_result,
                                     correlation_id=data_to_publish['person_id']),
                                 body=data_to_publish['transaction_id'])
            os.remove('./resp_bank_{}.json'.format(props.correlation_id))
            os.remove('./resp_exams_{}.json'.format(props.correlation_id))


    def final_ok(self, ch, method, props, body):
        """
        callback-функция для приема итоговых ответов от комиссии
        """
        if self.person_id == props.correlation_id:
            self.final_response = body


    def call(self, message):
        """
        отправляем запрос на получение вида на жительства
        :param message: сообщение запроса
        :return: ответ на запрос
        """
        my_message = helper.simple_pack({'message_from_person':message})
        self.response = None
        self.response_from_exams = None
        self.response_from_bank = None
        self.final_response = None
        self.person_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='',
                                   routing_key='approval',
                                   properties=pika.BasicProperties(
                                       reply_to=self.callback_queue,
                                       correlation_id=self.person_id),
                                   body=my_message)
        """
        слушаем очереди, пока ловим self.response_from_exams, 
        self.response и self.response_from_bank. Когда поймали 
        self.response_from_bank, выводим тело self.response, 
        затем выводим тело self.response_from_exams и тело
        self.response_from_bank.
        
        """


        while self.response is None:
            while self.response_from_exams is None:
                while self.response_from_bank is None:
                    while self.final_response is None:
                        self.connection.process_data_events()
                    print(self.response.decode())
                print (self.response_from_exams.decode())
            print(self.response_from_bank)
        return self.final_response

        # while self.final_response is None:
        #     self.connection.process_data_events()
        # print(self.response.decode())
        # print("I want to pass exams.")
        # print (self.response_from_exams.decode())
        # print(self.response_from_bank)
        # return self.final_response


person = PersonRpcClient()

print('[x] I want to get a residence permit')
response = person.call('I want to get a residence permit')
print(response.decode())
# print (response)
