import pika
import uuid
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
        создаем очереди,
        и принимаем по ним ответ на наш запрос
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
        Если полученный response ok, oтправляем запрос на сдачу экзаменов,
        иначе меняем флаг is_final на True
        """
        if self.person_id == props.correlation_id:
            self.response = body.decode()
            pre_ok_dict = helper.unpack_str(body)

            if pre_ok_dict['response'] == 'ok':
                request_exams = helper.pack_to_str({
                    "Person {}".format(props.correlation_id): "I want to pass exams",
                    "exam_result": 80
                })

                ch.basic_publish(exchange='',
                                 routing_key='exams_center',
                                 properties=pika.BasicProperties(
                                     reply_to=self.callback_queue_exams,
                                     correlation_id=props.correlation_id),
                                 body=request_exams)
            else:
                self.is_final = True


    def passed_exams(self, ch, method, props, body):
        """
        callback-функция для приема предварительного одобрения на запрос,
        проверяем соответствие id и в response помещаем полученный ответ от
        центра сдачи экзаменов. Если полученный response ok, oтправляем
        запрос на сдачу экзаменов, создаем файл с ответом от центра
        экзаменов и отправляем пошлину в банк. Если полученный response
         другой - меняем флаг is_final на True
        """
        if self.person_id == props.correlation_id:
            self.response_from_exams = body.decode()
            dict_exams_response = helper.unpack_str(body)
            if dict_exams_response['response'] == 'ok':

                with open("resp_exams_{}.json".format(props.correlation_id),
                          "w") as write_file: write_file.write(body.decode())

                fee_dict = {
                    'Person {}'.format(props.correlation_id): 'I would like to pay',
                    'type': 'fee',
                    'sum': 500
                }
                req_bank = helper.pack_to_str(fee_dict)

                self.channel.basic_publish(exchange='',
                                           routing_key='bank',
                                           properties=pika.BasicProperties(
                                               reply_to=self.callback_queue_bank,
                                               correlation_id=props.correlation_id),
                                           body=req_bank)
                self.exist_file_exams_bank(ch, props)
            else:
                self.is_final = True
        ch.basic_ack(delivery_tag=method.delivery_tag)


    def bank_request(self, ch, method, props, body):
        """
        callback-функция для приема ответов от банка,
        проверяем соответствие id, и в response помещаем полученный
        ответ от банка. Если полученный response ok, создаем файл
        с кодом транзакции от банка и person_id. Если полученный
        response другой - меняем флаг is_final на True.
        """
        if self.person_id == props.correlation_id:
            dict_bank_resp = helper.unpack_str(body)
            if dict_bank_resp['response'] == 'ok':

                self.response_from_bank = 'transaction_id {}'.format(body.decode())

                helper.write_file("resp_bank_{}.json".format(props.correlation_id),
                                  {'transaction_id': dict_bank_resp['transaction_id'],
                                   'person_id': props.correlation_id})
                self.exist_file_exams_bank(ch, props)
            else:
                self.response_from_bank = body.decode()
                self.is_final = True

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
                data_to_publish = helper.unpack_file(read_file)

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
        callback-функция для приема итоговых ответов от комиссии.
        Если полученный response ok,
        """
        if self.person_id == props.correlation_id:
            self.final_response = body.decode()




    def call(self, message):
        """
        отправляем запрос на получение вида на жительства
        :param message: сообщение запроса
        :return: ответ на запрос
        """
        my_message = helper.pack_to_str({'message_from_person':message})

        # предварительный ок
        self.response = None

        # ответ от экзаменационного центра
        self.response_from_exams = None

        # ответ от банка
        self.response_from_bank = None

        # финальный ответ
        self.final_response = None

        # флаг, если мы не дождались финального ответа
        self.is_final = False
        self.person_id = str(uuid.uuid4())
        # person_id для негативных test-case
        # self.person_id = 'c2772114-b159-402c-9e6c-ffdd35a7ad9e'
        # self.person_id = '8c460b99-d9a3-46bd-abb5-cd651a10310c'
        # self.person_id = '7a6099e2-bcf8-4b89-8287-9662cc8adbe9'
        # self.person_id = 'b1af25d2-dd3d-41b9-b53c-7e8cc0abc145'
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


        # # while self.response is None and self.is_final==False:
        # while self.response is None:
        #     while self.response_from_exams is None:
        #         while self.response_from_bank is None:
        #             while self.final_response==None and self.is_final==False:
        #             # while self.final_response == None:
        #                 self.connection.process_data_events()
        #             print(self.response)
        #         print (self.response_from_exams.decode())
        #     print(self.response_from_bank)
        # return self.final_response

        while not self.final_response and not self.is_final:
            self.connection.process_data_events()
        print(self.response)
        print(self.response_from_exams)
        print(self.response_from_bank)
        return self.final_response


person = PersonRpcClient()

print('[x] I want to get a residence permit')
response = person.call('I want to get a residence permit')
print(response)
