import pika
import datetime
import json
import uuid
import pprint

pp = pprint.PrettyPrinter(indent=4)

cl = 'feaabac6-af5e-11e8-bbc6-f30400728df0'
# select test
sel_request = ("select l.value, a.tech_name_attr from olink l "
               f"inner join oattribute a on l.dst_attribute_id=a.uuid where l.src_class_id='{cl}'")
sel_req = json.dumps(
    {'user': {'Login': 'admin', 'Password': 'admin'}, 'request': sel_request, 'Query': 'select',
     'Database': 'ontology'})

# insert test
ins_request = {'table': 'oclass', 'prefixId': 'uuid',
               'fields': {'name_class': 'new_test', 'tech_name_class': 'new_test_tech', 'type_class': 'TEST'}}
ins_req = json.dumps(
    {'user': {'Login': 'admin', 'Password': 'admin'}, 'request': ins_request, 'Query': 'insert',
     'Database': 'ontology'})

# update test
upd_request = {'table': 'oclass', 'objectId': 'ea5dc948b42a11e89f989cb6d0ddcf3b', 'prefixId': 'uuid',
               'fields': {'name_class': 'updated_test', 'tech_name_class': 'updated_test_tech', 'type_class': 'TEST'}}
upd_req = json.dumps(
    {'user': {'Login': 'admin', 'Password': 'admin'}, 'request': upd_request, 'Query': 'update',
     'Database': 'ontology'})

del_request = {'table': 'oclass', 'objectId': 'ea5dc948b42a11e89f989cb6d0ddcf3b', 'prefixId': 'uuid'}
del_req = json.dumps(
    {'user': {'Login': 'admin', 'Password': 'admin'}, 'request': del_request, 'Query': 'delete',
     'Database': 'ontology'})

auth_req = json.dumps({'user': {'Login': 'admin', 'Password': 'admin'}, 'Query': 'auth'})

host = '192.168.88.88'
isSSL = 'false'
login = 'test'
passwd = 'test'
port = 5672


class RMQClient:
    def __init__(self):

        credentials = pika.PlainCredentials(username=login, password=passwd)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=port, credentials=credentials))
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(self.on_response, no_ack=True, queue=self.callback_queue)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='', routing_key='hello',
                                   properties=pika.BasicProperties(reply_to=self.callback_queue,
                                                                   correlation_id=self.corr_id), body=str(n))
        while self.response is None:
            self.connection.process_data_events()
        return self.response


rm = RMQClient()


def test(name, amount, req):
    t1 = datetime.datetime.now()
    for i in range(amount):
        response = rm.call(req)
        pp.pprint(json.loads(response.decode('utf-8')))
    t2 = datetime.datetime.now() - t1
    print(f'{name}: {t2}')


test('first', 1000, sel_req)
