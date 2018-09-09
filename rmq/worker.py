import pika
import json
from dbs import workconnect
from uhd_api import request_processing

user = {'Login': 'Ayaz', 'Password': '1111'}

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')

types = ['select', 'insert', 'update', 'create', 'delete']


def check_request(data):
    rights = workconnect.execute(f"select u.username, u.password_hash from \"user\" u where "
                                 f"u.username='{data['user']['Login']}' and u.password_hash='{data['user']['Password']}'")
    if rights.fetchone():
        request_type = data['request_type'].lower()
        if request_type in types:
            ind = types.index(request_type)
            return request_processing(data['request'], types[ind])
        else:
            return {'result': 'False', 'reason': 'request type is not correct'}
    else:
        return {'result': 'False', 'reason': 'login or password is not correct'}


def on_request(ch, method, props, body):
    data = json.loads(body.decode('utf-8'))
    result = check_request(data)
    ch.basic_publish(exchange='', routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=props.correlation_id),
                     body=json.dumps(result))
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(on_request, queue='hello')

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
