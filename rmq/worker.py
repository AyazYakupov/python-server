import pika
import json
from dbs import workconnect
from uhd_api import request_processing

# user = {'Login': 'admin', 'Password': 'admin'}
host = '192.168.88.89'
isSSL = 'false'
login = 'test'
passwd = 'test'
port = 5672

credentials = pika.PlainCredentials(username=login, password=passwd)
connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port, credentials=credentials))
channel = connection.channel()

channel.queue_declare(queue='helloIn')

types = ['select', 'update', 'create', 'delete', 'auth', 'query']


def check_request(data):
    print(data)
    request_type = data['Query'].lower()
    if request_type in types:
        ind = types.index(request_type)
        if request_type == 'auth':
            return request_processing(data, types[ind], )
        else:
            # rights = workconnect.execute(f"select s.session from session s where "
            #                              f"s.session='{data['SessionId']}'")
            # if rights.fetchone():
            return request_processing(data, types[ind], )
            # else:
            #     return {'Result': 'False', 'reason': 'session is not exist'}
    else:
        return {'Result': 'False', 'reason': 'Query is not correct', data['Data']['ObjectType']: {'Data': []}}


def on_request(ch, method, props, body):
    data = json.loads(body.decode('utf-8'))
    result = check_request(data)
    print(result)
    try:
        ch.basic_publish(exchange='', routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id=props.correlation_id),
                         body=result)
    except Exception as e:
        print(f'Error is: {e}')
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=10)
channel.basic_consume(on_request, queue='helloIn')

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
