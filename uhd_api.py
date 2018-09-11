from dbs import dbs
from sqlalchemy.exc import *
from uuid import UUID
import json


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            return obj.hex
        return json.JSONEncoder.default(self, obj)


par = "'"


def uhd_auth(data):
    login = data['user']['Login']
    password = data['user']['Password']
    session = data.get('sessionId')
    user_request = (f"select * from users u where u.login='{login}' and u.password='{password}'")
    response = request_exec(user_request, dbs['working'])
    if response['result']:
        user_id = response['result'][0]['u_id']
        form_request = (
            f"select c.cmname form, c.cmroute, u.login route from components c, comp_permissions p, users u "
            f"where c.cm_id=p.cm_cm_id and p.u_u_id='{user_id}' and u.u_id='{user_id}'")
        form_data = request_exec(form_request, dbs['working'])
        sess_exist_request = (f"select s.s_id from session s where s.u_u_id='{user_id}'")
        sess_data = request_exec(sess_exist_request, dbs['working'])
        if sess_data:
            s_id = sess_data['result'][0]['s_id']
            sess_update_request = (f"update session set session = '{session}' where s_id='{s_id}' returning s_id")
            sess_update_response = request_exec(sess_update_request, dbs['working'])
            response['result'].extend(form_data['result'])
            response['result'].extend(sess_update_response['result'])
            return response
        sess_insert_request = (f"insert into session (u_u_id, session) values ('{user_id}', '{session}')")
        sess_data = request_exec(sess_insert_request, dbs['working'])
        response['result'].extend(form_data['result'])
        response['result'].extend(sess_data['result'])
        return response


def uhd_create(data):
    request_data = data['request']
    if request_data.split()[0].lower() == 'create':
        return request_exec(request_data, connection=dbs[data['Database']])
    else:
        return {'result': 'False', 'reason': 'request type and data command are not the same'}


def uhd_insert(data):
    request_data = data['request']
    try:
        request = (f"insert into {request_data['table']} "
                   f"({', '.join([key for key, value in sorted(request_data['fields'].items())])}) "
                   f"values ('{f'{par}, {par}'.join([value for key,value in sorted(request_data['fields'].items())])}') "
                   f"returning {request_data['prefixId']}")
    except KeyError as e:
        return {'result': 'False', 'reason': f'unfilled request body field: {e}'}
    else:
        return request_exec(request, connection=dbs[data['Database']])


def uhd_update(data):
    request_data = data['request']
    try:
        request = (f"update {request_data['table']} set "
                   f"({', '.join([key for key, value in sorted(request_data['fields'].items())])}) "
                   f"= ('{f'{par}, {par}'.join([value for key,value in sorted(request_data['fields'].items())])}') "
                   f"where {request_data['prefixId']}='{request_data['objectId']}'"
                   f"returning {request_data['prefixId']}")
    except KeyError as e:
        return {'result': 'False', 'reason': f'unfilled request body field: {e}'}
    else:
        return request_exec(request, connection=dbs[data['Database']])


def uhd_delete(data):
    request_data = data['request']
    try:
        request = (f"delete from {request_data['table']} where {request_data['prefixId']}='{request_data['objectId']}' "
                   f"returning {request_data['prefixId']}")
    except KeyError as e:
        return {'result': 'False', 'reason': f'unfilled request body field: {e}'}
    else:
        return request_exec(request, connection=dbs[data['Database']])


def uhd_select(data):
    request_data = data['request']
    if request_data.split()[0].lower() == 'select':
        return request_exec(request_data, connection=dbs[data['Database']])
    else:
        return {'result': 'False', 'reason': 'request type and data command are not the same'}


def request_exec(request, connection=dbs['ontology']):
    if isinstance(request, str):
        res = connection.execute(request)
        try:
            result = {
                'result': [{key: value for (key, value) in i.items()} for i in
                           res]}
        except ProgrammingError as e:
            return {'result': 'False', 'reason': f'request is not correct: {e}'}
        except ResourceClosedError:
            return {'result': 'True'}
        else:
            if result['result']:
                return result
            else:
                return {'result': 'False', 'reason': 'no objects found'}
    else:
        return {'result': 'False', 'reason': 'select request does not have str type'}


func_hub = {'create': uhd_create, 'update': uhd_update, 'insert': uhd_insert, 'delete': uhd_delete,
            'select': uhd_select, 'auth': uhd_auth}


def request_processing(data, command):
    return json.dumps(func_hub[command](data), cls=UUIDEncoder)
