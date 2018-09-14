from dbs import dbs
from sqlalchemy.exc import *
from uuid import UUID
from datetime import datetime
import sqlalchemy
import json
from datetime import datetime


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            return obj.hex
        if isinstance(obj, datetime):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


par = "'"


def uhd_auth(data):
    login = data['Data']['Login']
    password = data['Data']['Password']
    session = data.get('SessionId')
    user_request = (
        f"select u.patronymic Patronymic, u.surname Surname, u.firstname, u.u_id UserId from users u where u.login='{login}' and u.password='{password}'")
    response = request_exec(user_request, dbs['working'])
    if response['Data']:
        user_id = response['Data'][0]['userid']
        form_request = (
            f"select c.cmname form, c.cmroute route from components c, comp_permissions p "
            f"where c.cm_id=p.cm_cm_id and p.u_u_id='{user_id}'")
        form_data = request_exec(form_request, dbs['working'])
        sess_exist_request = (f"select s.s_id from session s where s.u_u_id='{user_id}'")
        sess_data = request_exec(sess_exist_request, dbs['working'])
        if sess_data:
            s_id = sess_data['Data'][0]['s_id']
            sess_update_request = (f"update session set session = '{session}' where s_id='{s_id}' returning s_id")
            sess_update_response = request_exec(sess_update_request, dbs['working'])
            response['Data'][0]['AllowedForms'] = form_data['Data']
            response['Data'][0]['NewSessionID'] = sess_update_response['Data'][0]['s_id']
            response['Data'][0]['ForbiddenElements'] = []
            response['Data'][0]['Roles'] = []
            response['Data'] = response['Data'][0]
            return response
        sess_insert_request = (
            f"insert into session (u_u_id, session) values ('{user_id}', '{session}') returning s_id")
        sess_data = request_exec(sess_insert_request, dbs['working'])
        response['Data'][0]['AllowedForms'] = form_data['Data']
        response['Data'][0]['NewSessionID'] = sess_data['Data'][0]['s_id']
        response['Data'][0]['ForbiddenElements'] = []
        response['Data'][0]['Roles'] = []
        response['Data'] = response['Data'][0]
        return response


def uhd_create(data):
    request_data = data['request']
    if request_data.split()[0].lower() == 'create':
        return request_exec(request_data, connection=dbs[data['Database']])
    else:
        return {'Result': 'False', 'reason': 'request type and data command are not the same'}


def uhd_insert(data):
    request_data = data['Data']
    print(request_data)
    try:
        request = (f"insert into {request_data['ObjectType']} "
                   f"({', '.join([key for key, value in sorted(request_data['Content'].items()) if value])}) "
                   f"values ('{f'{par}, {par}'.join([value for key,value in sorted(request_data['Content'].items()) if value])}') "
                   f"returning {request_data['PrefixId']}")
    except KeyError as e:
        return {'Result': 'False', 'reason': f'unfilled request body field: {e}'}
    else:
        dt = request_exec(request, connection=dbs[request_data['Database']], objectType=request_data['ObjectType'],
                          sessionId=data['SessionId'])
        dt['ObjectId'] = dt[request_data['ObjectType']]['Data'][0]['uuid']
        print(dt)
        return dt


def uhd_update(data):
    request_data = data['Data']
    try:
        request = (f"update {request_data['ObjectType']} set "
                   f"({', '.join([key for key, value in sorted(request_data['Content'].items())])}) "
                   f"= (ROW('{f'{par}), ROW({par}'.join([value for key,value in sorted(request_data['Content'].items())])}')) "
                   f"where {request_data['PrefixId']}='{request_data['ObjectId']}'"
                   f"returning {request_data['PrefixId']}")
    except KeyError as e:
        return {'Result': 'False', 'reason': f'unfilled request body field: {e}'}
    else:
        dt = request_exec(request, connection=dbs[request_data['Database']], objectType=request_data['ObjectType'],
                          sessionId=data['SessionId'])
        dt['ObjectId'] = dt[request_data['ObjectType']]['Data'][0]['uuid']
        return dt


def uhd_delete(data):
    request_data = data['Data']
    try:
        request = (
            f"delete from {request_data['ObjectType']} where {request_data['PrefixId']}='{request_data['ObjectId']}' "
            f"returning {request_data['PrefixId']}")
    except KeyError as e:
        return {'Result': 'False', 'reason': f'unfilled request body field: {e}'}
    else:
        dt = request_exec(request, connection=dbs[request_data['Database']], objectType=request_data['ObjectType'],
                          sessionId=data['SessionId'])
        dt['ObjectId'] = dt[request_data['ObjectType']]['Data'][0]['uuid']
        return dt


def uhd_select(data):
    request_data = data['Data']['SQL']
    if request_data.split()[0].lower() == 'select':
        return request_exec(request_data, connection=dbs[data['Data']['Database']],
                            objectType=data['Data']['ObjectType'], sessionId=data['SessionId'])
    else:
        return {'Result': 'False', 'reason': 'request type and data command are not the same'}


def request_exec(request, connection=dbs['ontology'], objectType=None, sessionId=None):
    if isinstance(request, str):
        res = connection.execute(sqlalchemy.text(request))
        try:
            result = {
                'Data': [{key: value for (key, value) in i.items()} for i in
                         res]}
        except ProgrammingError as e:
            return {'Result': 'False', 'reason': f'request is not correct: {e}'}
        except ResourceClosedError:
            return {'Result': 'True'}
        else:
            if result['Data']:
                if objectType:
                    return {'Result': 'True', objectType: result, 'SessionId': sessionId}
                else:
                    result['Result'] = 'True'
                    return result
            else:
                return {'Result': 'False', 'reason': 'No objects found', objectType: {'Data': []}}
    else:
        return {'Result': 'False', 'reason': 'select request does not have str type'}


func_hub = {'create': uhd_insert, 'update': uhd_update, 'insert': uhd_insert, 'delete': uhd_delete,
            'select': uhd_select, 'auth': uhd_auth}


def request_processing(data, command):
    return json.dumps(func_hub[command](data), cls=UUIDEncoder)
