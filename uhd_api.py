from dbs import dbs
from sqlalchemy.exc import *
from uuid import UUID
import json

sample = "{'Query': 'AUTH', 'SessionId': '16e92264-da94-4267-8144-f94f3aac5c57', 'Data': {'Login': 'admin', 'Password': 'admin'}}"


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            return obj.hex
        return json.JSONEncoder.default(self, obj)


par = "'"


def uhd_auth(data):
    login = data['Data']['Login']
    password = data['Data']['Password']
    session = data.get('SessionId')
    user_request = (
        f"select u.patronymic Patronymic, u.surname Surname, u.u_id UserId from users u where u.login='{login}' and u.password='{password}'")
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
            response['Data'][0]['NewSessionId'] = sess_update_response['Data'][0]['s_id']
            response['Data'] = response['Data'][0]
            return response
        sess_insert_request = (
            f"insert into session (u_u_id, session) values ('{user_id}', '{session}') returning s_id")
        sess_data = request_exec(sess_insert_request, dbs['working'])
        response['Data'][0]['AllowedForms'] = form_data['Data']
        response['Data'][0]['NewSessionId'] = sess_data['Data'][0]['s_id']
        response['Data'] = response['Data'][0]
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
    request_data = data['Request']
    try:
        request = (f"update {request_data['table']} set "
                   f"({', '.join([key for key, value in sorted(request_data['fields'].items())])}) "
                   f"= ('{f'{par}, {par}'.join([value for key,value in sorted(request_data['fields'].items())])}') "
                   f"where {request_data['prefixId']}='{request_data['objectId']}'"
                   f"returning {request_data['prefixId']}")
    except KeyError as e:
        return {'Result': 'False', 'reason': f'unfilled request body field: {e}'}
    else:
        return request_exec(request, connection=dbs[data['Database']])


def uhd_delete(data):
    request_data = data['request']
    try:
        request = (f"delete from {request_data['table']} where {request_data['prefixId']}='{request_data['objectId']}' "
                   f"returning {request_data['prefixId']}")
    except KeyError as e:
        return {'Result': 'False', 'reason': f'unfilled request body field: {e}'}
    else:
        return request_exec(request, connection=dbs[data['Database']])


def uhd_select(data):
    callData = {'BPSET': {'Columns': ['name_class', 'a_uuid', 'a_name', 'a_alias', 'a_status', 'a_code', 'a_atomar'],
                          'Data': [{'a_alias': '', 'a_atomar': '', 'a_code': '', 'a_name': 'MyProcess',
                                    'a_status': 'В архиве', 'a_uuid': 'bc8dc0f0-a13c-11e8-be65-e3051ce7b7e2',
                                    'name_class': 'MyProcess'},
                                   {'a_alias': '', 'a_atomar': '1', 'a_code': '004', 'a_name': 'Test13',
                                    'a_status': 'Неактивен', 'a_uuid': 'f853327e-acee-11e8-be65-5f0e8f0ca98f',
                                    'name_class': 'Test13'},
                                   {'a_alias': '', 'a_atomar': '0', 'a_code': '', 'a_name': '1221',
                                    'a_status': 'Неактивен', 'a_uuid': 'bc37701a-aaba-11e8-a45d-cfd86e1a6664',
                                    'name_class': '1221'},
                                   {'a_alias': '', 'a_atomar': '0', 'a_code': '011', 'a_name': 'Test11',
                                    'a_status': 'Неактивен', 'a_uuid': 'da0e794c-ab66-11e8-8b83-4fa2fe7b92a1',
                                    'name_class': 'Test11'}]}, 'Result': 'true',
                'SessionId': 'fd0bf936-e241-4e5f-bf1c-f059a98e575f'}
    samp = {"Query": "SELECT", "SessionId": "fd0bf936-e241-4e5f-bf1c-f059a98e575f",
            "Data": {"Database": "ontology", "ObjectType": "BPSET",
                     "SQL": "SELECT o.name_class, o.uuid a_uuid,(SELECT l.value FROM olink l LEFT JOIN oattribute a ON l.dst_attribute_id = a.uuid WHERE l.src_class_id = o.uuid AND a.tech_name_attr='Name' LIMIT 1) a_Name,(SELECT l.value FROM olink l LEFT JOIN oattribute a ON l.dst_attribute_id = a.uuid WHERE l.src_class_id = o.uuid AND a.tech_name_attr='Alias' LIMIT 1) a_Alias,(SELECT l.value FROM olink l LEFT JOIN oattribute a ON l.dst_attribute_id = a.uuid WHERE l.src_class_id = o.uuid AND a.tech_name_attr='Status' LIMIT 1) a_Status, (SELECT l.value FROM olink l LEFT JOIN oattribute a ON l.dst_attribute_id = a.uuid WHERE l.src_class_id = o.uuid AND a.tech_name_attr='Code' LIMIT 1) a_Code, (SELECT l.value FROM olink l LEFT JOIN oattribute a ON l.dst_attribute_id = a.uuid WHERE l.src_class_id = o.uuid AND a.tech_name_attr='Atomar' LIMIT 1) a_Atomar FROM oclass o WHERE o.type_class = 'BP'and o.uuid IN (SELECT l.src_class_id FROM olink l JOIN oattribute a ON l.dst_attribute_id = a.uuid WHERE a.tech_name_attr IN ('Name','Alias','Keywords','Description'))and (SELECT l.value FROM olink l LEFT JOIN oattribute a ON l.dst_attribute_id = a.uuid WHERE l.src_class_id = o.uuid AND a.tech_name_attr='Status' LIMIT 1) NOT LIKE 'Удалён' "}}
    request_data = data['Data']['SQL']
    if request_data.split()[0].lower() == 'select':
        return request_exec(request_data, connection=dbs[data['Data']['Database']], objectType=data['Data']['ObjectType'], sessionId=data['SessionId'])
    else:
        return {'Result': 'False', 'reason': 'request type and data command are not the same'}


def request_exec(request, connection=dbs['ontology'], objectType=None, sessionId=None):
    if isinstance(request, str):
        res = connection.execute(request)
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
                return {'Result': 'False', 'reason': 'no objects found'}
    else:
        return {'Result': 'False', 'reason': 'select request does not have str type'}


func_hub = {'create': uhd_create, 'update': uhd_update, 'insert': uhd_insert, 'delete': uhd_delete,
            'select': uhd_select, 'auth': uhd_auth}


def request_processing(data, command):
    return json.dumps(func_hub[command](data), cls=UUIDEncoder)
