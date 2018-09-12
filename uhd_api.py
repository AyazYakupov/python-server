from dbs import dbs
from sqlalchemy.exc import ProgrammingError

par = "'"


def uhd_auth(data):
    login = data['user']['Login']
    password = data['user']['Password']
    sessionId = data['sessionId']
    forms_data = (f"select c.cmname form, c.cmroute route from components c, comp_permissions p, users u "
                    f"where c.cm_id=p.cm_cm_id and p.u_u_id=u.u_id and u.login='{login}' and u.password='{password}'")
    # sessions_data = (f"-- insert into session ")
    return request_exec(forms_data, connection=dbs['working'])


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
        try:
            result = {
                'result': [{key: value for (key, value) in i.items()} for i in
                           connection.execute(request)]}
        except ProgrammingError as e:
            return {'result': 'False', 'reason': f'request is not correct: {e}'}
        else:
            if result['result'][0].get('uuid'):
                return {'result': 'True', 'objectId': result['result'][0]['uuid'].hex}
            return result
    else:
        return {'result': 'False', 'reason': 'select request does not have str type'}


func_hub = {'create': uhd_create, 'update': uhd_update, 'insert': uhd_insert, 'delete': uhd_delete,
            'select': uhd_select, 'auth': uhd_auth}


def request_processing(data, command):
    return func_hub[command](data)
