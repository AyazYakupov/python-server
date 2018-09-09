from dbs import ontoconnect
from sqlalchemy.exc import ProgrammingError
par = "'"


def uhd_create(request_data):
    if request_data.split()[0].lower() == 'create':
        return request_exec(request_data)
    else:
        return {'result': 'False', 'reason': 'request type and data command are not the same'}


def uhd_insert(request_data):
    try:
        request = (f"insert into {request_data['table']} "
                   f"({', '.join([key for key, value in sorted(request_data['fields'].items())])}) "
                   f"values ('{f'{par}, {par}'.join([value for key,value in sorted(request_data['fields'].items())])}') "
                   f"returning {request_data['prefixId']}")
    except KeyError as e:
        return {'result': 'False', 'reason': f'unfilled request body field: {e}'}
    else:
        return request_exec(request)


def uhd_update(request_data):
    try:
        request = (f"update {request_data['table']} set "
                   f"({', '.join([key for key, value in sorted(request_data['fields'].items())])}) "
                   f"= ('{f'{par}, {par}'.join([value for key,value in sorted(request_data['fields'].items())])}') "
                   f"where {request_data['prefixId']}='{request_data['objectId']}'"
                   f"returning {request_data['prefixId']}")
    except KeyError as e:
        return {'result': 'False', 'reason': f'unfilled request body field: {e}'}
    else:
        return request_exec(request)


def uhd_delete(request_data):
    try:
        request = (f"delete from {request_data['table']} where {request_data['prefixId']}='{request_data['objectId']}' "
                   f"returning {request_data['prefixId']}")
    except KeyError as e:
        return {'result': 'False', 'reason': f'unfilled request body field: {e}'}
    else:
        return request_exec(request)


def uhd_select(request_data):
    if request_data.split()[0].lower() == 'select':
        return request_exec(request_data)
    else:
        return {'result': 'False', 'reason': 'request type and data command are not the same'}


def request_exec(request):
    if isinstance(request, str):
        try:
            result = {
                'result': [{key: value for (key, value) in i.items()} for i in
                           ontoconnect.execute(request)]}
        except ProgrammingError as e:
            return {'result': 'False', 'reason': f'request is not correct: {e}'}
        else:
            if result['result'][0].get('uuid'):
                return {'result': 'True', 'objectId': result['result'][0]['uuid'].hex}
            return result
    else:
        return {'result': 'False', 'reason': 'select request does not have str type'}


func_hub = {'create': uhd_create, 'update': uhd_update, 'insert': uhd_insert, 'delete': uhd_delete,
            'select': uhd_select}


def request_processing(request_data, command):
    return func_hub[command](request_data)
