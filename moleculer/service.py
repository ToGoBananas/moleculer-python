import json


def service_builder(name, cache=False, params=None):
    """

    :param name:
    :param cache:
    :param params: dict. Example: {'name': {'optional': bool, 'type': 'typeof'}}
    :return:
    """

    result = {
        'cache': cache,
        'name': name,
    }
    if params is not None:
        result['params'] = params
    return result


def request_handler(action: str, params: dict) -> bool:
    with open('1.txt', 'w') as f:
        f.write(action)
        f.write('\n')
        f.write(json.dumps(params))
    return True


def event_handler(sender_node_id: str, event: str, payload: dict):
    pass


INFO_PACKET_TEMPLATE = {
    'ver': '2',
    'sender': None,
    'services': [{
        'actions': {'test': service_builder('test')},  # DO NOT name like service_name.action name. Just action name
        'events': {'event_test': {'name': 'event_test'}},
        'metadata': {},
        'name': '$python',
        'nodeID': None,
        'settings': {}
    }],
    'config': {},
    'ipList': ['127.0.0.1', ],
    'port': None,
    'client': {'langVersion': '3.6.3', 'type': 'python', 'version': '0.0.1'},
}
