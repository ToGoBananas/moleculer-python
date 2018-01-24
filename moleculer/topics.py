from .service import INFO_PACKET_TEMPLATE


EXCHANGES = {
    'DISCOVER': 'MOL.DISCOVER',
    'INFO': 'MOL.INFO',
    'HEARTBEAT': 'MOL.HEARTBEAT',
    'PING': 'MOL.PING',
    'DISCONNECT': 'MOL.DISCONNECT'
}

REQB = 'MOL.REQB.{service_name}.{action}'
REQB_NAMESPACE = 'MOL-{namespace}.REQB.{service_name}.{action}'
EVENTB = 'MOL.EVENTB.{service_name}.{event}'
EVENTB_NAMESPACE = 'MOL-{namespace}.EVENTB.{service_name}.{event}'


class MoleculerTopics:
    EVENT_QUEUE = 'MOL.EVENT.{node_id}'
    REQUEST_QUEUE = 'MOL.REQ.{node_id}'
    RESPONSE_QUEUE = 'MOL.RES.{node_id}'
    PONG_QUEUE = 'MOL.PONG.{node_id}'
    INFO_QUEUE = 'MOL.INFO.{node_id}'
    PING_QUEUE = 'MOL.PING.{node_id}'
    DISCONNECT_QUEUE = 'MOL.DISCONNECT.{node_id}'
    DISCOVER_QUEUE = 'MOL.DISCOVER.{node_id}'
    HEARTBEAT_QUEUE = 'MOL.HEARTBEAT.{node_id}'

    @property
    def queue_attrs(self):
        return [attr for attr in MoleculerTopics.__dict__ if attr.endswith('_QUEUE')]

    @property
    def queues(self):
        result = {}
        for attr in self.queue_attrs:
            result[attr.replace('_QUEUE', '')] = getattr(self, attr)
        return result

    def __init__(self, node_id, namespace=None):
        for queue_name in self.queue_attrs:
            if namespace is None:
                setattr(self, queue_name, getattr(MoleculerTopics, queue_name).format(node_id=node_id))
            else:
                queue_string = getattr(MoleculerTopics, queue_name).format(node_id=node_id)
                queue_string = queue_string.replace('MOL', 'MOL-' + namespace)
                setattr(self, queue_name, queue_string)
        self.namespace = namespace
        self.exchanges = self.generate_exchanges()

    @property
    def bindings(self):
        result = {}
        for queue_type, queue_name in self.queues.items():
            exchanges = self.exchanges
            if queue_type in exchanges:
                result[queue_name] = exchanges[queue_type]
        return result

    @property
    def action_queues(self):
        if self.namespace is None:
            template = REQB
        else:
            template = REQB_NAMESPACE
        result = []
        for service in INFO_PACKET_TEMPLATE['services']:
            service_name = service['name']
            for action in service['actions'].keys():
                result.append(template.format(service_name=service_name, action=action, namespace=self.namespace))
        return result

    @property
    def event_queues(self):
        if self.namespace is None:
            template = EVENTB
        else:
            template = EVENTB_NAMESPACE
        result = []
        for service in INFO_PACKET_TEMPLATE['services']:
            service_name = service['name']
            for event in service['events'].keys():
                result.append(template.format(service_name=service_name, event=event, namespace=self.namespace))
        return result

    def generate_exchanges(self):
        if self.namespace is None:
            return EXCHANGES
        else:
            return {key: val.replace('MOL', 'MOL-' + self.namespace) for key, val in EXCHANGES.items()}
