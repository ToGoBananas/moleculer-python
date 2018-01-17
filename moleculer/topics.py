EXCHANGES = {
    'DISCOVER': 'MOL.DISCOVER',
    'INFO': 'MOL.INFO',
    'HEARTBEAT': 'MOL.HEARTBEAT',
    'PING': 'MOL.PING',
    'DISCONNECT': 'MOL.DISCONNECT'
}


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

    def __init__(self, node_id):
        for queue_name in self.queue_attrs:
            setattr(self, queue_name, getattr(MoleculerTopics, queue_name).format(node_id=node_id))

    @property
    def bindings(self):
        result = {}
        for queue_type, queue_name in self.queues.items():
            if queue_type in EXCHANGES:
                result[queue_name] = EXCHANGES[queue_type]
        return result
