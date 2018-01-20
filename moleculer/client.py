from .node import MoleculerNode, LOGGER
from pika.channel import Channel
import json


class MoleculerClient(MoleculerNode):

    def __init__(self, amqp_url):
        super().__init__(amqp_url, node_id='PYTHON-CLIENT')
        self.network = NetworkInfo()

    def on_channel_open(self, channel):
        LOGGER.info('Channel opened')
        self.channel: Channel = channel
        self.add_on_channel_close_callback()
        info_queue = 'MOL.INFO.{node_id}'.format(node_id=self.NODE_ID)
        self.setup_queue(info_queue)
        self.channel.queue_bind(self.on_bindok, info_queue, 'MOL.INFO')
        self.channel.basic_consume(self.process_info_packages, info_queue)
        self.discover_packet()

    def process_info_packages(self, unused_channel, basic_deliver, properties, body):
        info_packet = json.loads(body)
        self.network.add_node(info_packet)

    def emit(self, event_name, data=None):
        candidates = self.get_emit_candidates(event_name)
        if len(candidates) == 0:
            return {'error': 'This event not registered.'}
        else:
            if data is None:
                data = {}
            event_package = MoleculerClient.build_event('PYTHON-CLIENT', event_name, data)
            for service_name in candidates:
                queue_name = 'MOL.EVENTB.{service}.{event}'.format(service=service_name, event=event_name)
                self.channel.basic_publish('', queue_name, event_package)

    def broadcast(self, event_name, data=None):
        candidates = self.get_broadcast_candidates(event_name)
        if len(candidates) == 0:
            return {'error': 'This event not registered.'}
        else:
            if data is None:
                data = {}
            event_package = MoleculerClient.build_event('PYTHON-CLIENT', event_name, data)
            for node_id in candidates:
                queue_name = 'MOL.EVENT.{node_id}'.format(node_id=node_id)
                self.channel.basic_publish('', queue_name, event_package)

    def get_emit_candidates(self, event_name):
        service_names = set()
        for node_id, node_info in self.network.NODES.items():
            if event_name in node_info['events']:
                service_name = node_info['service_name']
                service_names.add(service_name)
        return service_names

    def get_broadcast_candidates(self, event_name):
        candidates = []
        for node_id, node_info in self.network.NODES.items():
            if event_name in node_info['events']:
                candidates.append(node_id)
        return candidates

    @staticmethod
    def build_event(sender_node_id, event_name, payload):
        event = {
            'ver': '2',
            'sender': sender_node_id,
            'event': event_name,
            'data': payload,
            'groups': []
        }
        return json.dumps(event)


class NetworkInfo:
    NODES = {}

    def add_node(self, info_packet: dict):
        node_id = info_packet['sender']
        if node_id not in self.NODES.keys():
            self.NODES[node_id] = {
                'actions': {},
                'events': []
            }
            node = self.NODES[node_id]
            for service in info_packet['services']:
                service_name = service['name']
                is_service_node = bool(service_name == '$node')
                for action_name, action_spec in service['actions'].items():
                    if is_service_node:
                        queue_name = 'MOL.REQB.{action}'.format(action=action_name)
                    else:
                        queue_name = 'MOL.REQB.{service_name}.{action}'.format(service_name=service_name,
                                                                               action=action_name)
                    node['actions'][action_name] = queue_name

                for event_name in service['events'].keys():
                    node['events'].append(event_name)
            else:
                node['service_name'] = service_name

    def disconnect_node(self, node_id):
        del self.NODES[node_id]
