import json
import datetime
from .service import request_handler, INFO_PACKET_TEMPLATE, event_handler


class MoleculerConsumer:

    def __init__(self, node_id, moleculer_topics, namespace=None):
        self.namespace = namespace
        self.moleculer_topics = moleculer_topics
        self.node_id = node_id
        self.is_node_discovered = False
        if self.namespace is None:
            self.info_template = 'MOL.INFO.{node_id}'
        else:
            self.info_template = 'MOL-{namespace}.INFO.{node_id}'
        if self.namespace is None:
            self.res_template = 'MOL.RES.{node_id}'
        else:
            self.res_template = 'MOL-{namespace}.RES.{node_id}'
        if self.namespace is None:
            self.pong_template = 'MOL.PONG.{node_id}'
        else:
            self.pong_template = 'MOL-{namespace}.PONG.{node_id}'

    def build_info_package(self):
        info_packet = INFO_PACKET_TEMPLATE
        info_packet['sender'] = self.node_id
        info_packet['services'][0]['nodeID'] = self.node_id
        return info_packet

    def discover(self, channel, basic_deliver, properties, body):
        discover_packet = json.loads(body)
        sender = discover_packet['sender']
        sender_queue = self.info_template.format(node_id=sender, namespace=self.namespace)
        info_packet = self.build_info_package()  # TODO: reuse same package
        channel.basic_publish('', sender_queue, json.dumps(info_packet))

    def info(self, channel, basic_deliver, properties, body):
        if not self.is_node_discovered:
            info_packet = json.loads(body)
            sender = info_packet['sender']
            if sender != self.node_id:  # TODO: send info package anyway
                info_packet = INFO_PACKET_TEMPLATE
                info_packet['sender'] = self.node_id
                info_packet['services'][0]['nodeID'] = self.node_id
                channel.basic_publish(self.moleculer_topics.exchanges['INFO'], '', json.dumps(info_packet))
                self.is_node_discovered = True
        else:
            pass  # TODO: save discovered services

    def heartbeat(self, channel, basic_deliver, properties, body):
        pass

    def ping(self, channel, basic_deliver, properties, body):
        ping_packet = json.loads(body)
        sender_node_id, time = ping_packet['sender'], ping_packet['time']
        if sender_node_id != self.node_id:
            sender_exchange = self.pong_template.format(node_id=sender_node_id, namespace=self.namespace)
            pong_packet = {
                'ver': '2',
                'sender': self.node_id,
                'time': time,
                'arrived': datetime.datetime.utcnow().timestamp(),
            }
            channel.basic_publish(sender_exchange, '', json.dumps(pong_packet))

    def pong(self, channel, basic_deliver, properties, body):
        pass

    def request(self, channel, basic_deliver, properties, body):
        channel.basic_ack(basic_deliver.delivery_tag)
        request_packet = json.loads(body)
        action, params, request_id, sender = request_packet['action'], request_packet['params'], \
                                             request_packet['id'], request_packet['sender']

        result = request_handler(action, params)
        response_packet = {
            'ver': '2',
            'sender': self.node_id,
            'id': request_id,
            'success': True,
            'data': {'result': 'Response from python node: ' + self.node_id}
        }
        sender_exchange = self.res_template.format(node_id=sender, namespace=self.namespace)
        channel.basic_publish('', sender_exchange, json.dumps(response_packet))

    def disconnect(self, channel, basic_deliver, properties, body):
        # TODO: remove disconnected services DISCOVERED list
        pass

    def response(self, channel, basic_deliver, properties, body):
        # channel.basic_ack(basic_deliver.delivery_tag)
        pass
        #  TODO: handle responses from other services

    def event(self, channel, basic_deliver, properties, body):
        # print('EVENT!!!!')
        event_packet = json.loads(body)
        sender, event, data = event_packet['sender'], event_packet['event'], event_packet['data']
        event_handler(sender, event, data)
