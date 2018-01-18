import json
from .topics import EXCHANGES as MOLECULER_EXCHANGES
import datetime
from .service import request_handler, INFO_PACKET_TEMPLATE, event_handler


class MoleculerConsumer:

    def __init__(self, node_id):
        self.node_id = node_id
        self.is_node_discovered = False

    def discover(self, unused_channel, basic_deliver, properties, body):
        pass

    def info(self, channel, basic_deliver, properties, body):
        if not self.is_node_discovered:
            info_packet = json.loads(body)
            sender = info_packet['sender']
            if sender != self.node_id:  # TODO: send info package anyway
                info_packet = INFO_PACKET_TEMPLATE
                info_packet['sender'] = self.node_id
                info_packet['services'][0]['nodeID'] = self.node_id
                channel.basic_publish(MOLECULER_EXCHANGES['INFO'], '',  json.dumps(info_packet))
                self.is_node_discovered = True
        else:
            pass  # TODO: save discovered services

    def heartbeat(self, channel, basic_deliver, properties, body):
        pass

    def ping(self, channel, basic_deliver, properties, body):
        ping_packet = json.loads(body)
        sender_node_id, time = ping_packet['sender'], ping_packet['time']
        if sender_node_id != self.node_id:
            sender_exchange = 'MOL.PONG.{node_id}'.format(node_id=sender_node_id)
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
            'data': {'result': 'Response from python node'}
        }
        sender_exchange = 'MOL.RES.{node_id}'.format(node_id=sender)
        channel.basic_publish('', sender_exchange, json.dumps(response_packet))

    def disconnect(self, channel, basic_deliver, properties, body):
        # TODO: remove disconnected services DISCOVERED list
        pass

    def response(self, channel, basic_deliver, properties, body):
        channel.basic_ack(basic_deliver.delivery_tag)
        #  TODO: handle responses from other services

    def event(self, channel, basic_deliver, properties, body):
        event_packet = json.loads(body)
        sender, event, data = event_packet['sender'], event_packet['event'], event_packet['data']
        event_handler(sender, event, data)
