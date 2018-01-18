import pika


class MoleculerClient:

    def __init__(self, username='guest', password='guest', host='localhost'):
        url = 'amqp://{username}:{password}@{host}:5672/%2F?connection_attempts=3&heartbeat_interval=3600'.format(
            username=username, password=password, host=host
        )
        connection = pika.BlockingConnection(pika.URLParameters(url))
        self.channel = connection.channel()

    def publish_event(self, node_id=None, balanced=False, event_name=None):
        if balanced:
            pass