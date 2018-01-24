from moleculer.client import MoleculerClient
import threading
import time


def main():
    client = MoleculerClient('amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat_interval=3600')
    t1 = threading.Thread(target=client.run)  # run loop in separate thread
    t1.start()

    # wait for initialization
    time.sleep(5)  # TODO: extract 'ready' event from thread
    client.emit('event_test')
    client.broadcast('event_test')


if __name__ == '__main__':
    main()
