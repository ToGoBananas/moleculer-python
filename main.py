from moleculer.client import MoleculerClient, LOG_FORMAT
import logging


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    # Connect to localhost:5672 as guest with the password guest and virtual host "/" (%2F)
    service = MoleculerClient('amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat_interval=3600')
    service.run()


if __name__ == '__main__':
    main()
