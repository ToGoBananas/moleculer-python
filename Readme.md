### Usage:

Each node should run instance of `MoleculerNode` class. Node id can be specified on initialization.

To configure node you must edit `moleculer/service.py` file.

Service configuration in `INFO_PACKET_TEMPLATE`

To handle requests from other nodes (call, dcall), you must implement `request_handler` function.

To handle events (emit, broadcast), you must implement `event_handler`.

