import json
import threading
import SocketServer
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import ClauseElement
from models import Message, Node, create_schema


# From http://stackoverflow.com/questions/2546207/does-sqlalchemy-have-an-equivalent-of-djangos-get-or-create
# with modifications
def get_or_create(session, model, defaults=None, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    else:
        params = dict((k, v) for k, v in kwargs.iteritems() if not isinstance(v, ClauseElement))
        if defaults is not None:
            params.update(defaults)
        instance = model(**params)
        session.add(instance)
        return instance, True


class DHTRequestHandler(SocketServer.StreamRequestHandler):

    def handle(self):
        data = self.rfile.readline().strip()
        self.server.node.handle_request(self, data)


class DHTServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):

    def __init__(self, node, handler_class):
        SocketServer.TCPServer.__init__(self, (node.host, node.port), handler_class)
        self.attach_node(node)


    def attach_node(self, node):
        self.node = node


class API(object):

    def __init__(self, node):
        self.node = node
        self.methods = {'ping': self.ping}

    def call_method(self, request_handler, command, args):
        self.methods[command](request_handler, *args)

    def ping(self, request_handler):
        request_handler.request.sendall('pong')


class DHTBase(object):
    """Subclass and extend to become a node"""

    def __init__(self, name, host, port):

        # Connect to datastore
        # TODO: get from config
        engine = create_engine('postgresql+psycopg2://localhost:5432/{}'.format(name))
        # TODO: not on init
        create_schema(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()

        # Add self to node table
        node, new = get_or_create(self.session, Node, defaults=None, name=name)
        self.node = node
        self.node.name = name
        # TODO: get connection from config
        self.node.connection = '{}:{}'.format(host, port)
        # TODO: get guid from config
        self.node.guid = name
        self.session.add(self.node)
        self.session.commit()

        # Setup listening for new messages being committed to the database
        # TODO: probably won't work multithreaded, need just one thread doing this
        event.listen(Message, 'after_insert', self.get_message_listener())

        # Setup socket server
        self.setup_server()

        # Setup API class for handling requests
        self.api = API(self)


    def setup_server(self):
        # TODO: repeat calls should clean up old servers/threads
        self.host, self.port = self.node.connection.split(':')
        self.port = int(self.port)
        self.server = DHTServer(self, DHTRequestHandler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.daemon = True


    def get_message_listener(self):
        def new_message(mapper, connection, target):
            message = target
            if message.receiver == self.node.guid:
                self.receive_message(message)
        return new_message


    def receive_message(self, message):
        raise NotImplemented


    def send_message(self, receiver, message):
        msg = Message(receiver=receiver, message=message)
        msg.sender = self.node.guid
        self.session.add(msg)
        self.session.commit()


    def handle_request(self, request_handler, data):
        try:
            request = json.loads(data)
            command = request['command']
            args = getattr(request, 'args', [])
            self.api.call_method(request_handler, command, args)

        except Exception, e:
            request_handler.request.sendall('{0}: {1}'.format(str(type(e)),  str(e)))


    def start(self):
        self.thread.start()


    def stop(self):
        self.server.shutdown()
