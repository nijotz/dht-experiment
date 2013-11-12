import json
import socket
import threading
import traceback
import SocketServer
import sys
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
        # False is for bind_and_activate, which will skip the socket bind on
        # init so that allow_reuse_address can be set on the socket which will
        # call socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1) which avoids
        # 'Address is already in use' errors when server crashes non-gracefully
        SocketServer.TCPServer.__init__(self, (node.host, node.port), handler_class, False)
        self.attach_node(node)
        self.allow_reuse_address = True
        # The above sets SO_REUSEADDR, but on OSX I need REUSEPORT too
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.server_bind()
        self.server_activate()

    def attach_node(self, node):
        self.node = node


class API(object):

    def __init__(self, node):
        self.node = node
        self.methods = {
            'ping': self.ping,
            'sync': self.sync,
        }

    def call_method(self, request_handler, command, args):
        self.methods[command](request_handler, *args)

    def ping(self, request_handler):
        request_handler.request.sendall('pong')

    def sync(self, request_handler, hashsum=None, host=None, port=None):
        # Try using the host:port of the sender if they don't send info
        self.node.sync_send(request_handler.request)


class DHTBase(object):
    """Subclass and extend to become a node"""

    def __init__(self, name, host, port, database=None):

        # The models to sync
        self.sync_models = [Node, Message]

        # Connect to datastore TODO: get from config
        if not database:
            database = name
        engine = create_engine('postgresql+psycopg2://localhost:5432/{}'.format(database))
        create_schema(engine) # verify our schema is correct

        Session = sessionmaker(bind=engine)
        self.session = Session()

        # Add self to node table
        node, new = get_or_create(self.session, Node, defaults=None, name=name)
        self.node = node
        self.node.name = name
        # TODO: get connection from config
        self.node.connection = '{}:{}'.format(host, port)
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
        "Return a callback called when new messages created in the database"
        # TODO: doesn't necessarily need to be the same process as the node server

        def new_message(mapper, connection, target):
            message = target
            if message.receiver == self.node.hashsum:
                self.receive_message(message)
        return new_message

    def receive_message(self, message):
        raise NotImplemented

    def send_message(self, receiver, message):
        msg = Message(receiver=receiver, message=message)
        msg.sender = self.node.hashsum
        self.session.add(msg)
        self.session.commit()

    def handle_request(self, request_handler, data):
        try:
            request = json.loads(data)
            command = request['command']
            args = getattr(request, 'args', [])
            self.api.call_method(request_handler, command, args)

        except Exception, e:
            (exc_type, exc_value, exc_traceback) = sys.exc_info()
            tb = traceback.format_exception(exc_type, exc_value,
                                          exc_traceback)
            request_handler.request.send('Error handling request: ' + \
                str(type(e)) + ' - ' + str(e) + ' - ' + repr(tb))

    def get_node_connection(self, sock=None, hashsum=None, host=None, port=None):
        if not sock:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            node = None

            # Try getting connection information using hashsum
            if hashsum is not None:
                node = self.session.query(Node).filter(hashsum=hashsum).all()
                node_host, node_port = node.connection.split(':')
                node_port = int(self.port)
                sock.connect((node_host, node_port))

            # If hashsum is not provided, just use host and port
            elif host is not None and port is not None:
                sock.connect((host, port))

            else:
                # TODO: exception here
                pass

        return sock

    def sync_with(self, **kwargs):
        "Initiate a sync with another node"

        sock = self.get_node_connection(**kwargs)
        sock.send(json.dumps({'command': 'sync'}) + '\n')
        self.sync_recv(sock)
        self.sync_send(sock, receive_after=False)
        sock.close()

    def sync_recv(self, sock):
        """Receive data from another node. Also the method for connecting to
        another node to receive data. When connecting to another node,
        sync_send is called after receiving data. Otherwise, just receive
        data."""

        # TODO: recieve more than 1024...
        json_data = json.loads(sock.recv(1024))

        for model in self.sync_models:
            model_data = json_data[model.__name__]
            for obj_dict in model_data:
                obj, new = get_or_create(self.session, model, defaults=None, **obj_dict)
                self.session.add(obj)

        self.session.commit()

    def sync_send(self, sock, receive_after=True):
        "Share data with another node, then receive data if flagged to."

        sync_data = {}
        for model in self.sync_models:
            objects = self.session.query(model).all()
            model_data = []
            for obj in objects:
                obj_dict = {}
                for column in model.__hashables__:
                    obj_dict[column] = getattr(obj, column)
                model_data.append(obj_dict)
            sync_data[model.__name__] = model_data

        sock.sendall(json.dumps(sync_data) + '\n')

        if receive_after:
            self.sync_recv(sock=sock)

    def start(self):
        self.thread.start()

    def stop(self):
        self.session.close_all()
        self.session.bind.dispose()
        self.server.shutdown()
