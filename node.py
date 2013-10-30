from sqlalchemy.orm import sessionmaker
from models import Message, Node, engine


class DHT(object):
    """Subclass and extend to become a node"""

    def __init__(self, name):

        # Connect to datastore
        Session = sessionmaker(bind=engine)
        self.session = Session()

        # Add self to node table
        self.node = Node()
        self.node.name = name
        # TODO: get connection from config
        self.node.connection = 'localhost:1337'
        # TODO: get guid from config
        self.node.guid = name
        self.session.add(self.node)
        self.session.commit()

    def receive_message(self, sender, message):
        raise NotImplemented

    def send_message(self, receiver, message):
        msg = Message(receiver=receiver, message=message)
        msg.sender = self.node.guid
        self.session.add(msg)
        self.session.commit()
