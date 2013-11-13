from hashlib import sha1
import json
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


def hash_default(hashables):
    """Return a SQLAlchemy default callback. Hash the fields for the row being
    added in the 'context' variable"""

    def hash_from_fields(context):
        data = [context.current_parameters[field] for field in
                sorted(hashables)]
        json_data = json.dumps(data)
        return sha1(json_data).hexdigest()

    return hash_from_fields


class Message(Base):

    __tablename__ = 'message'
    __hashables__ = ['sender', 'receiver', 'message']

    hashsum = Column(String, primary_key=True,
                     default=hash_default(__hashables__))
    sender = Column(String, ForeignKey('node.hashsum'), nullable=False)
    receiver = Column(String, ForeignKey('node.hashsum'), nullable=False)
    message = Column(String, nullable=False)


class Node(Base):

    __tablename__ = 'node'
    __hashables__ = ['name', 'connection']

    hashsum = Column(String, primary_key=True,
                     default=hash_default(__hashables__))
    name = Column(String, nullable=False)
    connection = Column(String)


class Share(Base):

    __tablename__ = 'share'

    id = Column(Integer, primary_key=True)
    message = Column(String, ForeignKey('message.hashsum'))
    node = Column(String, ForeignKey('node.hashsum'))
    share_start = Column(DateTime)
    share_end = Column(DateTime)


def create_schema(engine):
    Base.metadata.create_all(engine)
