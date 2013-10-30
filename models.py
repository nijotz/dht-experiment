from uuid import uuid4
from sqlalchemy import create_engine, Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


# TODO: get database connection from config
engine = create_engine('postgresql+psycopg2://localhost:5432/parthnode')

def new_uuid():
    return str(uuid4())

class Message(Base):

    __tablename__ = 'message'

    guid = Column(String, primary_key=True, default=new_uuid)
    sender = Column(String, ForeignKey('node.guid'), nullable=False)
    receiver = Column(String, ForeignKey('node.guid'), nullable=False)
    message = Column(String, nullable=False)
    received = Column(Boolean)


class Node(Base):

    __tablename__ = 'node'

    guid = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    connection = Column(String)


class Share(Base):

    __tablename__ = 'share'
    
    id = Column(Integer, primary_key=True)
    message = Column(String, ForeignKey('message.guid'))
    node = Column(String, ForeignKey('node.guid'))
    timestamp = Column(DateTime)

Base.metadata.create_all(engine)
