from datetime import datetime
from functools import wraps
import json
import pep8
from random import random
import socket
import time
import unittest
from sqlalchemy import create_engine
from node import DHTBase
from models import Message, Node


def multiple(num=2):
    def wrap(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            for i in range(num):
                fn(*args, **kwargs)
        return wrapper
    return wrap


class DHTTest(DHTBase):

    def receive_message(self, message):
        self._message = message


class TestNode(unittest.TestCase):

    def setUp(self):
        engine = create_engine('postgresql+psycopg2://localhost:5432/postgres')
        self.db_conn = engine.connect()

        # postgres doesn't allow createdb inside transactions, commit to end
        self.db_conn.execute("commit")
        self.db_conn.execute("create database test_node1")
        self.db_conn.execute("commit")

        self.db_conn.execute("commit")
        self.db_conn.execute("create database test_node2")
        self.db_conn.execute("commit")

        self.node1 = DHTTest('node1', 'localhost', 1111, 'test_node1')
        self.node2 = DHTTest('node2', 'localhost', 1112, 'test_node2')
        self.node1.start()
        self.node2.start()

    @multiple(5)
    def test_nodes_can_respond_to_pings(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.node1.host, self.node1.port))
        sock.send(json.dumps({'command': 'ping'}) + '\n')
        self.assertTrue(sock.recv(1024) == 'pong')
        sock.close()

    def test_nodes_dont_pong_to_junk(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.node1.host, self.node1.port))
        sock.send(json.dumps({'command': 'sup mang?'}) + '\n')
        self.assertTrue(sock.recv(1024) != 'pong')
        sock.close()

    def test_nodes_dont_crash_on_junk(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.node1.host, self.node1.port))
        sock.send('junkgarbagebasura' + '\n')
        sock.close()
        self.test_nodes_can_respond_to_pings()

    @multiple(2)
    def test_nodes_store_records_of_each_other(self):
        self.node1.sync_with(host=self.node2.host, port=self.node2.port)
        time.sleep(1)

        node2_rows = self.node1.session.query(Node).filter(
            Node.hashsum == self.node2.node.hashsum).all()
        self.assertTrue(len(node2_rows) != 0)

        node1_rows = self.node2.session.query(Node).filter(
            Node.hashsum == self.node1.node.hashsum).all()
        self.assertTrue(len(node1_rows) != 0)

    @multiple(5)
    def test_nodes_share_messages(self):
        self.node1.sync_with(host=self.node2.host, port=self.node2.port)
        time.sleep(1)

        msg_txt = 'test message {0} {1}'.format(datetime.now(), str(random()))
        msg = Message(sender=self.node2.node.hashsum,
                      receiver=self.node1.node.hashsum,
                      message=msg_txt)

        self.node2.session.add(msg)
        self.node2.session.commit()

        self.node1.sync_with(host=self.node2.host, port=self.node2.port)
        time.sleep(1)
        node1_msgs = self.node1.session.query(Message).filter(
            Message.message == msg_txt).all()
        self.assertTrue(len(node1_msgs) != 0)

    def tearDown(self):
        self.node1.stop()
        self.node2.stop()

        self.db_conn.execute("commit")
        try:
            self.db_conn.execute("drop database test_node1")
        except Exception, e:
            print e

        self.db_conn.execute("commit")
        try:
            self.db_conn.execute("drop database test_node2")
        except Exception, e:
            print e

        self.db_conn.close()


class TestCodeFormat(unittest.TestCase):

    def test_pep8_compliance(self):
        pep8test = pep8.StyleGuide(quiet=True)
        result = pep8test.check_files(['node.py', 'tests.py', 'models.py'])
        self.assertEqual(result.total_errors, 0)
