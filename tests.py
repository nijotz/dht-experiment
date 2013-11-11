import json
import socket
from sqlalchemy import create_engine
import unittest
from node import DHTBase
from models import Node


class DHTTest(DHTBase):

    def receive_message(self, message):
        self._message = message


class TestNode(unittest.TestCase):

    # Only setup once (don't create a socket server for every test)
    def setUp(self):
        engine = create_engine('postgresql+psycopg2://localhost:5432/postgres')
        self.db_conn = engine.connect()

        #postgres doesn't allow create db inside a transaction, commits end them
        self.db_conn.execute("commit")
        self.db_conn.execute("create database test_node1")
        self.db_conn.execute("commit")

        self.db_conn.execute("commit")
        self.db_conn.execute("create database test_node2")
        self.db_conn.execute("commit")

        self.node1 = DHTTest('node1', 'localhost', 1111, database='test_node1')
        self.node2 = DHTTest('node2', 'localhost', 1112, database='test_node2')
        self.node1.start()
        self.node2.start()

    def test_nodes_can_respond_to_pings(self, numruns=0):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.node1.host, self.node1.port))
        sock.send(json.dumps({'command': 'ping'}) + '\n')
        self.assertTrue(sock.recv(1024) == 'pong')
        sock.close()

        # Try pinging a few times in a row
        if numruns < 5:
            self.test_nodes_can_respond_to_pings(numruns=numruns+1)

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

    def test_nodes_store_records_of_each_other(self):
        self.node1.sync_send(host=self.node2.host, port=self.node2.port)
        node2_rows = self.node1.session.query(Node).filter(Node.guid == self.node2.node.guid).all()
        self.assertTrue(len(node2_rows) != 0)

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
