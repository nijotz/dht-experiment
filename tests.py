import json
import socket
import unittest 
from node import DHTBase


class DHTTest(DHTBase):

    def receive_message(self, message):
        self._message = message


class TestNode(unittest.TestCase):

    # Only setup once (don't create a socket server for every test)
    @classmethod
    def setUpClass(cls):
        cls.node1 = DHTTest('node1', 'localhost', 1111)
        cls.node2 = DHTTest('node2', 'localhost', 1112)
        cls.node1.start()
        cls.node2.start()


    def test_nodes_can_respond_to_pings(self, numruns=0):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.node1.host, self.node1.port))
        sock.send(json.dumps({'command':'ping'}) + '\n')
        self.assertTrue(sock.recv(1024) == 'pong')
        sock.close()

        # Try pinging a few times in a row
        if numruns < 5:
            self.test_nodes_can_respond_to_pings(numruns=numruns+1)

    @classmethod
    def tearDownClass(cls):
        # TODO: sessions everywhere!
        cls.node1.session.delete(cls.node1.node)
        cls.node2.session.delete(cls.node2.node)
        cls.node1.stop()
        cls.node2.stop()
