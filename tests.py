import unittest 
from node import DHT


class TestNode(unittest.TestCase):

    def setUp(self):
        self.node1 = DHT('node1')
        self.node2 = DHT('node2')

    def test_node_can_send(self):
        self.node1.send_message(self.node2.node.guid, 'Hi node2!')
