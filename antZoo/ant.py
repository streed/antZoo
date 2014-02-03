
from kazoo.client import KazooClient


class Ant( object ):

    def __init__( self, name, hosts="127.0.0.1:2181" )
        self.name = name
        sekf.zk = KazooClient( hosts=hosts )
