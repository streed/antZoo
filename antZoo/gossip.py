import logging
import random
import threading
import yaml

from gossip.ttypes import GossipStatus, 

logger = logging.getLogger( __name__ )

class GossipServiceHandler:
    def __init__( self, config ):
        self.messages = {}
        self.config = yaml.load( open( config ) )
        self._fanout = int( self.config["fanout"] )
        self._tick = int( self.config["tick"] )
        self._pulseTicks = int( self.config["pulseTicks"] )
        self._roundTime = self._tick * self._pulseTicks

        self._status = GossipStatus.IDLE
        self._node = GossipNode( self.config["address"], int( self.config["port"] ), self._status )

        self._nodeList = self.load_saved_list( yaml.load( self.config["node_list"] ) )

        self._zk = KazooClient( hosts="/".join( self.config["zk_hosts"] ) )
        self.zoo_start()

        self._lock = threading.Lock()

    def zoo_start( self ):
        logger.info( "Starting ZooKeeper session" )
        self._zk.start()

    def zoo_stop( self ):
        logger.info( "Ending the ZooKeeper session." )
        self._zk.stop()

    def view( self, nodeList ):
        """
            This will take in the nodeList from
            the other peer and then randomly merge
            the two lists.
        """
        ret = None
        self._lock.acquire()

        logger.info( "Merging remote view with my view." )

        pool = nodeList + self._nodeList 
        self._nodeList = random.choice( pool, self._fanout )

        self._lock.release()

        return self._nodeList
   
   def new_job( self, job ):
       """
            This will start the Ant server and
            start the recruiting process for the 
            job itself. 

            This process entails the following:
                - Sending out a broadcast message to recruit workers. 
                - Starting the Ant server.
        """

        #Spawn the ant server first.
        self._spawn_ant()

        #Recruit ants for the job.
        self._recruit( job )

    def _recruit( self, job ):
        self._lock.acquire()
        logger.info( "Recruting Ants to work on the new job." )

        ant = Ant( self._node, int( self.config["ant_port"] ) )

        for n in self._nodeList:
            n.recruit( job, ant )

        self._lock.release()

    def _spawn_ant( self ):
        """
            Spawn the Ant server so that the job can be processed.
        """
        if( not self._ant_running ):
            handler = AntZooServiceHandler( self )
            processor = AntZooService.Processor( handler )
            transport = TSocket.TServerSocket( self.config["ant_port"] )
            tfactory = TTransport.TBufferedTrasnportFactory()
            pfactory = TBinaryProtocol.TBinaryProtocolFactory()

            server = TServer.TSimpleServer( processor, transport, tfactory, pfactory )

            self._ant_server = server

            class AntThread( threading.Thread ):
                def __init__( self, gossip ):
                    self.gossip = gossip

                def run( self ):
                    self.gossip._ant_server.serve()

            self._ant = AntThread( self )
            self._ant.run()
        else:
            logger.info( "Ant is already running." )

