import logging
import random
import threading
import yaml

from Queue import Queue

from .gossipService.gossiping.ttypes import GossipStatus, GossipNode

logger = logging.getLogger( __name__ )

class GossipServiceHeart( threading.Thread ):
    def __init__( self, gossipService ):
        super( GossipServiceHandler, self ).__init__()

        self.gossipService = gossipService

    def run( self ):
        self.gossipService.round()

        thread.Sleep( self.gossipService._roundTime )

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

        self._nodeList = self._load_saved_list()

        self._zk = KazooClient( hosts="/".join( self.config["zk_hosts"] ) )
        self.zoo_start()

        self._lock = threading.Lock()
        self._queue = Queue()

        self._heart = GossipServiceHeart( self )
        self._heart.start()


    def zoo_start( self ):
        logger.info( "Starting ZooKeeper session" )
        self._zk.start()

    def zoo_stop( self ):
        logger.info( "Ending the ZooKeeper session." )
        self._zk.stop()

    def round( self ):
        """
            This is called in the heart thread, so it can block.
        """
        message = self._queue.get()

        message[0]( *message[1] )

    def view( self, nodeList ):
        """
            This will take in the nodeList from
            the other peer and then randomly merge
            the two lists.
        """
        ret = self._nodeList[:]
        self._lock.acquire()

        logger.info( "Merging remote view with my view." )

        pool = nodeList + self._nodeList 
        self._nodeList = random.choice( pool, self._fanout )

        self._lock.release()

        return ret
   
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

        #Setup the job before the recruitment process.
        self._ant_client.new_job( job )

        #Recruit ants for the job.
        self._queue.push( self._recruit, ( job, ) )

    def recruit( self, job, ant ):
        #Are we a leader already? If yes leaders cannot switch.
        if( not self.is_leader ):
            switch = random.random()
            threshold = 0.7 if self._is_working else 0.3

            if( switch > threshold ):
                self.zk.leave_work_group()
                self.zk.join_work_group( job_id )


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

            #This is kind of a hack
            #Subclass the TServer and allow
            #for graceful shutdown.
            class AntThread( threading.Thread ):
                def __init__( self, gossip ):
                    super( AntThread, self ).__init__()
                    self.gossip = gossip

                def run( self ):
                    self.gossip._ant_server.serve()

            self._ant = AntThread( self )
            self._ant.run()
        else:
            logger.info( "Ant is already running." )

    def _load_saved_list( self ):
        nodeList = yaml.load( open( self.config["node_list"] ) )
        ret = []

        for n in nodeList["nodes"]:
            ret.append( GossipNode( address=n["address"], port=n["port"], status=n["status"] ) )

        return ret

    def _save_nodes( self ):
        out = []

        for n in self.nodeList:
            out.append( { "address": n.address, "port": n.port, "status": n.status } )

        with open( self.config["node_list"], "w" ) as f:
            f.write( yaml.dump( { "nodes": out } ) )

