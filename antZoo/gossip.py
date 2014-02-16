import logging
import random
import threading
import time
import yaml


from pybloom import BloomFilter
from Queue import Queue
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from .gossipService.gossiping.Gossiping import Client, Processor
from .gossipService.gossiping.ttypes import GossipStatus, GossipNode, GossipData

logging.basicConfig( level=logging.INFO )
logger = logging.getLogger( __name__ )

def make_client( address, port ):
    transport = TSocket.TSocket( address, port )
    transport = TTransport.TBufferedTransport( transport )
    protocol = TBinaryProtocol.TBinaryProtocol( transport )
    client = Client( protocol )
    transport.open()

    return client


class GossipServiceHeart( threading.Thread ):
    def __init__( self, gossipService ):
        super( GossipServiceHeart, self ).__init__()
        self.daemon = True

        self.gossipService = gossipService

        self.rounds = 0

    def run( self ):
        while True:
            self.rounds += 1
            self.gossipService.round()

            time.sleep( self.gossipService._roundTime )

            if( self.rounds % 5 == 0 ):
                #self.gossipService._save_nodes()
                self.gossipService.attemptBadNodes()

class GossipServiceHandler( object ):
    def __init__( self, config ):
        #create a bloom filter with a capaticy of a million messages
        #with an error rate of 0.000001%
        self.messages = BloomFilter( capacity=1000000, error_rate = 0.0001 )
        self.storage = {}
        self.config = yaml.load( open( config ) )
        self._fanout = int( self.config["fanout"] )
        self._tick = int( self.config["tick"] ) / 1000
        self._pulseTicks = int( self.config["pulseTicks"] )
        self._roundTime = self._tick * self._pulseTicks
        logger.info( "Sleeping between rounds for %f seconds." % self._roundTime )

        self._status = GossipStatus.IDLE
        self._node = GossipNode( self.config["address"], int( self.config["port"] ), self._status )

        self._nodeList, self._nodeClientList, self._badNodesList = self._load_saved_list()

        #self._zk = KazooClient( hosts="/".join( self.config["zk_hosts"] ) )
        #self.zoo_start()

        self._lock = threading.Lock()
        self._queue = Queue()

        self._heart = GossipServiceHeart( self )
        self._heart.start()

        #self._setup_zk_watch()

    @classmethod
    def Server( cls, config ):
        handler = cls( config )
        processor = Processor( handler )
        transport = TSocket.TServerSocket( port=int( handler.config["port"] ) )
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()

        server = TServer.TThreadedServer( processor, transport, tfactory, pfactory );

        return server

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
        try:
            message = self._queue.get( block=False, timeout=self._pulseTicks )

            logger.info( message )

            message[0]( *message[1] )
        except Exception as e:
            if( not str( e ) == "" ):
                logger.info( e )

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
        self._queue.push( ( self._recruit, ( job, ), ) )

    def recruit( self, job, ant ):
        #Are we a leader already? If yes leaders cannot switch.
        if( not self.is_leader ):
            switch = random.random()
            threshold = 0.7 if self._is_working else 0.3

            if( switch > threshold ):
                self.zk.leave_work_group()
                self.zk.join_work_group( job_id )

    def added_to_view( self, node ):
        #Add ourselves to this node's view. Make it so when/if we die our node is automatically
        #deleted.
        self._zk.create( "/nodes/views/%s/%s" % ( node.id, self.config["id"] ), ephemeral=True )


    def disseminate( self, data ):
        if( not data.uuid in self.messages ):
            logger.info( "Queuing, disseminate" )
            self._queue.put( ( self._disseminate, ( data, ), ) )
            logger.info( "Storing value." )
            self.storage[data.key] = data.value
            logger.info( "Add the data.uuid to the self.messages bloom filter" )
            self.messages.add( data.uuid )

    def _disseminate( self, data ):
        self._lock.acquire()

        logger.info( "Disseminating %s => %s" % ( data.key, data.value ) )

        for n in self._nodeClientList:
            n.disseminate( data )

        logger.info( "Done disseminating." )
        self._lock.release()

    def getData( self ):
        data = [ GossipData( uuid="", key=k, value=v ) for k, v in self.storage.iteritems() ]

        return data

    def _added_to_view( self ):
        self._lock.acquire()
        logger.info( "Requesting that I be added to my view's zk lists." )

        for n in self._nodeClientList:
            n.added_to_view( self._node )

        self._lock.release()



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

    def attemptBadNodes( self ):
        newBadList = []
        for b in self._badNodesList:
            try:
                logger.info( "Attempting to reconnect to %s:%d" % ( b.address, b.port ) )
                c = make_client( b.address, b.port )
                self._nodeClientList.append( c )
                self._nodeList.append( b )
            except:
                logger.info( "Attempted to reconnect to node: %s:%d" % ( b.address, b.port ) )
                newBadList.append( b )

        self._badNodesList = newBadList


    def _load_saved_list( self ):
        nodeList = yaml.load( open( self.config["node_list"] ) )
        ret = []
        ret2 = []

        bad_nodes = []

        for n in nodeList["nodes"]:
            try: 
                c = make_client( n["address"], n["port"] )
                ret.append( GossipNode( address=n["address"], port=n["port"], status=n["status"] ) )
                ret2.append( c )
            except:
                logger.info( "Could not connect to node: %s:%d" % ( n["address"], n["port"] ) )
                bad_nodes.append( GossipNode( address=n["address"], port=n["port"], status=n["status"] ) )

        return ret, ret2, bad_nodes

    def _save_nodes( self ):
        out = []

        for n in self._nodeList:
            out.append( { "address": n.address, "port": n.port, "status": n.status } )

        with open( self.config["node_list"], "w" ) as f:
            f.write( yaml.dump( { "nodes": out } ) )

    def _setup_zk_watch( self ):
        #Create the parent node.
        self._zk.create( "/nodes/views/%s" % self.config["id"], ephemeral=True )

        #Watch for changes.
        self._zk.ChildrenWatch( "/nodes/views/%s" % self.config["id"], self._zk_view_change )

        #Queue a message to be sent to the view.
        self._queue.push( self._added_to_view, () )

    def _zk_view_change( self, children ):
        pass
        

