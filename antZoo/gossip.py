import copy
import logging
import random
import threading
import time
import yaml
import sys

from pybloom import BloomFilter
from Queue import Queue
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from .gossipService.gossiping.Gossiping import Client, Processor
from .gossipService.gossiping.ttypes import GossipStatus, GossipNode, GossipData, GossipNodeView
from .gossipheart import GossipServiceHeart
from .utils import make_client, destroy_client

logging.basicConfig( level=logging.INFO )
logger = logging.getLogger( __name__ )



class GossipServiceHandler( object ):
  THRESHOLD = 0.0001

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
    self._queue = Queue()

    logger.info( "Sleeping between rounds for %f seconds." % self._roundTime )

    self._status = GossipStatus.IDLE
    self._node = GossipNode( self.config["address"], int( self.config["port"] ), self._status )

    self._id = "%s:%s" % ( self.config["address"], self.config["port"] )
    
    self.reload_nodes()

    self._heart = GossipServiceHeart( self )
    self._heart.start()

    self.jobs = []
    self._leader = False

  @classmethod
  def Server( cls, config ):
    handler = cls( config )
    processor = Processor( handler )
    transport = TSocket.TServerSocket( port=int( handler.config["port"] ) )
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer( processor, transport, tfactory, pfactory );

    return server

  def round( self ):
    """
          This is called in the heart thread, so it can block.
    """
    try:
      message = self._queue.get( block=False, timeout=self._pulseTicks )
      message[0]( *message[1] )
    except Exception as e:
      if( not str( e ) == "" ):
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logger.info( "%s" % ( e ) )
        raise

  def view( self, view ):
    """
        This will take in the nodeList from
        the other peer and then randomly merge
        the two lists.

        {
          neighborhood: { "b": [ "a", "c" ], 
                          "c": [ "a", "b" ],
                        }
                  view: [
                          "b",
                          "c"
                        ]
        }
    """
    ret = self._view
    self._view = GossipNodeView()

    self._view.view = ret.view[:]
    self._view.neighborhood = copy.deepcopy( ret.neighborhood )
    self._view.owner = self._id

    for k in view.view:
      if k in self._view.neighborhood:
        if not view.owner in self._view.neighborhood[k]:
          self._view.neighborhood[k].append( view.owner )
      else:
        self._view.neighborhood[k] = [ view.owner ]

    if len( self._view.view ) < self._fanout:
      self._view.view.append( view.owner )

    return ret

  def get_view( self ):
    return self._view
 
  def new_job( self, job ):
    """
        This will start the Ant server and
        start the recruiting process for the 
        job itself. 

        This process entails the following:
            - Sending out a broadcast message to recruit workers. 
            - Starting the Ant server.
    """
    if( not self._leader ):
      self._leader = True
      #Spawn the ant server first.
      self._spawn_ant()

      #Setup the job before the recruitment process.
      self._ant_client.new_job( job )

      #Recruit ants for the job.
      #self._queue.push( ( self._recruit, ( job, ), ) )
      self.recruit( job )

  def recruit( self, job ):
    if( not data.uuid in self.messages and not self._leader ):
      data.hops += 1
      job_tuple = ( math.exp( -( data.hops - data-priority ) ), data, )
      logger.info( job_tuple )

      if( job_tuple[0] >= THRESHOLD )
        self.jobs.append( job )
        self.jobs.sort( key=lambda a: a[0] )

        njob = self.jobs[0]

        if( self.ant == None ):
          self.spawn_ant( job_tuple )
        else:
          self._ant_client.signal_new_job( njob[1] )

        self._queue.put( ( self._recruit, ( job, ), ) ) 
        self.messages.add( data.uuid )

  def _recruit( self, job ):
    logger.info( "Recruiting: %s" % job )

    for n in self._view.view:
      logger.info( n )
      c = make_client( n )
      c.recruit( job )

      destroy_client( c )

  def disseminate( self, data ):
      elif( isinstance( data, GossipData ) ):
        self.storage[data.key] = data.value

      self._queue.put( ( self._disseminate, ( data, ), ) )
      self.messages.add( data.uuid )

  def _disseminate( self, data ):
    logger.info( "Disseminating: %s" % data )

    for n in self._view.view:
      logger.info( n )
      c = make_client( n )
      c.disseminate( data )

      destroy_client( c )

    logger.info( "Done disseminating." )

  def getData( self ):
    data = [ GossipData( uuid="", key=k, value=v ) for k, v in self.storage.iteritems() ]

    return data

  def _added_to_view( self ):
    logger.info( "Requesting that I be added to my view's zk lists." )

    for n in self._view.view:
      c = make_client( n )
      c.added_to_view( self._node )

      destroy_client( c )

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

      server = TServer.TThreadedServer( processor, transport, tfactory, pfactory )

      self._ant_server = server
      self._ant_client = self._ant_server.make_client()

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

  def exchangeViews( self ):
    for n in self._view.view:
      c = make_client( n )
      logger.info( "Connecting to %s" % n )
      c.view( self._view )

  def reload_nodes( self ):
    self._view = self._load_saved_list()
    self._view.owner = self._id

  def _load_saved_list( self ):
    nodeList = yaml.load( open( self.config["node_list"] ) )
    ret = GossipNodeView()

    ret.neighborhood = nodeList["neighborhood"]
    ret.view = nodeList["view"]

    return ret

  def _save_nodes( self ):
    out = { "neighborhood": self._view.neighborhood, "view": self._view.view }

    with open( self.config["node_list"], "w" ) as f:
      f.write( yaml.dump( out ) )


