import threading
import logging

logging.basicConfig( level=logging.INFO )
logger = logging.getLogger( __name__ )

class GossipServiceHeart( threading.Thread ):
    def __init__( self, gossipService ):
        super( GossipServiceHeart, self ).__init__()
        self.daemon = True

        self.gossipService = gossipService

        self.rounds = 0

    def run( self ):
        while True:
            try:
                self.rounds += 1
                self.gossipService.round()

                if( self.rounds % 7 == 0 ):
                    self.gossipService._queue.put( ( self.gossipService.exchangeViews, ( ), ) )
            except Exception as e:
                logger.info( "%s" % ( e ) )
                raise
            finally:
                time.sleep( self.gossipService._roundTime )
