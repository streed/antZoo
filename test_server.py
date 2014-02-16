import sys
from antZoo.gossip import GossipServiceHandler

server = GossipServiceHandler.Server( sys.argv[1] )
server.serve()
