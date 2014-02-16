from antZoo.gossip import GossipServiceHandler

server = GossipServiceHandler.Server( "sample_server.cfg" )
server.serve()
