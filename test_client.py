import uuid
from antZoo.gossipService.gossiping.ttypes import GossipData
from antZoo.gossip import make_client

c = make_client( "localhost", 33000 )
d = GossipData( uuid=str( uuid.uuid4() ), key="lol", value="lol2" )
c = make_client( "localhost", 33000 )
c.disseminate( d )

