import random
import uuid

from flask import Flask, render_template
from flask.ext.wtf import Form
from wtforms import TextField
from wtforms.validators import Required

from ..gossipService.gossiping.ttypes import GossipData
from ..gossip import make_client

app = Flask( __name__ )

nodes = [
    { "address": "localhost", "port": 33000 },
    { "address": "localhost", "port": 33001 },
    { "address": "localhost", "port": 33002 },
]

class KeyValueForm( Form ):
    key = TextField( "Key", validators=[Required()] )
    value = TextField( "Value", validators=[Required()] )

@app.route( "/", methods=[ "GET", "POST" ] )
def index():
    form = KeyValueForm()

    print form.validate_on_submit()

    if( form.validate_on_submit() ):
        key = form.key.data
        value = form.value.data
        
        n = random.choice( nodes )

        c = make_client( n["address"], n["port"] )

        c.disseminate( GossipData( uuid=str( uuid.uuid4() ), key=key, value=value ) )
    
    return render_template( "index.html", form=form )

@app.route( "/nodes" )
def nodesView():
    ret = []

    for n in nodes:
        c = make_client( n["address"], n["port"] )

        data = c.getData()

        ret.append( ( "%s:%d" % ( n["address"], n["port"] ), data[:] ) )

    return render_template( "nodes.html", nodes=ret )
