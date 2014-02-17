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
]

class KeyValueForm( Form ):
    key = TextField( "Key", validators=[Required()] )
    value = TextField( "Value", validators=[Required()] )

@app.route( "/", methods=[ "GET", "POST" ] )
def index():
    form = KeyValueForm()

    if( form.validate_on_submit() ):
        key = form.key.data
        value = form.value.data
        
        n = random.choice( nodes )

        c = make_client( n["address"], n["port"] )

        c.disseminate( GossipData( uuid=str( uuid.uuid4() ), key=key, value=value ) )

    nodes2 = []
    views = []

    for n in nodes:
        try:
            c = make_client( n["address"], n["port"] )

            data = c.getData()
            view = c.get_view()

            for v in view:
                i = { "address": v.address, "port": v.port }
                if not i in nodes:
                    nodes.append( i )

            nodes2.append( ( "%s:%d" % ( n["address"], n["port"] ), data[:], view ) )
        except:
            pass
 
    return render_template( "index.html", form=form, nodes=nodes2 )

@app.route( "/nodes" )
def nodesValues():
    ret = []

    for n in nodes:
        try:
            c = make_client( n["address"], n["port"] )

            data = c.getData()

            ret.append( ( "%s:%d" % ( n["address"], n["port"] ), data[:] ) )
        except:
            pass

    return render_template( "nodes.html", nodes=ret )

@app.route( "/views" )
def nodesView():
    ret = []

    for n in nodes:
        c = make_client( n["address"], n["port"] )
        view = c.get_view()

        vv = []

        for v in view:
            vv.append( "%s:%d" % ( v.address, v.port ) )

        ret.append( ( "%s:%d" % ( n["address"], n["port"] ), vv, ) )

    return render_template( "views.html", nodes=ret )

