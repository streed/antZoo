from flask import Flask, render_template
from flask.ext.wtf import Form

from ..gossip import make_client

app = Flask( __name__ )

nodes = [
    { "address": "localhost", "port": 33000 },
    { "address": "localhost", "port": 33001 },
    { "address": "localhost", "port": 33002 },
]

@app.route( "/" )
def index():
    return render_template( "index.html" )

@app.route( "/nodes" )
def nodesView():
    ret = []

    for n in nodes:
        c = make_client( n["address"], n["port"] )

        data = c.getData()

        ret.append( ( "%s:%d" % ( n["address"], n["port"] ), data[:] ) )

    return render_template( "nodes.html", nodes=ret )
