from kazoo.client import KazooClient, KazooState

zk = KazooClient( hosts="127.0.0.1:2181" )


def my_listener( state ):
    if state == KazooState.LOST:
        print "Lost connection"
    elif state == KazooState.SUSPENDED:
        print "Disconnected from zooKeeper"
    else:
        print "Stuff.", state

zk.add_listener( my_listener )

zk.start()
