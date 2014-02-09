import threading
import random
import rpyc
import yaml
from kazoo.client import KazooClient
from docopt import docopt

"""Sean Reed

    Usage:
        ant run <job_file> <job_data> [--debug] [-c <config>]
        ant result <job_id> [--debug] [-c <config>]

        ant daemon [--debug] [-c <config>]
    
    Options:
        -h, --help      Show this screen.
        --debug         Turn on debugging.
        -c              Where to find the config.
"""

class ElectionRunner( threading.Thread ):
    def __init__( self, ant ):
        super( ElectionRunner, self ).__init__()
        self.ant = ant

    def run( self ):
        """
            This will allow for the main thread to continue to run.
            If the election says that this ant wins then call the
            _set_leader() method to switch over this ant to that
            job.
        """
        def set_leader():
            self.ant._set_leader()
        self.ant._election.run( set_leader )

class AntZooClientError( Exception ):
    pass

class AntZooHandler:

    def __init__( self, config ):

        self._config = yaml.load( open( config ) )
        self._id = self._config["id"]
        self.zk = KazooClient( hosts="/".join( self._config["zk"] ) )
        self._leader = False
        self._has_leader = False
        self._is_working = False
        self._is_running_for_leader = False
        self._election = None

        self.zoo_start()

    def zoo_start( self ):
        self.zk.start()
        self.zk.ensure_path( "/nodes" )
        self.zk.create( "/nodes/%s" % ( self._id ), ephemeral=True, makepath=True )

    def zoo_stop( self ):
        self.zk.stop()

    def new_job( self, job ):
        self.create_work_group( job.id )
        self.create_work_queue( job.id )
        self.join_work_group( job.id )
        self.setup_election( job.id )

    @property
    def is_leader( self ):
        return self._leader

    @property
    def leader_id( self ):
        if( self._has_leader ):
            return self._get_leader_id()
        else:
            return None

    def start_election( self, job_id ):
        if( not self._is_running_for_leader and not self._leader ):
            self._election = self.zk.Election( "/work_elections/%s" % job_id, self._id )
            self._is_running_for_leader = True
            self._last_job_id = job_id

            if( not self.zk.exists( "/work_election_signals/%s" % job_id ) ):
                self.zk.ensure_path( "/work_election_signals/%s" % ( job_id ) )
            self.zk.get( "/work_election_signals/%s" % job_id, watch=self._cancel_election )

            self._election_runner = ElectionRunner( self )
            self._election_runner.start()
        else:
            raise AntZooClientError( "Cannot start an election while this node is currently in the process of an election." )

    def create_work_group( self, group_id ):
        if( self.is_leader ):
            self._create_work_group( group_id )
        else:
            raise AntZooClientError( "Cannot create a group when not a leader." )

    def create_work_queue( self, queue_id ):
        if( self.is_leader ):
            self._create_work_queue( queue_id )
        else:
            raise AntZooClientError( "Cannot create a group when not a leader." )

    def join_work_group( self, group_id ):
        self._join_work_group( group_id )

    def _create_work_group( self, group_id ):
        """
            Creates the group as a permenant node
            then joins the group itself by creating a child as a ephemeral node.
        """
        self.zk.ensure_path( "/work_groups/%s" % group_id )
        self._join_work_group( group_id )

    def _join_work_group( self, group_id ):
        """
            Joins the group by creating an ephemeral node.
        """
        self.zk.ensure_path( "/work_groups/%s" % ( group_id ) )

        if( not self.zk.exists( "/work_groups/%s/%s" % ( group_id, self._id ) ) ):
            self.zk.create( "/work_groups/%s/%s" % ( group_id, self._id ), ephemeral=True )


    def _set_leader( self ):
        self.zk.set( "/work_election_signals/%s" % self._last_job_id, bytes( "%s" % self._id ) )
        self._election = None
        self._leader = True
        self._is_running_for_leader = False

    def _cancel_election( self, data ):
        print "Leader was found"
        if( self._election ):
            self._election.cancel()

