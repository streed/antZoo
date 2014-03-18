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
        self._leader = False
        self._has_leader = False
        self._is_working = False
        self._is_running_for_leader = False

    def new_job( self, job ):
        self.create_work_group( job.id )
        self.create_work_queue( job.id )
        self.join_work_group( job.id )
        self.setup_election( job.id )

    def process_task( self, task ):
      return None

    def get_update( self, task ):
      return self._get_status()

    def signal_new_job( self, job ):
      self._start_new_job( job )

    @property
    def is_leader( self ):
        return self._leader

    @property
    def leader_id( self ):
        if( self._has_leader ):
            return self._get_leader_id()
        else:
            return None


