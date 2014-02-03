import random
import rpyc
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

class AntZooClientError( Exception ):
    pass

class AntZooClient( object ):

    def __init__( self, config )
        sekf.zk = KazooClient( hosts=hosts )

        self._client = client
        self._leader = False
        self._has_leader = False
        self._is_working = False

    @property
    def is_leader( self ):
        return self._leader

    @property
    def leader_id( self ):
        if( self._has_leader ):
            return self._get_leader_id()
        else:
            return None

    def start_election( self, election_id ):
        pass

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
        self.zk.create( "/work_groups/%s" % group_id )
        self._join_work_group( group_id )

    def _join_work_group( self, group_id ):
        """
            Joins the group by creating an ephemeral node.
        """
        self.zk.create( "/work_groups/%s/%s" % ( group_id, self._id ), ephemeral=True )

class AntDaemon( rpyc.Service ):
    """
        An Ant must possess the following functionality:
            - Communicate with other nodes in a gossiping fashion.
            - Start elections
            - Perform work

        This is the daemon service that runs on the ant servers
        to process jobs.

        To facilitate this, an ant uses ZooKeeper to keep track and
        perform most of those tasks.

        An Ant does the following when it gets a job request.

            It first will create a new job_group with a generated
            ID. After this has occured a message to it's local
            view will be sent into the cluster recruiting servers
            to come work with it. At this point nodes will randomly
            determine if they will join the work group. If they do then
            they also join the group based on the group id. Once a 
            number greater than 2 nodes has joined a group then an 
            election will occur. Once this election occurs the newly
            found leader will then determine based on the work load,
            in our case the work is the number of lines in the data file,
            and figure out the total number of nodes allowed and create 
            a semaphor. This semaphor will count down to 0 and once it
            has reached 0 the group has been formed. At this point
            the leader will create the work queue and the workers
            will join the work queue and begin processing tasks. 
            At this point each node will broadcast to all of it's fellow
            works work results so that they are durably stored in the 
            event that they were to go down. In the case of the leader
            failing then a new election is started and the queue is
            processed as normally. Once the queue is empty then the
            leader gathers the results, it should have all of them,
            and then will make it known that the client can gather the
            results by getting the node with the data from the ZooKeeper
            by checking the result key.


    """

    zk = None
    
    def on_connect( self ):
        if( not self.zk ):
            arguments = docopt( __doc__, versions="Ant 0.1" )

            self.config = self._read_config( arguments["-c"] )
            self.zk = AntZooClient( self.config )
            self.zk.start()
            self._connect_to_peers()

    def _connect_to_peers( self ):
        if( self.zk.exists( "/peers" ) )
            peers = self.zk.get_children( "/peers", watch=self._change_in_peers )
            peers = random.sample( peers, self.config["fanout"] )

            self._create_peer_connections( peers )
            self._send_hello_to_peers()
        else:
            self._peers = []

    def _create_peer_connections( peers )
        self._peers = []

        for p in peers:
            peer = rpyc.connect( p, 18861 )
            self._peers.append( peer )

    def _send_hello_to_peers( self ):
        """
            This method wakes up this nodes peers.
        """
        if( self._peers ):
            for p in self._peers:
                p.hello()


    def exposed_hello( self ):
        """
            This method is called to wake up this service. Is
            used to help boostrap the service.
        """
        pass

    def exposed_new_job( self, job_tuple ):
        """
            When a new job comes in a 4-tuple is sent to the
            node. 

            ( job_id, result_id, data_location, code_location )
        """

        #A ant cannot be a leader of a job or working currently
        #to accept a new job, if they are then they must find
        #another node to perform the work.
        if( not self._is_working and not self.is_leader ):
            job_id, result_id, data_location, code_location = job_tuple
            self.zk.create_work_group( job_id )
            self.zk.join_work( job_id )
            self._recruit_for_work( job_id )
        else:
            self._find_new_job_sponsor( job_tuple )

    def _recruit_for_work( self, job_id ):
        if( self._peers ):
            for p in peers:
                p.recruit_for( job_id )

    def exposed_recruit_for( self, job_id ):
        if( not self.is_leader ):
            switch = random.random()
            threshold = self._is_working ? 0.7: 0.3

            if( switch > threshold ):
                self.zk.leave_work_group()
                self.zk.join_work_group( job_id )
            

