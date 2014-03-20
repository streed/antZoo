import threading
import random
import rpyc
import yaml
from Queue import Queue
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

class AntScriptRunner( object ):
  def __init__( self, codeLocation ):
    self.codeLocation = codeLocation

    self._job = subprocess.Popen( self.codeLocation, stdout=subprocess.PIPE )

  def send( self, line ):
    return self._job.communicate( input=line )[0]

  def finish( self ):
    self._job.communicate input="ANT-JOB-DONE\n" )

class AntJobRunner( threading.Thread ):

  def __init__( self, ant ):
    self._queue = Queue()
    self._new_job = threading.Event()
    self.daemon = True
    self.job = None
    self.ant = ant

  def run( self ):
    while True:
     if( sef._new_job.wait() ) 
      self._new_job.clear()

      if( self.job ):
        self._signal_leader_leaving( self.id )
        self._stop_job()
        self.job = None

      self.job = self._queue.pop()

      self._run()

  def push( self, job ):
    self._queue.push( job )
    self._signal.set()

  def _run( self ):
    class Runner( threading.Thread ):
      def __init__( self, runner ):
        self.runner = runner
        self.tasks = Queue()
      def run( self ):
        job = self.runner.job
        self._run_job( job )
        while not self.runner._job_signal.is_set():
          task = self.tasks.get()
          out = self._job.send( task.line )
          self.runner.ant.send_job_response( out )

    def _run_job( self, job ):
      self._job = self._spawn( job.source )

    def _spawn( self, job_code ):
      return AntScriptRunner( job_code )

  def _stop_job( self ):
    self._job_signal.set()

  def new_task( self, task ):
    self._runner.tasks.push( task )

class AntZooHandler:

    def __init__( self, ant ):
      self._job_handler = AntJobRunner( ant )

      self._job_handler.start()

    def new_job( self, job ):
      self._job = job
      self._job_handler.push( job )
      self._job_handler._new_job.set()

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


