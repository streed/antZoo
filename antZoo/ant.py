import threading
import random
import yaml
import subprocess
import sys
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
    self._job = subprocess.Popen( self.codeLocation.split( " " ),  stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.STDOUT )


  def send( self, line ):
    self._job.stdin.write( line )
    self._job.stdin.flush()
    out = self._job.stdout.readline()
    return out

  def finish( self ):
    self._job.communicate( input="ANT_JOB_DONE\n" )

class AntJobRunner( threading.Thread ):

  def __init__( self, ant ):
    super( AntJobRunner, self ).__init__()
    self._queue = Queue()
    self._new_job = threading.Event()
    self._is_accepting_work = threading.Event()
    self._job_signal = threading.Event()
    self.daemon = True
    self.job = None
    self.ant = ant

  def run( self ):
    while True:
      if( self._new_job.wait() ):
        self._new_job.clear()
        self.job = self._queue.get()
        self._run()

      #if( self.job ):
      #  self._signal_leader_leaving( self.id )
      #  self._stop_job()
      #  self.job = None

  def push( self, job ):
    self._new_job.set()
    self._queue.put( job )

  def _run( self ):
    class Runner( threading.Thread ):
      def __init__( self, runner ):
        super( Runner, self ).__init__()
        self.runner = runner
        self.tasks = Queue()

      def run( self ):
        self.runner._is_accepting_work.set()
        job = self.runner.job
        self._run_job( job )

        while not self.runner._job_signal.is_set() and not self.tasks.empty():
          task = self.tasks.get( block=False, timeout=2 )
          out = self._job.send( task )
          print out
          #self.runner.ant.send_job_response( out )

        self._job.finish()

      def _run_job( self, job ):
        self._job = self._spawn( job.source )

      def _spawn( self, job_code ):
        return AntScriptRunner( job_code )

    self._runner = Runner( self )
    self._runner.start()

  def _stop_job( self ):
    #self._is_accepting_work.wait()
    #self._job_signal.set()
    pass

  def finish( self ):
    self._stop_job()

  def new_task( self, task ):
    self._is_accepting_work.wait()
    self._runner.tasks.put( task )

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


