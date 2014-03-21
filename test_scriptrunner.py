from antZoo.ant import AntJobRunner

ant = AntJobRunner( None )

ant.start()

class Job:
  def __init__( self, source ):
    self.source = source

j = Job( "/Users/elchupa/code/school/antZoo/localenv/bin/python /Users/elchupa/code/school/antZoo/example_code/word_count.py" )

ant.push( j )

for i in range( 100 ):
  ant.new_task( "this is a test\n" )

print "Done sending tasks."
ant.finish()
ant._runner.join()

j = Job( "/Users/elchupa/code/school/antZoo/localenv/bin/python /Users/elchupa/code/school/antZoo/example_code/word_count.py" )

ant.push( j )

for i in range( 100 ):
  ant.new_task( "this is another test that is to be run :D\n" )

print "Done sending tasks."
ant.finish()
ant._runner.join()
