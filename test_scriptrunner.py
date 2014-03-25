from antZoo.ant import AntJobRunner

ant = AntJobRunner( None )

ant.start()

class Job:
  def __init__( self, source ):
    self.source = source

j = Job( "/Users/elchupa/code/school/antZoo/localenv/bin/python /Users/elchupa/code/school/antZoo/example_code/word_grep.py" )

ant.push( j )

with open( "/Users/elchupa/code/school/senior-research/grepTask/input/locations.list" ) as f:
  for l in f:
    ant.new_task( l )

print "Done sending tasks."
ant.finish()
ant._runner.join()

