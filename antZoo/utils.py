

def make_client( addressPort ):
  address, port = addressPort.split( ":" )
  port = int( port )
  transport = TSocket.TSocket( address, port )
  transport = TTransport.TBufferedTransport( transport )
  protocol = TBinaryProtocol.TBinaryProtocol( transport )
  client = Client( protocol )
  transport.open()

  return client

def destroy_client( c ):
    pass

