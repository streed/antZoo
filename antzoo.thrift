namespace py gossiping

/*
  We need to be able to explicitly state what state each
  node is in. There are only three states for the gossip
  commununication.
*/
enum GossipStatus {
  IDLE = 0,
  RECRUITING = 1,
  WORKING = 2
}

/*
  We need to be able to pass around the gossiping node
  information in a manner that encapsulates all of the
  information needed to judge what the node is doing
  and how to connect to it properly.
*/
struct GossipNode {
  1:string address,
  2:i32 port,
  3:GossipStatus status,
  4:string id
}

typedef list<GossipNode> GossipNodeList

struct GossipNodeView {
  1:map<string,list<string>> neighborhood,
  2:list<string> view,
  3:string owner
}

/*
  A message consists of the node that sent it
  as well as the uuid of the message itself.

  The uuid is used to track messages that are being
  sent through the network. The uuid's should be
  unique throughout the current network. Allowing
  for each node to keep track of past messages.
*/
struct GossipNodeMessage {
  1:GossipNode node,
  2:string uuid,
  3:i32 ttl
}

enum JobStatus {
  OK = 0,
  ERROR = 1
}

/*
  This describes a job. It encapsulates the
  location of both code and data as well as
  the start and end offsets into the data
  itself.
*/
struct Job {
  1:string codeLocation,
  2:string dataLocation,
  3:i64 startOffset,
  4:i64 endOffset,
  5:string uuid
}

/*
  This is sent to the leader so that it
  can record the data itself. The status
  can be used to indicate an error state 
  and the currentIndex is the index that 
  update is for.
*/
struct JobUpdate {
  1:JobStatus status,
  2:string data,
  3:i64 currentIndex
}

/*
  A gossip job is one that is associated with some
  leader and some timestamp. The leader is the one
  that receives the results from the GossipJob. The
  job describes where the information is, and the 
  timestamp shows when the job started.
*/
struct GossipJob { 
  1:GossipNode leader,
  2:string uuid,
  3:Job job,
  4:i64 timestamp,
  5:i32 hop,
  6:double priority
}

struct GossipJobUpdate {
  1:GossipNode node,
  2:JobUpdate update
}

struct GossipData {
  1:string uuid,
  2:string key,
  3:string value
}

typedef list<GossipData> GossipDataList

struct Ant {
  1:GossipNode node,
  2:i32 ant_port
}

/*
  The Gossiping service provides the interface that
  nodes will use to communicate with each other.
*/
service Gossiping {

  /*
    The list of gossiping nodes that is sent to
    this service will be used to make a new internal
    view of the network. The return value from
    this call is this nodes original, prior to
    mixing, view.
  */
  GossipNodeView view( 1:GossipNodeView view ),

  /*
    This is called when another node adds this node to its
    view.
  */
  void added_to_view( 1:GossipNode node ),

  /*
    Retreives the current view from this node.
  */
  GossipNodeView get_view(),

  /*
    This simply is used to send a recruit message
    to the network. The node is the node that is 
    currently recruiting.
  */
  oneway void recruit( 1:Job job, 2:Ant ant ),

  /*
    This sends a job to another node, as soon
    as the job is received the node will begin
    to work on the job.
  */
  oneway void send_job( 1:GossipJob job ),

  /*
    This is called on the job leader each time
    a portion of the current job is finished.
    This allows the leader to keep an up-to-date
    record of what has been completed.
  */
  oneway void send_job_update( 1:GossipJobUpdate update ),

  /*
    Dissemenate some information throughout the 
    cluster.
  */
  void disseminate( 1:GossipData data ),

  /*
    Retreive a list of GossipData stored on each node.
  */
  GossipDataList getData(),
}

/*
  This is run to run and process jobs that come into
  the cluster.
*/
service AntZoo {
  /*
    Called when a new job comes in, this will setup the following:
      - Work group
      - Leader path
  */
  void new_job( 1:GossipJob job, 2:Ant ant ),
 
  /*
    This is called to send the specific portion of the current
    job to this node. I.E this task contains the range of the
    overall job to run.
  */
  GossipJobUpdate process_task( 1:GossipJob task ),


  /*
    Given some job return the current status of that running
    job.
  */
  GossipJobUpdate get_update( 1:GossipJob task ),

}
