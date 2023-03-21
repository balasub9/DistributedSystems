package chandy_lamport

import (
	"log"
	"strconv"
)

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src

	localState *SyncMap

	inboundLinkSnaps      *SyncMap
	inboundLinksRecording *SyncMap
	activeSnaps           map[int]int // key = snapId

	// TODO: ADD MORE FIELDS HERE
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator, mapSyn *SyncMap) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		mapSyn,
		NewSyncMap(),
		NewSyncMap(),
		make(map[int]int),
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME
	inboundLink := server.inboundLinks[src]

	switch event := message.(type) {
	case MarkerMessage:

		//fmt.Println("MARKER MESSAGE RECEIVED at Server ", server.Id)

		snapShotId := event.snapshotId

		keyforSnapshot := strconv.Itoa(snapShotId) + "_" + inboundLink.src + "_" + inboundLink.dest

		//fmt.Println("Key for recording is  ", keyforSnapshot)

		// Marking we received marker along this inbound link
		server.inboundLinkSnaps.Store(keyforSnapshot, 1)

		if !server.isLocalStateOfServerRecorded(snapShotId) {
			//fmt.Println("Recoding local state  server ", server)
			server.StartSnapshot(snapShotId)

			//Initalize list of received message as empty
			// ie Marking inbound link state with zero
			server.inboundLinksRecording.Store(keyforSnapshot, "")
			// Mark as active snap
			server.activeSnaps[snapShotId] = 1
		}

		//Check if marker is received from all neighbours
		flag := true
		for k, _ := range server.inboundLinks {
			keyforSnapshot1 := strconv.Itoa(snapShotId) + "_" + k + "_" + server.Id
			//fmt.Println("Key retrived for ", keyforSnapshot1)
			val, ok := server.inboundLinkSnaps.Load(keyforSnapshot1)
			if !ok || val.(int) != 1 {
				flag = false
				break
			}
		}
		if flag {
			//fmt.Println("SNAPSHOT COMPLETE FOR SERVER  ", server.Id)
			// Marking the snap with snapShotId as complete
			server.activeSnaps[snapShotId] = 0
			server.sim.NotifySnapshotComplete(server.Id, snapShotId)
		}
		//fmt.Printf(MarkerMessage.snapshotId)

	case TokenMessage:

		tokenReceived := event.numTokens
		//fmt.Println("Token MESSAGE RECEIVED at ", server.Id)
		server.Tokens += tokenReceived

		for snapId, isSnapshotActive := range server.activeSnaps {
			//fmt.Println("Active Snap found ")

			keyforSnapshot1 := strconv.Itoa(snapId) + "_" + inboundLink.src + "_" + inboundLink.dest
			_, isMarkerReceived := server.inboundLinkSnaps.Load(keyforSnapshot1)
			//fmt.Println("isMarkerReceived ", isMarkerReceived)
			if isSnapshotActive == 1 && !isMarkerReceived {

				v, ok := server.inboundLinksRecording.Load(keyforSnapshot1)
				if ok {
					//fmt.Println("Reocring State of link ")

					temp := v.(string) + "_" + strconv.Itoa(tokenReceived)
					server.inboundLinksRecording.Store(keyforSnapshot1, temp)
				} else {
					//fmt.Println("Reocring State of link ")

					server.inboundLinksRecording.Store(keyforSnapshot1, strconv.Itoa(tokenReceived))
				}

			}
		}
		//fmt.Println("Token MESSAGE END at ", server.Id)

	default:
		log.Fatal("Error unknown event: ", event)
	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME
	//fmt.Println("Snapshot started at actual server for ", server.Id)

	//Server Records it own Local State
	server.localState.Store(snapshotId, server.Tokens)

	server.activeSnaps[snapshotId] = 1

	// Send Marker message to out going channels
	server.SendToNeighbors(MarkerMessage{snapshotId})

}

func (server *Server) isLocalStateOfServerRecorded(snapId int) bool {
	_, ok := server.localState.Load(snapId)
	return ok
}
