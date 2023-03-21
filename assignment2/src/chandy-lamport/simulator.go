package chandy_lamport

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// Max random delay added to packet delivery
const maxDelay = 5

// Simulator is the entry point to the distributed snapshot application.
//
// It is a discrete time simulator, i.e. events that happen at time t + 1 come
// strictly after events that happen at time t. At each time step, the simulator
// examines messages queued up across all the links in the system and decides
// which ones to deliver to the destination.
//
// The simulator is responsible for starting the snapshot process, inducing servers
// to pass tokens to each other, and collecting the snapshot state after the process
// has terminated.
type Simulator struct {
	time           int
	nextSnapshotId int
	servers        map[string]*Server // key = server ID
	logger         *Logger
	// TODO: ADD MORE FIELDS HERE
}

func NewSimulator() *Simulator {
	return &Simulator{
		0,
		0,
		make(map[string]*Server),
		NewLogger(),
	}
}

// Return the receive time of a message after adding a random delay.
// Note: since we only deliver one message to a given server at each time step,
// the message may be received *after* the time step returned in this function.
func (sim *Simulator) GetReceiveTime() int {
	return sim.time + 1 + rand.Intn(5)
}

// Add a server to this simulator with the specified number of starting tokens
func (sim *Simulator) AddServer(id string, tokens int) {
	server := NewServer(id, tokens, sim, NewSyncMap())
	sim.servers[id] = server
}

// Add a unidirectional link between two servers
func (sim *Simulator) AddForwardLink(src string, dest string) {
	server1, ok1 := sim.servers[src]
	server2, ok2 := sim.servers[dest]
	if !ok1 {
		log.Fatalf("Server %v does not exist\n", src)
	}
	if !ok2 {
		log.Fatalf("Server %v does not exist\n", dest)
	}
	server1.AddOutboundLink(server2)
}

// Run an event in the system
func (sim *Simulator) InjectEvent(event interface{}) {
	switch event := event.(type) {
	case PassTokenEvent:
		src := sim.servers[event.src]
		src.SendTokens(event.tokens, event.dest)
	case SnapshotEvent:
		sim.StartSnapshot(event.serverId)
	default:
		log.Fatal("Error unknown event: ", event)
	}
}

// Advance the simulator time forward by one step, handling all send message events
// that expire at the new time step, if any.
func (sim *Simulator) Tick() {
	sim.time++
	sim.logger.NewEpoch()
	// Note: to ensure deterministic ordering of packet delivery across the servers,
	// we must also iterate through the servers and the links in a deterministic way
	for _, serverId := range getSortedKeys(sim.servers) {
		server := sim.servers[serverId]
		for _, dest := range getSortedKeys(server.outboundLinks) {
			link := server.outboundLinks[dest]
			// Deliver at most one packet per server at each time step to
			// establish total ordering of packet delivery to each server
			if !link.events.Empty() {
				e := link.events.Peek().(SendMessageEvent)
				if e.receiveTime <= sim.time {
					link.events.Pop()
					sim.logger.RecordEvent(
						sim.servers[e.dest],
						ReceivedMessageEvent{e.src, e.dest, e.message})
					sim.servers[e.dest].HandlePacket(e.src, e.message)
					break
				}
			}
		}
	}
}

// Start a new snapshot process at the specified server
func (sim *Simulator) StartSnapshot(serverId string) {
	snapshotId := sim.nextSnapshotId
	sim.nextSnapshotId++
	sim.logger.RecordEvent(sim.servers[serverId], StartSnapshot{serverId, snapshotId})
	// TODO: IMPLEMENT ME
	//fmt.Println("Snapshot started at server form simulator ")
	fmt.Println(serverId)
	//fmt.Sprintf("Running test '%v', '%v'", topFile, eventsFile)
	server := sim.servers[serverId]
	server.StartSnapshot(snapshotId)
}

// Callback for servers to notify the simulator that the snapshot process has
// completed on a particular server
func (sim *Simulator) NotifySnapshotComplete(serverId string, snapshotId int) {
	sim.logger.RecordEvent(sim.servers[serverId], EndSnapshot{serverId, snapshotId})
	// TODO: IMPLEMENT ME
	// fmt.Println("Snapshot completed for  server  and its state is", serverId+"_"+strconv.Itoa(snapshotId))
	// for k, v := range sim.servers[serverId].inboundLinksRecording.internalMap {
	// 	if v.(string) != "" {
	// 		fmt.Print("Key is ", k)
	// 		fmt.Print("Value is ", v)
	// 		fmt.Println()
	// 	}
	// }
	// v, _ := sim.servers[serverId].localState.Load(snapshotId)
	// fmt.Println("SERVER TOKEN VALUE IS ", v.(int))

}

// Collect and merge snapshot state from all the servers.
// This function blocks until the snapshot process has completed on all servers.
func (sim *Simulator) CollectSnapshot(snapshotId int) *SnapshotState {
	snap := SnapshotState{snapshotId, make(map[string]int), make([]*SnapshotMessage, 0)}
	time.Sleep(3 * time.Second)

	for serverId, _ := range sim.servers {
		val, ok := sim.servers[serverId].localState.Load(snapshotId)
		if ok {
			snap.tokens[serverId] = val.(int)
		}
		sim.getChannelMessages(serverId, snapshotId, &snap)
	}

	return &snap
}

func (sim *Simulator) getChannelMessages(sId string, snapshotId int, snap *SnapshotState) {
	for k, v := range sim.servers[sId].inboundLinksRecording.internalMap {
		completeKey := k.(string)
		valueString := v.(string)
		splitkey := strings.Split(completeKey, "_")
		if splitkey[0] == strconv.Itoa(snapshotId) && valueString != "" {
			splitvalue := strings.Split(valueString, "_")
			for _, t := range splitvalue {
				ac, _ := strconv.Atoi(t)
				snap.messages = append(snap.messages, &SnapshotMessage{splitkey[1], splitkey[2], TokenMessage{ac}})
			}

		}
	}

}
