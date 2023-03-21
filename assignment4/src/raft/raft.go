package raft

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

type LogEntry struct {
	LogIndex int
	LogTerm  int
	Cmd      interface{}
}

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

const (
	rpcTimoutInt = 102 * time.Millisecond
)
const (
	LEAD StateType = iota
	FOLL
	CAND
)

type StateType uint

type Raft struct {
	mu                  sync.Mutex
	peers               []*labrpc.ClientEnd
	persister           *Persister
	me                  int
	currentTerm         int
	votedFor            int
	nextIndex           []int
	voteCount           int
	lastIndexComitted   int
	lastIndexApplied    int
	matchIndex          []int
	handleHeartBeat     chan bool
	wonElection         chan bool
	handleCommitMessage chan bool
	log                 []LogEntry
	state               StateType
}

func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == LEAD
}

func (raft *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rpcResp := raft.peers[server].Call("Raft.RequestVote", args, reply)
	raft.mu.Lock()
	defer raft.mu.Unlock()
	if rpcResp {
		if raft.state != CAND {
			return rpcResp
		}
		if raft.currentTerm < reply.Term {
			raft.changetoFollower(reply.Term)
			return rpcResp
		}
		if reply.VoteGranted {
			raft.voteCount++
			if raft.voteCount == len(raft.peers)/2+1 {
				raft.wonElection <- true
			}
		}
	}
	return rpcResp
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.log)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term {
		rf.changetoFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.LastLogTerm < rf.log[len(rf.log)-1].LogTerm ||
		args.LastLogTerm == rf.log[len(rf.log)-1].LogTerm && args.LastLogIndex < rf.log[len(rf.log)-1].LogIndex {
		reply.VoteGranted = false
	} else if rf.votedFor == -1 {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false

	}
}

func (rf *Raft) sendRequVotetoAllNeigh() {
	rf.mu.Lock()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].LogIndex,
		LastLogTerm:  rf.log[len(rf.log)-1].LogTerm,
	}
	rf.mu.Unlock()
	for j := range rf.peers {
		if j != rf.me {
			go func(j int) {
				rf.sendRequestVote(j, args, &RequestVoteReply{})
			}(j)
		}
	}
	select {
	case <-rf.wonElection:
		rf.mu.Lock()
		rf.state = LEAD
		rf.votedFor = -1
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for j := range rf.peers {
			rf.nextIndex[j] = (rf.log[len(rf.log)-1].LogIndex + 1)
			rf.matchIndex[j] = 0
		}
		rf.persist()
		rf.mu.Unlock()
	case <-rf.handleHeartBeat:
		rf.mu.Lock()
		rf.changetoFollower(rf.currentTerm)
		rf.mu.Unlock()
	case <-time.After(time.Duration(rand.Int63()%300+300) * time.Millisecond):
		rf.mu.Lock()
		rf.becomeCandidate()
		rf.mu.Unlock()
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (raft *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	reply.Success = false

	if args.Term < raft.currentTerm {
		reply.Term = raft.currentTerm
		return
	}
	raft.handleHeartBeat <- true
	reply.Term = args.Term
	if args.Term > raft.currentTerm {
		raft.changetoFollower(args.Term)
		// raft.state = FOLL
		// raft.currentTerm = args.Term
		// raft.votedFor = -1
		// raft.persist()
	}

	if args.PreLogIndex > raft.log[len(raft.log)-1].LogIndex {
		reply.NextIndex = (raft.log[len(raft.log)-1].LogIndex + 1)
		return
	}
	term := raft.log[args.PreLogIndex].LogTerm
	if args.PreLogTerm != term {
		for j := args.PreLogIndex - 1; j >= 0; j-- {
			if raft.log[j].LogTerm != term {
				reply.NextIndex = j + 1
				break
			}
		}
		return
	}
	raft.log = raft.log[:args.PreLogIndex+1]
	raft.log = append(raft.log, args.Entries...)
	raft.persist()
	reply.Success = true
	reply.NextIndex = (raft.log[len(raft.log)-1].LogIndex + 1)
	if args.LeaderCommit > raft.lastIndexComitted {
		mini := raft.log[len(raft.log)-1].LogIndex
		if args.LeaderCommit <= raft.log[len(raft.log)-1].LogIndex {
			mini = args.LeaderCommit
		}
		raft.lastIndexComitted = mini
		raft.handleCommitMessage <- true
	}

}

func (raft *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rpcResp := raft.peers[server].Call("Raft.AppendEntries", args, reply)
	raft.mu.Lock()
	defer raft.mu.Unlock()
	if rpcResp {
		if raft.state != LEAD || args.Term != raft.currentTerm {
			return rpcResp
		}
		if raft.currentTerm < reply.Term {
			raft.changetoFollower(reply.Term)
			return rpcResp
		}
		raft.nextIndex[server] = reply.NextIndex
		if reply.Success {
			raft.matchIndex[server] = raft.nextIndex[server] - 1
		}
	}
	return rpcResp
}

func (raft *Raft) sendRpcAppend() {
	raft.mu.Lock()
	defer raft.mu.Unlock()

	newCommitIndex := raft.lastIndexComitted
	for j := raft.lastIndexComitted + 1; j <= raft.log[len(raft.log)-1].LogIndex; j++ {
		count := 1
		for server := range raft.peers {
			if server != raft.me && raft.matchIndex[server] >= j && raft.log[j].LogTerm == raft.currentTerm {
				count++

				if count == len(raft.peers)/2+1 {
					newCommitIndex = j
					break
				}
			}
		}
	}

	if newCommitIndex != raft.lastIndexComitted {
		raft.lastIndexComitted = newCommitIndex
		raft.handleCommitMessage <- true
	}

	for j := range raft.peers {
		if j != raft.me {
			go func(j int) {
				vg := &AppendEntriesArgs{
					Term:         raft.currentTerm,
					LeaderId:     raft.me,
					PreLogIndex:  raft.nextIndex[j] - 1,
					PreLogTerm:   raft.log[raft.nextIndex[j]-1].LogTerm,
					LeaderCommit: raft.lastIndexComitted,
				}
				vg.Entries = make([]LogEntry, len(raft.log[raft.nextIndex[j]:]))
				copy(vg.Entries, raft.log[raft.nextIndex[j]:])

				reply := &AppendEntriesReply{}
				raft.sendAppendEntries(j, vg, reply)
			}(j)
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, term, isLeader
	}
	rf.log = append(rf.log, LogEntry{
		LogTerm:  rf.currentTerm,
		LogIndex: len(rf.log),
		Cmd:      command,
	})
	rf.persist()
	index := rf.log[len(rf.log)-1].LogIndex
	return index, term, isLeader
}

func (rf *Raft) Kill() {
}

func (rf *Raft) becomeCandidate() {
	rf.state = CAND
	rf.currentTerm = rf.currentTerm + 1
	rf.voteCount = 1
	rf.votedFor = rf.me
	rf.persist()
}

func (rf *Raft) changetoFollower(term int) {
	rf.state = FOLL
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.log = append(rf.log, LogEntry{LogTerm: 0})
	rf.handleHeartBeat = make(chan bool, 100)
	rf.wonElection = make(chan bool, 100)
	rf.handleCommitMessage = make(chan bool, 100)
	rf.state = FOLL
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.readPersist(persister.ReadRaftState())
	rf.persist()

	go func() {
		for {
			switch rf.state {
			case LEAD:
				rf.sendRpcAppend()
				time.Sleep(rpcTimoutInt)
			case FOLL:
				select {
				case <-rf.handleHeartBeat:
				case <-time.After(time.Duration(rand.Int63()%300+300) * time.Millisecond):
					rf.mu.Lock()
					rf.becomeCandidate()
					rf.mu.Unlock()
				}
			case CAND:
				rf.sendRequVotetoAllNeigh()
			}

		}
	}()

	go func() {
		for {
			select {
			case <-rf.handleCommitMessage:
				rf.mu.Lock()
				for j := rf.lastIndexApplied + 1; j <= rf.lastIndexComitted; j++ {
					applyCh <- ApplyMsg{Index: j, Command: rf.log[j].Cmd}
					rf.lastIndexApplied = j
				}
				rf.mu.Unlock()
			}
		}
	}()
	return rf
}
