package raft

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

var leader string = "Leader"
var follower string = "Follower"
var candidate string = "Candidate"

type Raft struct {
	mu              sync.Mutex
	peers           []*labrpc.ClientEnd
	persister       *Persister
	me              int
	currentTerm     int
	votedFor        int
	currState       string
	voteCount       int
	hearbeatChannel chan bool
	wonElection     chan bool
}

func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.currState == leader
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.currentTerm)
}

func randomDelay() time.Duration {
	return time.Duration(150+rand.Int63()%150) * time.Millisecond
}

type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.currentTerm < args.Term {
		changeStatetoFollower(args.Term, rf)
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) sendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rpcStatus := rf.peers[peer].Call("Raft.RequestVote", args, reply)
	if rpcStatus {
		if rf.currState == candidate {
			if rf.currentTerm < reply.Term {
				changeStatetoFollower(reply.Term, rf)
			} else if reply.VoteGranted {
				noofPeers := len(rf.peers)
				rf.voteCount = rf.voteCount + 1
				if rf.voteCount == (noofPeers/2)+1 {
					rf.wonElection <- true
				}
			}
		}
	}
}

func performCandidateActvities(rf *Raft) {
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				rf.sendRequestVote(i,
					&RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me},
					&RequestVoteReply{})
			}(i)
		}
	}
	select {
	case <-rf.wonElection:
		rf.currState = leader
		rf.votedFor = -1
		rf.persist()
	case <-rf.hearbeatChannel:
		changeStatetoFollower(rf.currentTerm, rf)
	case <-time.After(randomDelay()):
		rf.currState = candidate
		rf.currentTerm = rf.currentTerm + 1
		rf.voteCount = 1
		rf.votedFor = rf.me
		rf.persist()
	}
}

type AppendEntryRPC struct {
	Term     int
	LeaderId int
}

type AppendEntryRPCReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntryRPC, reply *AppendEntryRPCReply) {
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.hearbeatChannel <- true
	reply.Term = args.Term
	if args.Term > rf.currentTerm {
		changeStatetoFollower(args.Term, rf)
	}
	rf.persist()
}

func (rf *Raft) sendHeartBeatRPC(peer int, args *AppendEntryRPC, reply *AppendEntryRPCReply) {
	rpcStatus := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	if rpcStatus {
		if rf.currState != leader || args.Term != rf.currentTerm {
			return
		}
		if rf.currentTerm < reply.Term {
			changeStatetoFollower(reply.Term, rf)
			return
		}
	}
}

func performLeaderActvities(rf *Raft) {
	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				rf.sendHeartBeatRPC(peer,
					&AppendEntryRPC{Term: rf.currentTerm, LeaderId: rf.me}, &AppendEntryRPCReply{})
			}(peer)
		}
	}

	time.Sleep(60 * time.Millisecond)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

func (rf *Raft) Kill() {
}

func changeStatetoFollower(term int, rf *Raft) {
	rf.currState = follower
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
	rf.hearbeatChannel = make(chan bool, 250)
	rf.wonElection = make(chan bool, 250)
	rf.currState = follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	go startRaftServer(rf)
	return rf
}

func startRaftServer(rf *Raft) {
	for {
		switch rf.currState {
		case follower:
			select {
			case <-rf.hearbeatChannel:
			case <-time.After(randomDelay()):
				rf.currState = candidate
				rf.currentTerm = rf.currentTerm + 1
				rf.voteCount = 1
				rf.votedFor = rf.me
				rf.persist()
			}
		case candidate:
			performCandidateActvities(rf)

		case leader:
			performLeaderActvities(rf)
		}

	}
}
