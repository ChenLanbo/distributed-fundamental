package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
    "bytes"
    "encoding/gob"
    "log"
    "math/rand"
    "sort"
    "sync"
    "sync/atomic"
    "time"

    "labrpc"
)

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
    stop int32
    state RaftState
    store *Store
    queue chan *RaftOperation

    role RaftComponent
    roleMu sync.Mutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.store.GetState()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
    rf.store.mu.Lock()
    defer rf.store.mu.Unlock()
    log.Println(rf.me, "save states at term", rf.store.currentTerm)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)

    e.Encode(rf.store.currentTerm)
    e.Encode(rf.store.votedFor)
    e.Encode(rf.store.commitIndex)
    e.Encode(rf.store.applyIndex)

    e.Encode(len(rf.store.logs) - 1)
    for i, log := range(rf.store.logs) {
        if i == 0 {
            continue
        }
        e.Encode(log.Term)
        e.Encode(log.Index)
        e.Encode(log.Command)
    }
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
    rf.store.mu.Lock()
    defer rf.store.mu.Unlock()
    log.Println(rf.me, "recover data", len(data))
    var numLogs int
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
    d.Decode(&rf.store.currentTerm)
    d.Decode(&rf.store.votedFor)
    d.Decode(&rf.store.commitIndex)
    d.Decode(&rf.store.applyIndex)

    d.Decode(&numLogs)
    for i := 0; i < numLogs; i++ {
        var l Log
        d.Decode(&l.Term)
        d.Decode(&l.Index)
        d.Decode(&l.Command)
        rf.store.logs = append(rf.store.logs, l)
    }

    log.Println(rf.me, "my starting term", rf.store.currentTerm)
}

func (rf *Raft) setState(state RaftState) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    rf.state = state
}

func (rf *Raft) getState() string {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.state == LEADER {
        return "LEADER"
    } else if rf.state == CANDIDATE {
        return "CANDIDATE"
    } else if rf.state == FOLLOWER {
        return "FOLLOWER"
    }
    return "BAD"
}

func (rf *Raft) run() {
    go func() {
        for !rf.Killed() {
            switch rf.state {
            case LEADER:
                log.Println(rf.me, ": run as a leader at term", rf.store.GetTerm())
                rf.role = MakeLeader(rf)
            case CANDIDATE:
                log.Println(rf.me, ": run as a candidate.")
                rf.role = MakeCandidate(rf)
            case FOLLOWER:
                log.Println(rf.me, ": run as a follower.")
                rf.role = MakeFollower(rf)
            }
            rf.role.Run()
        }
    } ()
}

//
// State of a peer: leader, candidate or follower.
//
type RaftState int
const (
    LEADER RaftState = iota + 1
    CANDIDATE
    FOLLOWER
)

//
// Struct that wraps a higher term and the peer sends it.
//
type HigherTerm struct {PeerId, Term int}

//
// Generic interface of a peer roll
//
type RaftComponent interface {
    Run()
    Stop()
    Stopped() bool
}

//
// Leader component
//
type RaftLeader struct {
    mu sync.Mutex
    rf *Raft
    newTermChan chan HigherTerm
    replicators []*LogReplicator
    committer *LogCommitter
    stop int32
}

func MakeLeader(rf *Raft) *RaftLeader {
    leader := &RaftLeader{}
    leader.rf = rf
    leader.newTermChan = make(chan HigherTerm, 1)
    leader.replicators = make([]*LogReplicator, len(leader.rf.peers))
    for i := 0; i < len(leader.rf.peers); i++ {
        leader.replicators[i] = MakeLogReplicator(leader, i)
    }
    leader.committer = MakeLogCommitter(leader)
    leader.stop = 0
    return leader
}

func (leader *RaftLeader) Run() {
    leader.rf.roleMu.Lock()
    defer leader.rf.roleMu.Unlock()

    if leader.rf.state != LEADER {
        // Log error
        log.Println(leader.rf.me, ": not in leader state.")
        return
    }

    // Process requests
    go func() {
        leader.ProcessRequests()
    } ()

    for i, _ := range(leader.replicators) {
        go func(id int) {
            leader.replicators[id].ReplicateLogs()
        } (i)
    }

    go func() {
        leader.committer.CommitLogs()
    } ()

    for {
        higherTerm := <- leader.newTermChan
        if leader.rf.store.GetTerm() < higherTerm.Term {
            leader.Stop()
            leader.rf.store.SetTerm(higherTerm.Term)
            leader.rf.store.SetVotedFor(-1)
            leader.rf.setState(FOLLOWER)
            return
        } else {
            // ignore
        }
    }
}

func (leader *RaftLeader) Stop() {
    leader.mu.Lock()
    defer leader.mu.Unlock()
    leader.stop = 1
}

func (leader *RaftLeader) Stopped() bool {
    leader.mu.Lock()
    defer leader.mu.Unlock()
    return leader.stop == 1
}

func (leader *RaftLeader) ProcessRequests() {
    log.Println(leader.rf.me, ": start processing requests")
    for !leader.Stopped() {
        op := <- leader.rf.queue

        if op == nil {
            continue
        }

        if op.VoteRequest != nil {
            log.Println(leader.rf.me, "get vote request from", op.VoteRequest.CandidateId)
            reply := RequestVoteReply{VoteGranted:false, Term:leader.rf.store.GetTerm()}
            op.VoteCallback <- reply

            if leader.rf.store.GetTerm() < op.VoteRequest.Term {
                higherTerm := HigherTerm{PeerId:op.VoteRequest.CandidateId, Term:op.VoteRequest.Term}
                leader.newTermChan <- higherTerm
                return
            }
        } else if op.AppendRequest != nil {
            log.Println(leader.rf.me, "get append request from", op.AppendRequest.LeaderId)
            reply := AppendEntriesReply{Term:leader.rf.store.GetTerm(), Success:false}
            op.AppendCallback <- reply

            if leader.rf.store.GetTerm() < op.AppendRequest.Term {
                higherTerm := HigherTerm{PeerId:op.AppendRequest.LeaderId, Term:op.AppendRequest.Term}
                leader.newTermChan <- higherTerm
            }
        }
    }
}

//
// Log replicator component
//
type LogReplicator struct {
    mu sync.Mutex
    leader *RaftLeader
    peer int
    replIndex int
    prevLog Log
}

func MakeLogReplicator(leader *RaftLeader, peer int) *LogReplicator {
    repl := &LogReplicator{}
    repl.leader = leader
    repl.peer = peer
    repl.replIndex = leader.rf.store.GetCommitIndex()
    _, repl.prevLog = leader.rf.store.Read(repl.replIndex)
    return repl
}

func (repl *LogReplicator) GetReplIndex() int {
    repl.mu.Lock()
    defer repl.mu.Unlock()
    return repl.replIndex
}

func (repl *LogReplicator) IncrementReplIndex(batch int) {
    repl.mu.Lock()
    defer repl.mu.Unlock()
    repl.replIndex += batch
}

func (repl *LogReplicator) DecrementReplIndex() {
    repl.mu.Lock()
    defer repl.mu.Unlock()
    if repl.replIndex > 0 {
        repl.replIndex--
    }
}

func (repl *LogReplicator) ReplicateLogs() {
    if repl.peer == repl.leader.rf.me {
        return
    }

    log.Println(repl.leader.rf.me, "replicating logs to", repl.peer)
    for !repl.leader.Stopped() {

        request := &AppendEntriesArgs{}
        request.LeaderId = repl.leader.rf.me
        request.Term = repl.leader.rf.store.GetTerm()
        request.PrevLogTerm = repl.prevLog.Term
        request.PrevLogIndex = repl.prevLog.Index
        request.CommitIndex = repl.leader.rf.store.GetCommitIndex()
        request.Entries = make([]Log, 0)

        for batch := 0; batch < 16; batch++ {
            hasNewLog, nextLog := repl.leader.rf.store.Read(repl.GetReplIndex() + batch + 1)
            if !hasNewLog {
                break
            }
            request.Entries = append(request.Entries, nextLog)
        }

        reply := &AppendEntriesReply{}
        for {
            // log.Println(repl.peer, "peer commit index", request.CommitIndex)
            if repl.leader.rf.sendAppendEntries(repl.peer, request, reply) {
                break
            } else {
                // log.Println(repl.leader.rf.me, "error sending AppendEntries to", repl.peer)
            }

            if repl.leader.Stopped() {
                return
            }
        }

        if reply.Success {
            if len(request.Entries) > 0 {
                repl.IncrementReplIndex(len(request.Entries))
                repl.prevLog = request.Entries[len(request.Entries) - 1]
            } else {
                // no new log, wait for some time
                time.Sleep(time.Millisecond * time.Duration(32 + rand.Int31n(32)))
            }
        } else {
            if request.Term < reply.Term {
                // send to new term chan
                repl.leader.newTermChan <- HigherTerm{PeerId:repl.peer, Term:reply.Term}
                return
            } else {
                // previous log doesn't match, replicate backward
                repl.DecrementReplIndex()
                _, repl.prevLog = repl.leader.rf.store.Read(repl.GetReplIndex())
            }
        }
    }
}

//
// Log committer component
//
type LogCommitter struct {
    leader *RaftLeader
}

func MakeLogCommitter(leader *RaftLeader) *LogCommitter {
    committer := &LogCommitter{}
    committer.leader = leader
    return committer
}

func (committer *LogCommitter) CommitLogs() {
    for !committer.leader.Stopped() {
        replProgress := make([]int, len(committer.leader.replicators))
        for i, replicator := range(committer.leader.replicators) {
            if i == committer.leader.rf.me {
                _, replProgress[i] = committer.leader.rf.store.GetLatestLogTermAndIndex()
            } else {
                replProgress[i] = replicator.GetReplIndex()
            }
        }

        sort.Ints(replProgress)
        mid := len(committer.leader.replicators) / 2
        if committer.leader.rf.store.GetCommitIndex() < replProgress[mid] {
            log.Println(committer.leader.rf.me, ": advance commit index to", replProgress[mid])
            committer.leader.rf.store.SetCommitIndex(replProgress[mid])
        } else {
            time.Sleep(time.Millisecond * time.Duration(32))
        }
    }
}

//
// Candidate component
//
type RaftCandidate struct {
    rf *Raft
    mu sync.Mutex
    voteChan chan bool
    newTermChan chan HigherTerm
    voteMu sync.Mutex
    processMu sync.Mutex
    stop int32
    pause int32
}

func MakeCandidate(rf *Raft) *RaftCandidate {
    candidate := &RaftCandidate{}
    candidate.rf = rf
    candidate.voteChan = make(chan bool, 1)
    candidate.newTermChan = make(chan HigherTerm, 1)
    candidate.stop = 0
    candidate.pause = 0
    return candidate
}

func (candidate *RaftCandidate) Run() {
    candidate.rf.roleMu.Lock()
    defer candidate.rf.roleMu.Unlock()

    if candidate.rf.state != CANDIDATE {
        // Log error
        log.Println(candidate.rf.me, ": not in candidate state.")
        return
    }

    candidate.rf.store.SetVotedFor(-1)

    // Process requests
    go func() {
        candidate.ProcessRequests()
    } ()

    // Vote myself as the new leader
    go func() {
        for !candidate.Stopped() {
            newTerm := candidate.rf.store.IncrementTerm()
            latestLogTerm, latestLogIndex := candidate.rf.store.GetLatestLogTermAndIndex()

            log.Println(candidate.rf.me, ": start vote at term", newTerm)
            if !candidate.VoteAtTerm(newTerm, latestLogTerm, latestLogIndex) {
                st := rand.Int31n(512) + 256
                // log.Println(candidate.rf.me, "round", newTerm, "failed. sleep", st)
                time.Sleep(time.Millisecond * time.Duration(st))
            } else {
                break
            }
        }
    } ()

    for {
        select {
        case higherTerm := <- candidate.newTermChan:
            if candidate.rf.store.GetTerm() <= higherTerm.Term {
                candidate.Stop()
                candidate.rf.setState(FOLLOWER)
                candidate.rf.store.SetTerm(higherTerm.Term)
                candidate.rf.store.SetVotedFor(higherTerm.PeerId)
                return
            }
        case isNewLeader := <- candidate.voteChan:
            if isNewLeader {
                log.Println(candidate.rf.me, "win vote")
                candidate.Stop()
                candidate.rf.setState(LEADER)
                candidate.rf.store.SetVotedFor(candidate.rf.me)
                return
            }
        }
    }
}

func (candidate *RaftCandidate) Stop() {
    candidate.mu.Lock()
    defer candidate.mu.Unlock()
    candidate.stop = 1
}

func (candidate *RaftCandidate) Stopped() bool {
    candidate.mu.Lock()
    defer candidate.mu.Unlock()
    return candidate.stop == 1
}

func (candidate *RaftCandidate) VoteAtTerm(newTerm, latestLogTerm, latestLogIndex int) bool {
    candidate.voteMu.Lock()
    defer candidate.voteMu.Unlock()

    request := &RequestVoteArgs{}
    request.CandidateId = candidate.rf.me
    request.Term = newTerm
    request.LatestLogTerm = latestLogTerm
    request.LatestLogIndex = latestLogIndex

    numSuccess := 1
    replyChan := make(chan bool, len(candidate.rf.peers))
    defer close(replyChan)

    // Send votes to other peers
    shuffle := rand.Perm(len(candidate.rf.peers))
    for _, server := range shuffle {
        if server == candidate.rf.me {
            continue
        }
        if candidate.Stopped() {
            log.Println(candidate.rf.me, ": candidate stopped.")
            return true
        }

        go func(server int) {
            defer func() {
                if r := recover(); r != nil {
                    // ignore error
                }
            } ()

            // log.Println(candidate.rf.me, ": sending vote request to", server)
            reply := &RequestVoteReply{}
            success := false

            if candidate.rf.sendRequestVote(server, request, reply) {
                if reply.VoteGranted {
                    success = true
                } else if newTerm < reply.Term {
                    higherTerm := HigherTerm {PeerId:-1, Term:reply.Term}
                    candidate.newTermChan <- higherTerm
                }
            }
            replyChan <- success
        } (server)
    }

    for i := 0; i < len(candidate.rf.peers) - 1; i++ {
        success := <- replyChan
        if success {
            numSuccess++
        }

        if numSuccess > len(shuffle) / 2 {
            candidate.voteChan <- true
            return true
        }
    }
    if numSuccess > len(shuffle) / 2 {
        candidate.voteChan <- true
        return true
    } else {
        return false
    }
}

func (candidate *RaftCandidate) ProcessRequests() {
    candidate.processMu.Lock()
    defer candidate.processMu.Unlock()

    if candidate.Stopped() {
        return
    }

    for !candidate.Stopped() {
        var op *RaftOperation = nil
        select {
        case op = <- candidate.rf.queue:
            // Noop
        case <- time.After(time.Millisecond * time.Duration(100)):
            // Noop
        }

        if op == nil {
            continue
        }

        myTerm := candidate.rf.store.GetTerm()

        if op.VoteRequest != nil {
            reply := RequestVoteReply{VoteGranted:false, Term:myTerm}
            op.VoteCallback <- reply

            if candidate.rf.store.GetTerm() < op.VoteRequest.Term {
                higherTerm := HigherTerm {PeerId:-1, Term:op.VoteRequest.Term}
                candidate.newTermChan <- higherTerm
                return
            }
        } else if op.AppendRequest != nil {
            reply := AppendEntriesReply{Term:myTerm, Success:false}
            op.AppendCallback <- reply

            // New AppendEntries RPC from another server claiming to be
            // leader. If the leader’s term (included in its RPC) is at least
            // as large as the candidate’s current term, then the candidate
            // recognizes the leader as legitimate and returns to follower state.
            if candidate.rf.store.GetTerm() <= op.AppendRequest.Term {
                higherTerm := HigherTerm {PeerId:op.AppendRequest.LeaderId, Term:op.AppendRequest.Term}
                candidate.newTermChan <- higherTerm
                return
            }
        }
    }
}

//
// Follower component
//
type RaftFollower struct {
    rf *Raft
    mu sync.Mutex
    stop int32
}

func MakeFollower(rf *Raft) *RaftFollower {
    follower := &RaftFollower{}
    follower.rf = rf
    follower.stop = 0
    return follower
}

func (follower *RaftFollower) Run() {
    follower.rf.roleMu.Lock()
    defer follower.rf.roleMu.Unlock()

    if follower.rf.state != FOLLOWER {
        // Log error
        log.Println(follower.rf.me, ": not in follower state.")
        return
    }

    lastSawAppend := time.Now().UnixNano()

    // Process requests
    for !follower.Stopped() {
        success := false
        select {
        case op := <- follower.rf.queue:
            if op != nil && op.VoteRequest != nil {
                myTerm := follower.rf.store.GetTerm()
                if op.VoteRequest.Term == myTerm {
                    if op.VoteRequest.CandidateId ==
                        follower.rf.store.GetVotedFor() &&
                       follower.rf.store.OtherPeerLogMoreUpToDate(
                        op.VoteRequest.CandidateId,
                        op.VoteRequest.LatestLogTerm,
                        op.VoteRequest.LatestLogIndex) {
                        success = true
                        lastSawAppend = time.Now().UnixNano()
                   }
                } else if op.VoteRequest.Term > myTerm {
                    if follower.rf.store.OtherPeerLogMoreUpToDate(
                        op.VoteRequest.CandidateId,
                        op.VoteRequest.LatestLogTerm,
                        op.VoteRequest.LatestLogIndex) {
                        success = true
                        follower.rf.store.SetVotedFor(
                            op.VoteRequest.CandidateId)
                        lastSawAppend = time.Now().UnixNano()
                    }
                    follower.rf.store.SetTerm(op.VoteRequest.Term)
                }

                log.Println(follower.rf.me, ": follower votes candidate",
                            op.VoteRequest.CandidateId, success)
                if success {
                    lastSawAppend = time.Now().UnixNano()
                }

                reply := RequestVoteReply{}
                reply.VoteGranted = success
                reply.Term = myTerm
                op.VoteCallback <- reply
            } else if op != nil && op.AppendRequest != nil {
                success := false
                myTerm := follower.rf.store.GetTerm()

                if myTerm <= op.AppendRequest.Term {
                    follower.rf.store.SetTerm(op.AppendRequest.Term)
                    follower.rf.store.SetVotedFor(op.AppendRequest.LeaderId)
                    lastSawAppend = time.Now().UnixNano()

                    if follower.rf.store.LogMatch(op.AppendRequest.PrevLogTerm,
                        op.AppendRequest.PrevLogIndex) {
                        success = true
                        if len(op.AppendRequest.Entries) != 0 {
                            log.Println(follower.rf.me, "adding log with index", op.AppendRequest.Entries[0].Index, "command", op.AppendRequest.Entries[0].Command)
                            follower.rf.store.Append(op.AppendRequest.Entries)
                        }
                        follower.rf.store.SetCommitIndex(op.AppendRequest.CommitIndex)
                    }
                }

                reply := AppendEntriesReply{}
                reply.Term = myTerm
                reply.Success = success
                op.AppendCallback <- reply
            }

            tt := time.Now().UnixNano() - lastSawAppend
            if tt > int64(time.Second) {
                follower.rf.setState(CANDIDATE)
                follower.Stop()
                log.Println(follower.rf.me, "leader timeout: ", tt, int64(time.Second), "switch to candidate")
                return
            }
        case <- time.After(time.Second):
            tt := time.Now().UnixNano() - lastSawAppend
            if tt > int64(time.Second) {
                follower.rf.setState(CANDIDATE)
                follower.Stop()
                log.Println(follower.rf.me, "leader timeout: ", tt, int64(time.Second), "switch to candidate")
                return
            }
        }
    }
}

func (follower *RaftFollower) Stop() {
    atomic.StoreInt32(&follower.stop, 1)
}

func (follower *RaftFollower) Stopped() bool {
    return atomic.LoadInt32(&follower.stop) == 1
}

//
// Log & Store component
//
type Log struct {
    Term int
    Index int
    Command interface{}
}

func HeartBeatLog() Log {
    return Log{Term:-1, Index:-1, Command:nil}
}

type Store struct {
    rf *Raft
    currentTerm int
    votedFor int
    commitIndex int
    applyIndex int
    logs []Log
    applyChan chan ApplyMsg
    mu sync.Mutex
    applyMu sync.Mutex
}

func MakeNewStore(rf *Raft, applyChan chan ApplyMsg) *Store {
    store := &Store{}
    store.rf = rf
    store.currentTerm = 0
    store.votedFor = -1
    store.commitIndex = 0
    store.applyIndex = 0
    store.logs = make([]Log, 1)
    store.logs[0] = Log{Term:0, Index:0, Command:nil}
    store.applyChan = applyChan
    return store
}

func (s *Store) GetTerm() int {
    s.mu.Lock()
    defer s.mu.Unlock()
    return s.currentTerm
}

func (s *Store) IncrementTerm() int {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.currentTerm++
    return s.currentTerm
}

func (s *Store) SetTerm(term int) int {
    s.mu.Lock()
    defer s.mu.Unlock()
    if s.currentTerm < term {
        s.currentTerm = term
    }
    return s.currentTerm
}

func (s *Store) GetVotedFor() int {
    s.mu.Lock()
    defer s.mu.Unlock()
    return s.votedFor
}

func (s *Store) SetVotedFor(newLeader int) int {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.votedFor = newLeader
    return s.votedFor
}

func (s *Store) GetCommitIndex() int {
    s.mu.Lock()
    defer s.mu.Unlock()
    return s.commitIndex
}

func (s *Store) SetCommitIndex(commitIndex int) {
    s.mu.Lock()
    defer s.mu.Unlock()
    if s.commitIndex < commitIndex {
        latest := len(s.logs) - 1
        if latest < commitIndex {
            s.commitIndex = latest
        } else {
            s.commitIndex = commitIndex
        }

        go func() {
            s.ApplyLogs(s.commitIndex)
        } ()
    }
}

func (s *Store) GetState() (int, bool) {
    s.mu.Lock()
    defer s.mu.Unlock()
    // log.Println(s.rf.me, "term:", s.currentTerm, "votedFor:", s.votedFor)
    return s.currentTerm, s.votedFor == s.rf.me
}

func (s *Store) GetLatestLogTermAndIndex() (int, int) {
    s.mu.Lock()
    defer s.mu.Unlock()
    latest := len(s.logs) - 1
    return s.logs[latest].Term, s.logs[latest].Index
}

func (s *Store) LogMatch(term, index int) bool {
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.logs[len(s.logs) - 1].Index < index {
        return false
    }

    if s.logs[index].Term != term {
        return false
    }

    return true
}

// Checks if another peer's log is at least up to date as my log:
//
// Returns:
//   true if peerLogTerm > latestLogTerm or
//        if peerLogTerm == latestLogTerm and peerLogIndex >= latestLogIndex
func (s *Store) OtherPeerLogMoreUpToDate(peerId, peerLogTerm, peerLogIndex int) bool {
    s.mu.Lock()
    defer s.mu.Unlock()

    latestLogTerm := s.logs[len(s.logs) - 1].Term
    latestLogIndex := s.logs[len(s.logs) - 1].Index

    if latestLogTerm != peerLogTerm {
        return peerLogTerm > latestLogTerm
    } else {
        return peerLogIndex >= latestLogIndex
    }
}

//
// Propose a new command
//
func (s *Store) Propose(command interface{}) (int, int, bool) {
    s.mu.Lock()
    defer s.mu.Unlock()

    isLeader := s.rf.me == s.votedFor
    nextLogTerm := s.currentTerm
    nextLogIndex := s.logs[len(s.logs) - 1].Index + 1

    if isLeader {
        newLog := Log{Term:nextLogTerm, Index:nextLogIndex, Command:command}
        s.logs = append(s.logs, newLog)
    }

    return nextLogIndex, nextLogTerm, isLeader
}

func (s *Store) Append(logs []Log) {
    s.mu.Lock()
    defer s.mu.Unlock()

    latest := len(s.logs) - 1
    if s.logs[latest].Index >= logs[0].Index {
        s.logs = s.logs[:logs[0].Index - 1]
    }

    for _, log := range(logs) {
        s.logs = append(s.logs, log)
    }

}

//
// Read the log record at index i
//
func (s *Store) Read(index int) (bool, Log) {
    s.mu.Lock()
    defer s.mu.Unlock()

    latestLogIndex := s.logs[len(s.logs) - 1].Index
    success := false
    result := Log{}

    if index <= latestLogIndex {
        success = true
        result = s.logs[index]
    }

    return success, result
}

//
// Apply committed logs
//
func (s *Store) ApplyLogs(applyTo int) {
    s.applyMu.Lock()
    defer s.applyMu.Unlock()
    // apply logs
    for s.applyIndex + 1 <= applyTo {
        applyMsg := ApplyMsg{}
        applyMsg.Index = s.logs[s.applyIndex + 1].Index
        applyMsg.Command = s.logs[s.applyIndex + 1].Command
        applyMsg.UseSnapshot = false
        applyMsg.Snapshot = make([]byte, 0)
        // log.Println(s.rf.me, "apply log with index", s.applyIndex + 1, "command", applyMsg.Command)
        s.applyChan <- applyMsg
        s.applyIndex++
    }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    CandidateId int
    Term int
    LatestLogTerm int
    LatestLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    VoteGranted bool
    Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    // log.Println(rf.me, ": receives vote request from", args.CandidateId)
    defer func() {
        if r := recover(); r != nil {
            // ignore error
        }
    } ()
    op := MakeRaftOperation(args, nil)
    defer op.Stop()

    rf.queue <- op

    result := <- op.VoteCallback
    reply.VoteGranted = result.VoteGranted
    reply.Term = result.Term
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    ch := make(chan bool, 1)
    defer close(ch)

    go func() {
        defer func() {
            if r := recover(); r != nil {
                // ignore error
            }
        } ()
	    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
        ch <- ok
    } ()

    select {
    case result := <- ch:
        return result
    case <- time.After(time.Millisecond * time.Duration(128)):
        return false
    }
}

//
// AppendEntries args
//
type AppendEntriesArgs struct {
    LeaderId int
    Term int
    PrevLogTerm int
    PrevLogIndex int
    CommitIndex int
    Entries []Log
}

//
// AppendEntries reply
//
type AppendEntriesReply struct {
    Term int
    Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    // log.Println(rf.me, ": receives append request from", args.LeaderId)
    defer func() {
        if r := recover(); r != nil {
            // ignore error
        }
    } ()
    op := MakeRaftOperation(nil, args)
    defer op.Stop()

    rf.queue <- op

    result := <- op.AppendCallback
    reply.Term = result.Term
    reply.Success = result.Success
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ch := make(chan bool, 1)
    defer close(ch)

    go func() {
        defer func() {
            if r := recover(); r != nil {
                // ignore error
            }
        } ()
	    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
        ch <- ok
    } ()

    select {
    case result := <- ch:
        return result
    case <- time.After(time.Millisecond * time.Duration(128)):
        return false
    }
}

//
// RaftOperation encapsulates the context of a RPC request from a peer
//
type RaftOperation struct {
    VoteRequest *RequestVoteArgs
    VoteCallback chan RequestVoteReply
    AppendRequest *AppendEntriesArgs
    AppendCallback chan AppendEntriesReply
}

func MakeRaftOperation(voteRequest *RequestVoteArgs, appendRequest *AppendEntriesArgs) *RaftOperation {
    op := &RaftOperation{}
    op.VoteRequest = voteRequest
    op.VoteCallback = make(chan RequestVoteReply, 1)
    op.AppendRequest = appendRequest
    op.AppendCallback = make(chan AppendEntriesReply, 1)
    return op
}

func (op *RaftOperation) Stop() {
    close(op.VoteCallback)
    close(op.AppendCallback)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
    index, term, isLeader = rf.store.Propose(command)
    if isLeader {
        log.Println(rf.me, "propose new command at index", index, "command", command)
    }
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
    atomic.StoreInt32(&rf.stop, 1)
    if rf.role != nil {
        log.Println(rf.me, ": stop.")
        rf.role.Stop()
    }
    time.Sleep(time.Millisecond * time.Duration(32))
    rf.persist()
    close(rf.queue)
}

func (rf *Raft) Killed() bool {
    return atomic.LoadInt32(&rf.stop) == 1
}

//
// The service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
    rf.stop = 0
    rf.state = FOLLOWER
    rf.store = MakeNewStore(rf, applyCh)
    rf.role = nil
    rf.queue = make(chan *RaftOperation, 1)

	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    // Start this peer
    rf.run()

	return rf
}
