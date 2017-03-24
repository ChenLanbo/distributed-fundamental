package raftkv

import (
	"encoding/gob"
	"log"
	"sync"
	"sync/atomic"
    "time"

	"labrpc"
	"raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//
// Index storage of RaftKV
//
type KVIndexCell struct {
    Timestamp int64
    RequestId int64
    Value string
    Op string
}

type KVIndexRow []KVIndexCell

func (r KVIndexRow) Len() int {
    return len(r)
}

func (r KVIndexRow) Swap(i, j) {
    r[i], r[j] = r[j], r[i]
}

func (r KVIndexRow) Less(i, j int) bool {
    if r[i].Timestamp != r[j].Timestamp {
        return r[i].Timestamp < r[j].Timestamp
    }

    return r[i].RequestId < r[j].RequestId
}

type KVIndex struct {
	mu      sync.Mutex
    mp map[string][]KVIndexCell
}

func (index *KVIndex) Get(request *GetArgs) string {
    index.mu.Lock()
    defer index.mu.Unlock()

    value := ""
    row, prs := index.mp[request.Key]
    if !prs {
        return ""
    }

    for _, cell := range(row) {
        if cell.Op == "Put" {
            value = cell.Value
        } else if cell.Op == "Append" {
            value += cell.Value
        }
    }

    return value
}

func (index *KVIndex) PutAppend(request *PutAppendArgs) {
    index.mu.Lock()
    defer index.mu.Unlock()

    _, prs := index.mp[request.Key]
    if !prs {
        cell := KVIndexCell{
            Timestamp:request.Timestamp,
            RequestId:request.RequestId,
            Value:request.Value,
            Op:request.Op}

        index.mp[request.Key] = make([]KVIndexCell, 0)
        index.mp[request.Key] = append(index.mp[request.Key], cell)
        return
    }

    for _, cell := range(index.mp[request.Key]) {
        if cell.Timestamp == request.Timestamp && cell.RequestId == request.RequestId {
            return
        }
    }
    cell := KVIndexCell{
        Timestamp:request.Timestamp,
        RequestId:request.RequestId,
        Value:request.Value,
        Op:request.Op}
    index.mp[request.Key] = append(index.mp[request.Key], cell)
    sort.Sort(KVIndexRow(index.mp[request.Key])
}

type OpType int
const (
    GETOP OpType = iota + 1
    PUTAPPENDOP
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Type OpType
    GetRequest *GetArgs
    PutAppendRequest *PutAppendArgs
}

type OpCallback struct {
    op *Op
    lookupChan chan GetReply
    mutateChan chan PutAppendReply
}

func MakeOpCallback(op *Op) *OpCallback {
    cb := &OpCallback{}
    cb.op = op
    cb.lookupChan = make(chan GetReply, 1)
    cb.mutateChan = make(chan PutAppendReply, 1)
    return cb
}

//
// RaftKV server
//
type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    stop int
    index KVIndex
    callbacks map[int64]*OpCallback
}

func (kv *RaftKV) AddCallback(seq int, cb *OpCallback) {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    kv.callbacks[seq] = cb
}

func (kv *RaftKV) InvokeCallback(seq int, op *Op) {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    _, prs := kv.callbacks[seq]
    if !prs {
        return
    }

    if kv.callbacks[seq].op.Type == GETOP {
        if op.Type == GETOP {
        } else if op.Type == PUTAPPENDOP {
            reply := GetReply{WrongLeader:false, Err:""}
            kv.callbacks[seq].lookupChan <-
        }
    } else if kv.callbacks[seq].op.Type == PUTAPPENDOP {
        if op.Type == GETOP {
        } else if op.Type == PUTAPPENDOP {
        }
    }
}


func (kv *RaftKV) ApplyLogs() {
    go func() {
        for !kv.Killed() {
            select {
            case msg := <- kv.applyCh:
                //
                op := msg.Command.(*Op)
                if op.Type == GETOP {
                    value := kv.index.Get(op.GetRequest)
                    reply := GetReply{WrongLeader:false, Err:OK, Value:value}
                } else if op.Type == PUTAPPENDOP {
                    kv.index.PutAppend(op.PutAppendRequest)
                    reply := PutAppendReply{WrongLeader:false, Err:OK}
                }
            case <- time.After(time.Second):
                // Noop
            }
        }
    } ()
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    op := &Op{}
    op.Type = GETOP
    op.GetRequest = args
    op.PutAppendRequest = nil

    seq, term, isLeader := kv.rf.Start(op)
    if !isLeader {
        reply.WrongLeader = true
        return
    }

    cb := MakeOpCallback(op)
    defer close(cb.lookupChan)
    defer close(cb.mutateChan)
    kv.AddCallback(seq, cb)

    r := <- cb.lookupChan
    *reply = r
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    op := &Op{}
    op.Type = PUTAPPENDOP
    op.GetRequest = nil
    op.PutAppendRequest = args

    index, term, isLeader := kv.rf.Start(op)
    if !isLeader {
        reply.WrongLeader = true
        return
    }

    cb := MakeOpCallback(op)
    defer close(cb.lookupChan)
    defer close(cb.mutateChan)
    kv.AddCallback(seq, cb)

    r := <- cb.mutateChan
    *reply = r
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
    atomic.StoreInt32(&kv.stop, 1)
}

func (kv *RaftKV) Killed() bool {
    return atomic.LoadInt32(&kv.stop) == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
