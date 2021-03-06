package raftkv

import (
    crand "crypto/rand"
    "math/big"
    "math/rand"
    "sync"
    "time"

    "labrpc"
)


type Clerk struct {
    servers []*labrpc.ClientEnd
    // You will have to modify this struct.
    mu sync.Mutex
    leader int
}

func nrand() int64 {
    max := big.NewInt(int64(1) << 62)
    bigx, _ := crand.Int(crand.Reader, max)
    x := bigx.Int64()
    return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
    ck := new(Clerk)
    ck.servers = servers
    ck.leader = -1
    // You'll have to add code here.
    return ck
}

func (ck *Clerk) setLeader(leader int) {
    ck.mu.Lock()
    defer ck.mu.Unlock()
    ck.leader = leader
}

func (ck *Clerk) getLeader() int {
    ck.mu.Lock()
    defer ck.mu.Unlock()
    if ck.leader == -1 {
        return rand.Intn(len(ck.servers))
    }
    return ck.leader
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
    // You will have to modify this function.
    rid := nrand()
    valueChan := make(chan string, 1)
    defer close(valueChan)

    for {
        server := ck.getLeader()

        go func(server int) {
            defer func() {
                if r := recover(); r != nil {
                }
            } ()

            request := &GetArgs{
                Key:key, RequestId:rid, Timestamp:time.Now().UnixNano()}
            reply := &GetReply{}
            ok := ck.servers[server].Call("RaftKV.Get", request, reply)

            if ok {
                if !reply.WrongLeader {
                    if reply.Err == OK {
                        valueChan <- reply.Value
                    }
                }
            }
        } (server)

        select {
        case value := <- valueChan:
            ck.setLeader(server)
            return value
        case <- time.After(time.Second):
            // Timeout
        }
        ck.setLeader(-1)

        /* if ok {
            if !reply.WrongLeader {
                ck.setLeader(server)
                if reply.Err == OK {
                    return reply.Value
                } else {
                    // log error
                }
            } else {
                ck.setLeader(-1)
            }
        } */
    }

    return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
    rid := nrand()
    successChan := make(chan bool, 1)
    defer close(successChan)
    // You will have to modify this function.

    for {
        server := ck.getLeader()

        go func(server int) {
            defer func() {
                if r := recover(); r != nil {
                }
            } ()
            request := &PutAppendArgs{
                Key:key, Value:value, Op:op,
                RequestId:rid, Timestamp:time.Now().UnixNano()}
            reply := &PutAppendReply{}
            ok := ck.servers[server].Call("RaftKV.PutAppend", request, reply)

            if ok {
                if !reply.WrongLeader {
                    if reply.Err == OK {
                        successChan <- true
                    }
                }
            }
        } (server)

        select {
        case success := <- successChan:
            if success {
                ck.setLeader(server)
                return
            }
        case <- time.After(time.Second):
            // Timeout
        }
        ck.setLeader(-1)

        /* if ok {
            if !reply.WrongLeader {
                ck.setLeader(server)
                if reply.Err == OK {
                    return
                }
            } else {
                ck.setLeader(-1)
            }
        } */
    }
}

func (ck *Clerk) Put(key string, value string) {
    ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
    ck.PutAppend(key, value, "Append")
}
