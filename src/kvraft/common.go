package raftkv

const (
    OK       = "OK"
    ErrNoKey = "ErrNoKey"
    ErrLeaderSwitch = "ErrLeaderSwitch"
)

type Err string

// Put or Append
type PutAppendArgs struct {
    Key   string
    Value string
    Op    string // "Put" or "Append"
    // You'll have to add definitions here.
    // Field names must start with capital letters,
    // otherwise RPC will break.
    RequestId int64
    Timestamp int64
}

type PutAppendReply struct {
    WrongLeader bool
    Err         Err
}

type GetArgs struct {
    Key string
    // You'll have to add definitions here.
    RequestId int64
    Timestamp int64
}

type GetReply struct {
    WrongLeader bool
    Err         Err
    Value       string
}
