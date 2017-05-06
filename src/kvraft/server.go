package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	// "log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		format = time.Now().String() + " " + format + "\n"
		fmt.Printf(format)
		//log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Type     string // "Put" or "Append" or "Get"
	ClientId string
	SeqNo    int64
}

type Condition struct {
	seqno int64
	cond  chan int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	kvs          map[string]string

	requestFilter map[string]int64
	requestChan   map[int]chan Op

	// Your definitions here.
}

func (kv *RaftKV) WaitCommit(term, index int, targetOp Op, indexChan chan Op) bool {
	timeout := time.NewTimer(100 * time.Millisecond)
	for {
		select {
		case op := <-indexChan:
			DPrintf(fmt.Sprintf("recv channel: me = %v index = %v op = %v", kv.me, index, op))
			if op == targetOp {
				return true
			} else {
				return false
			}
		case <-timeout.C:
			DPrintf(fmt.Sprintf("timout channel: me = %v index = %v", kv.me, index))
			currentTerm, isLeader := kv.rf.GetState()
			if isLeader && term == currentTerm {
				timeout.Reset(100 * time.Millisecond)
			} else {
				// delete indexChan will cause apply() block, it should delete by apply()
				// to prevent timeout casue apply block, we must prevent dangling channel
				// so, just receive it
				go func() { <-indexChan }()
				return false
			}
		}
	}
	return false
}

func (kv *RaftKV) Apply() {
	for {
		select {
		case message := <-kv.applyCh:
			op := message.Command.(Op)
			DPrintf(fmt.Sprintf("Apply: me = %v, index = %v, op = %v", kv.me, message.Index, op))
			kv.mu.Lock()
			// S1 is leader, S1 append C1 in log
			// S1 send C1 to S2 then partition
			// S2 be the leader, C2 comes
			// C1 retry to S2, because C1 not commited, it's not in filter
			// S2 append C1 to log, is this right?
			// we should filt it here
			lastSeqNo, ok := kv.requestFilter[op.ClientId]
			if !ok || lastSeqNo < op.SeqNo {
				switch op.Type {
				case "Append":
					kv.kvs[op.Key] += op.Value
				case "Put":
					kv.kvs[op.Key] = op.Value
				default:
				}
				kv.requestFilter[op.ClientId] = op.SeqNo
			}
			indexChan, ok := kv.requestChan[message.Index]
			if ok {
				DPrintf(fmt.Sprintf("send op to channel me = %v, index = %v", kv.me, message.Index))
				indexChan <- op
				delete(kv.requestChan, message.Index)
			}
			kv.mu.Unlock()
		}
	}
}

// should under lock
func (kv *RaftKV) SetRequestChan(index int) chan Op {
	c, ok := kv.requestChan[index]
	if !ok {
		c = make(chan Op)
		kv.requestChan[index] = c
		DPrintf(fmt.Sprintf("SetRequestChan me = %v, index = %v", kv.me, index))
	} else {
		// S1 is leader, recv C1, C2 in index n, n+1
		// S1 loss leadership, C1 apply,  C2 was discard
		// S1 become leader, so index n + 1 will be use agin
		DPrintf(fmt.Sprintf("SetRequestChan me = %v, index = %v exsit", kv.me, index))
		c <- Op{}
	}
	return c
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		Key:      args.Key,
		Type:     "Get",
		ClientId: args.ClientId,
		SeqNo:    args.SeqNo}
	index, term, isLeader := kv.rf.Start(op)
	DPrintf(fmt.Sprintf("Get: me = %v, isLeader = %v, term = %v, index = %v, key = %v, args = %v", kv.me, isLeader, term, index, args.Key, args))
	if isLeader {
		indexChan := kv.SetRequestChan(index)
		kv.mu.Unlock()
		isCommited := kv.WaitCommit(term, index, op, indexChan)
		kv.mu.Lock()
		if isCommited {
			reply.WrongLeader = false
			reply.Err = OK
			reply.Value = kv.kvs[args.Key]
		} else {
			reply.WrongLeader = true
		}
	} else {
		reply.WrongLeader = true
	}
	DPrintf(fmt.Sprintf("Get: me = %v, isLeader = %v, term = %v, index = %v, reply = %v", kv.me, isLeader, term, index, reply))
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		Type:     args.Op,
		ClientId: args.ClientId,
		SeqNo:    args.SeqNo}
	seqno, ok := kv.requestFilter[args.ClientId]
	if ok && seqno >= args.SeqNo { // request already exsit
		DPrintf(fmt.Sprintf("PutAppend: duplicate me = %v, args = %v", kv.me, args))
		reply.WrongLeader = false
		reply.Err = OK
		return
	}
	index, term, isLeader := kv.rf.Start(op)
	DPrintf(fmt.Sprintf("PutAppend: me = %v, isLeader = %v, term = %v, index = %v, key = %v, args = %v", kv.me, isLeader, term, index, args.Key, args))
	if isLeader {
		indexChan := kv.SetRequestChan(index)
		kv.mu.Unlock()
		isCommited := kv.WaitCommit(term, index, op, indexChan)
		kv.mu.Lock()
		if isCommited {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	} else {
		reply.WrongLeader = true
	}
	DPrintf(fmt.Sprintf("PutAppend: me = %v, isLeader = %v, term = %v, index = %v, reply = %v", kv.me, isLeader, term, index, reply))
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
	kv.kvs = make(map[string]string)
	kv.requestFilter = make(map[string]int64)
	kv.requestChan = make(map[int]chan Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	go kv.Apply()
	return kv
}
