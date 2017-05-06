package raftkv

import (
	"crypto/rand"
	"fmt"
	"github.com/satori/go.uuid"
	"labrpc"
	"math/big"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderId int
	seqno    int64
	clientId string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func NextServerIndex(length, leaderId int) int {
	nextIndex := leaderId
	for nextIndex == -1 || nextIndex == leaderId {
		nextIndex = int(nrand() % int64(length))
	}
	return nextIndex
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = -1
	ck.seqno = -1
	ck.clientId = uuid.NewV4().String()
	return ck
}

func (ck *Clerk) GetCurrentSeqno() int64 {
	ck.seqno++
	return ck.seqno
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
	leaderId := ck.leaderId
	serverLen := len(ck.servers)
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqNo:    ck.GetCurrentSeqno()}
	for {
		reply := GetReply{}
		leaderId = NextServerIndex(serverLen, leaderId)
		ok := ck.servers[leaderId].Call("RaftKV.Get", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.leaderId = leaderId
			if reply.Err == OK {
				return reply.Value
			} else {
				break
			}
		}
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
	// You will have to modify this function.
	DPrintf(fmt.Sprintf("Client PutAppend start %v", ck.clientId))
	leaderId := ck.leaderId
	serverLen := len(ck.servers)
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqNo:    ck.GetCurrentSeqno()}
	for {
		reply := PutAppendReply{}
		leaderId = NextServerIndex(serverLen, leaderId)
		ok := ck.servers[leaderId].Call("RaftKV.PutAppend", &args, &reply)
		DPrintf(fmt.Sprintf("Client PutAppend rpc call return %v %v %v %v", ck.clientId, leaderId, ok, reply))
		if ok && !reply.WrongLeader {
			ck.leaderId = leaderId
			break
		}
	}
	DPrintf(fmt.Sprintf("Client PutAppend return %v", ck.clientId))
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
