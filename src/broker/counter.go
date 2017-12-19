package broker

import (
	"github.com/jacoblai/yiyidb"
)

func (e *Engine) CounterNext(clientid string) uint16 {
	rpid, err := e.counter.Get([]byte(clientid))
	if err == nil{
		pid := yiyidb.KeyToIDPureUint16(rpid)
		pid++
		pidbts := yiyidb.IdToKeyPureUint16(pid)
		e.counter.Put([]byte(clientid), pidbts, 0)
		return pid
	}else{
		pidbts := yiyidb.IdToKeyPureUint16(1)
		e.counter.Put([]byte(clientid), pidbts, 0)
		return 1
	}
}

func (e *Engine) CounterReset(clientid string){
	e.counter.Del([]byte(clientid))
}

