package broker

import (
	"packet"
)

func (e *Engine) StoreWill(clientid string, msg packet.Message) error {
	e.wills.PutObject([]byte(clientid), msg, 0)
	return nil
}

func (e *Engine) ClearWill(clientid string) error {
	e.wills.Del([]byte(clientid))
	return nil
}

func (e *Engine) SendWill(clientid string) {
	var willmsg packet.Message
	err := e.wills.GetObject([]byte(clientid), &willmsg)
	if err == nil {
		for _, v := range e.cmqtt.Match(willmsg.Topic) {
			v.(*Client).finishPublish(&willmsg)
		}
	}
}