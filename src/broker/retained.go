package broker

import (
	"regexp"
	"packet"
)

func (e *Engine) StoreRetained(msg *packet.Message) error {
	e.retained.PutObject([]byte(msg.Topic), msg.Copy(), 0)
	return nil
}

func (e *Engine) ClearRetained(topic string) error {
	e.retained.Del([]byte(topic))
	return nil
}

func (e *Engine) QueueRetained(topic string) error {
	var msg packet.Message
	pks := e.retained.AllByObject(msg)
	for _, v := range pks {
		regx, err := e.checker.GenRegexStr(v.Key)
		if err != nil {
			continue
		}
		//select first matched topic to handle publish qos
		matched, err := regexp.MatchString(string(regx), topic)
		if matched {
			for _, cv := range e.cmqtt.Match(topic) {
				cv.(*Client).Publish(v.Object.(*packet.Message))
			}
		}
	}
	return nil
}
