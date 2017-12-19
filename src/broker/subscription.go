package broker

import (
	"regexp"
	"gopkg.in/vmihailenco/msgpack.v2"
	"bytes"
	"packet"
)

func (e *Engine) SaveSubscription(chname string, sub *packet.Subscription) error {
	e.subscriptions.PutObject([]byte(chname+"-"+sub.Topic), sub, 0)
	return nil
}

func (e *Engine) LookupSubscription(chname, topic string) (*packet.Subscription, error) {
	var si packet.Subscription
	pks, err := e.subscriptions.KeyStartByObject([]byte(chname+"-"), si)
	if err != nil {
		return nil, err
	}
	for _, v := range pks {
		o := v.Object.(*packet.Subscription)
		regx, err := e.checker.GenRegexStr([]byte(o.Topic))
		if err != nil {
			continue
		}
		//select first matched topic to handle publish qos
		matched, err := regexp.MatchString(string(regx), topic)
		if matched {
			return o, nil
		}
	}
	return nil, nil
}

func (e *Engine) LookupSubscriptions(chname, topic string) ([]*packet.Subscription, error) {
	var si packet.Subscription
	pks, err := e.subscriptions.KeyStartByObject([]byte(chname+"-"), si)
	if err != nil {
		return nil, err
	}
	res := make([]*packet.Subscription, 0)
	for _, v := range pks {
		o := v.Object.(*packet.Subscription)
		regx, err := e.checker.GenRegexStr([]byte(o.Topic))
		if err != nil {
			continue
		}
		//select first matched topic to handle publish qos
		matched, err := regexp.MatchString(string(regx), topic)
		if matched {
			res = append(res, o)
		}
	}
	return res, nil
}

type SubsLookup struct {
	Sub *packet.Subscription
	ClientId string
}

func (e *Engine) LookupSubscriptionsByTopic(topic string) []SubsLookup {
	pks := e.subscriptions.AllByKV()
	res := make([]SubsLookup, 0)
	for _, v := range pks {
		var si packet.Subscription
		err := msgpack.Unmarshal(v.Value, &si)
		if err == nil {
			regx, err := e.checker.GenRegexStr([]byte(si.Topic))
			if err != nil {
				continue
			}
			//select first matched topic to handle publish qos
			matched, err := regexp.MatchString(string(regx), topic)
			if matched {
				cid := string(bytes.Split(v.Key,[]byte("-"))[0])
				if _, ok := e.clients[cid]; !ok {
					o := SubsLookup{}
					o.Sub = &si
					o.ClientId = cid
					res = append(res, o)
				}
			}
		}
	}
	return res
}

func (e *Engine) DeleteSubscription(chname, topic string) error {
	e.subscriptions.Del([]byte(chname + "-" + topic))
	return nil
}

func (e *Engine) AllSubscriptions(chname string) ([]*packet.Subscription, error) {
	all := make([]*packet.Subscription, 0)
	var si packet.Subscription
	pks, err := e.subscriptions.KeyStartByObject([]byte(chname+"-"), si)
	if err != nil {
		return nil, err
	}
	for _, v := range pks {
		all = append(all, v.Object.(*packet.Subscription))
	}
	return all, nil
}
