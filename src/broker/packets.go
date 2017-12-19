package broker

import (
	"errors"
	"packet"
)

type PacketStore struct {
	Ptype packet.Type
	Payload []byte
}

func (e *Engine) SavePacket(clientid, direction string, pkt packet.Packet) error {
	id, ok := packet.PacketID(pkt)
	if ok {
		ps := PacketStore{}
		ps.Ptype = pkt.Type()
		ps.Payload = make([]byte,pkt.Len())
		pkt.Encode(ps.Payload)
		if direction == incoming {
			err := e.storein.PutObject([]byte(clientid+"-"+string(id)), ps, 0)
			if err != nil {
				return err
			}
		}
		if direction == outgoing {
			err := e.storeout.PutObject([]byte(clientid+"-"+string(id)), ps, 0)
			if err != nil {
				return err
			}
		}
	} else {
		return errors.New("PacketId nil")
	}
	return nil
}

func (e *Engine) LookupPacket(clientid, direction string, id uint16) (packet.Packet, error) {
	var ps PacketStore
	if direction == incoming {
		err := e.storein.GetObject([]byte(clientid+"-"+string(id)), &ps)
		if err != nil {
			return nil, err
		}
	}
	if direction == outgoing {
		err := e.storeout.GetObject([]byte(clientid+"-"+string(id)), &ps)
		if err != nil {
			return nil, err
		}
	}

	pkt, err := ps.Ptype.New()
	if err != nil{
		return nil, err
	}
	_, err = pkt.Decode(ps.Payload)
	if err != nil{
		return nil, err
	}
	return pkt, nil
}

func (e *Engine) DeletePacket(clientid, direction string, id uint16) error {
	if direction == incoming {
		return e.storein.Del([]byte(clientid + "-" + string(id)))
	}
	if direction == outgoing {
		return e.storeout.Del([]byte(clientid + "-" + string(id)))
	}
	return errors.New("direction error")
}

func (e *Engine) AllPackets(clientid, direction string) ([]packet.Packet, error) {
	all := make([]packet.Packet, 0)
	var ps PacketStore
	if direction == incoming {
		pks, err := e.storein.KeyStartByObject([]byte(clientid+"-"), ps)
		if err != nil {
			return nil, err
		}
		for _, v := range pks {
			p := v.Object.(*PacketStore)
			pkt, err := p.Ptype.New()
			if err != nil {
				return nil, err
			}
			_, err = pkt.Decode(p.Payload)
			if err != nil {
				return nil, err
			}
			all = append(all, pkt)
		}
	}

	if direction == outgoing {
		pks, err := e.storeout.KeyStartByObject([]byte(clientid+"-"), ps)
		if err != nil {
			return nil, err
		}
		for _, v := range pks {
			p := v.Object.(*PacketStore)
			pkt, err := p.Ptype.New()
			if err != nil {
				return nil, err
			}
			_, err = pkt.Decode(p.Payload)
			if err != nil {
				return nil, err
			}
			all = append(all, pkt)
		}
	}
	return all, nil
}