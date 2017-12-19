package tools

import (
	"bytes"
	"errors"
)

type Cp_checker struct {
	Separator    []byte
	WildcardOne  []byte
	WildcardSome []byte
	OneRegex     []byte
	SomeRegex    []byte
	SpWord       []byte
	Dword        []byte
}

func NewCpChecker() *Cp_checker {
	return &Cp_checker{
		Separator:    []byte("/"),
		WildcardOne:  []byte("+"),
		WildcardSome: []byte("#"),
		OneRegex:     []byte("([^/#+]+/)"),
		SomeRegex:    []byte("((?:[^/#+]+/)*)"),
		SpWord:       []byte("-"),
		Dword:        []byte("$"),
	}
}

func (c *Cp_checker) GenRegexStr(topic []byte) ([]byte, error) {
	somewds, err := c.CheckSubTopic(topic)
	if err != nil {
		return nil, err
	}
	one := bytes.Replace(topic, c.WildcardOne, c.OneRegex, -1)
	if somewds == 1 {
		one = append(one[:len(one)-2], c.SomeRegex...)
	}
	return one, nil
}

func (c *Cp_checker) CheckSubTopic(topic []byte) (int, error) {
	//井号 (#) 只能用作主题字符串中的最后一个字符，并且是该级别的唯一字符。
	somewds := bytes.Count(topic, c.WildcardSome)
	if somewds > 1 {
		return 0, errors.New("can't more then one #")
	} else if somewds == 1 {
		if !bytes.HasSuffix(topic, c.WildcardSome) {
			return 0, errors.New("must suffix #")
		}
	}
	return somewds, nil
}

func (c *Cp_checker) CheckPubTopic(topic []byte) error {
	if bytes.HasPrefix(topic, c.Separator) || bytes.HasSuffix(topic, c.Separator) {
		return errors.New("can't / prefix or suffix")
	}
	//不能有 + 或 #号开头
	if bytes.Contains(topic, c.WildcardOne) || bytes.Contains(topic, c.WildcardSome) {
		return errors.New("can't contains + or #")
	}
	return nil
}
