package ddbmodel

import "time"

type Base struct {
	CreateTime int64 `json:"createTime" dynamodbav:",omitempty"`
	UpdateTime int64 `json:"updateTime" dynamodbav:",omitempty"`
}

func (m *Base) FillTime() {
	m.UpdateTime = time.Now().Unix()
	if m.CreateTime == 0 {
		m.CreateTime = m.UpdateTime
	}
}
