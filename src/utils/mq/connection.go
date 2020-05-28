package mq

import (
	"github.com/streadway/amqp"
	"log"
)

type Connection struct {
	*amqp.Connection
}

func NewConnection(server string) *Connection {
	// 서버 연결
	conn, err := amqp.Dial(server)
	if err != nil {
		log.Fatal("레빗엠큐 로그인 실패", server)
	}

	return &Connection{conn}
}

func (c *Connection) NewBizQueue(queueName string) *Queue {
	// 체널 연결
	ch, err := c.Channel()
	if err != nil {
		log.Fatal("레빗엠큐 체널 오픈 실패")
	}

	// 큐 생성하기
	queue, err := ch.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatal(queueName, " 큐 생성 실패")
	}

	return &Queue{
		ch:    ch,
		queue: queue,
	}
}
