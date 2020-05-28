package mq

import (
	"github.com/streadway/amqp"
	"log"
	"time"
)

type Queue struct {
	ch    *amqp.Channel
	queue amqp.Queue
}

func (m *Queue) PublishString(msg string) {
	err := m.ch.Publish(
		"",
		m.queue.Name,
		false,
		false,
		amqp.Publishing{
			Headers:         nil,
			ContentType:     "text/plain",
			ContentEncoding: "utf8",
			DeliveryMode:    0,
			Priority:        0,
			CorrelationId:   "",
			ReplyTo:         "",
			Expiration:      "",
			MessageId:       "",
			Timestamp:       time.Time{},
			Type:            "",
			UserId:          "",
			AppId:           "",
			Body:            []byte(msg),
		},
	)

	if err != nil {
		log.Fatal("큐 데이터 전송실패 ")
	}
}

func (m *Queue) PublishByte(msg []byte) {
	err := m.ch.Publish(
		"",
		m.queue.Name,
		false,
		false,
		amqp.Publishing{
			Headers:         nil,
			ContentType:     "text/plain",
			ContentEncoding: "utf8",
			DeliveryMode:    0,
			Priority:        0,
			CorrelationId:   "",
			ReplyTo:         "",
			Expiration:      "",
			MessageId:       "",
			Timestamp:       time.Time{},
			Type:            "",
			UserId:          "",
			AppId:           "",
			Body:            msg,
		},
	)

	if err != nil {
		log.Fatal("큐 데이터 전송실패 ")
	}
}

func (m *Queue) Consume(callback func([]byte)) {
	msgs, err := m.ch.Consume(
		m.queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatal("consumer 등록 실패")
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			callback(d.Body)
		}
	}()

	<-forever
}
