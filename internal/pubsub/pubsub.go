package pubsub

import (
	"context"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	mJSON, err := json.Marshal(val)
	if err != nil {
		return err
	}
	ctx := context.Background()
	msg := amqp.Publishing{
		ContentType: "application/json",
		Body:        mJSON,
	}
	ch.PublishWithContext(ctx, exchange, key, false, false, msg)
	return nil
}

func DeclarAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	newChan, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
		return nil, amqp.Queue{}, err
	}
	newQueue, err := newChan.QueueDeclare(queueName, queueType == Durable, queueType == Transient, queueType == Transient, false, nil)
	if err != nil {
		log.Fatal(err)
		return nil, amqp.Queue{}, err
	}
	if err = newChan.QueueBind(queueName, key, exchange, false, nil); err != nil {
		log.Fatal(err)
		return nil, amqp.Queue{}, err
	}
	return newChan, newQueue, nil
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T)) error {
	newChan, _, err := DeclarAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	consumedChan, err := newChan.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for i := range consumedChan {
			var val T
			json.Unmarshal(i.Body, val)
			handler(val)
			i.Ack(false)
		}
	}()
	return nil
}
