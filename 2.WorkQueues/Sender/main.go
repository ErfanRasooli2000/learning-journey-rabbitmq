package main

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {

	var texts = []string{
		"hello..",
		"hello.",
		"hello...",
		"hello....",
	}

	for i := 0; i < 100; i++ {
		for _, val := range texts {
			Send(val)
		}
	}
}

func Send(text string) {
	rabbit := ConnectToRabbit()
	channel := CreateChannel(rabbit)

	defer rabbit.Close()
	defer channel.Close()

	queue := CreateQueue(channel, "work")

	PublishMessage(channel, queue, CreateMessage(text))
}

func ConnectToRabbit() *amqp.Connection {

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err)
	return conn
}
func CreateChannel(rabbit *amqp.Connection) *amqp.Channel {

	channel, err := rabbit.Channel()
	failOnError(err)
	return channel
}

func CreateQueue(channel *amqp.Channel, name string) amqp.Queue {

	queue, err := channel.QueueDeclare(
		name,
		false,
		false,
		false,
		false,
		nil,
	)

	failOnError(err)

	return queue
}

func CreateMessage(msg string) amqp.Publishing {
	return amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(msg),
	}
}

func PublishMessage(channel *amqp.Channel, queue amqp.Queue, msg amqp.Publishing) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := channel.PublishWithContext(
		ctx,
		"",
		queue.Name,
		false,
		false,
		msg,
	)

	failOnError(err)
}

func failOnError(err error) {
	if err != nil {
		log.Panicln(err.Error())
	}
}
