package main

import (
	"bytes"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {

	connection := ConnectToRabbit()
	defer connection.Close()

	channel := CreateChannel(connection)
	defer channel.Close()

	queue := CreateQueue(channel, "work")

	err1 := channel.Qos(1, 0, false)
	failOnError(err1)

	msgs, err := channel.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err)

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			dotCount := bytes.Count(d.Body, []byte("."))
			time.Sleep(time.Microsecond * time.Duration(dotCount))
			d.Ack(false)
		}
	}()

	<-forever
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

func failOnError(err error) {
	if err != nil {
		log.Panicln(err.Error())
	}
}
