package main

import "github.com/streadway/amqp"
import (
	"fmt"
	"log"
)

func FailOnError(err error, msg interface{}) {
	if err != nil {
		if ms, ok := msg.(func() string); ok {
			msg = ms()
		}
		fmt.Printf("%v - %v\n", msg, err)
		log.Panicln(err)
	}
}

func MustCreateAmqpConnection(url string) *amqp.Connection {
	conn, err := amqp.Dial(url)
	FailOnError(err, "Failed to create AMQP Connection")
	return conn
}

func MustCreateAmqpChannel(conn *amqp.Connection) *amqp.Channel {
	ch, err := conn.Channel()
	FailOnError(err, "Failed to create AMQP Channel")
	return ch
}

func MustQueueDeclare(ch *amqp.Channel,
name string,
durable bool,
autoDelete bool,
exclusive bool,
noWait bool,
args amqp.Table) amqp.Queue {
	q, err := ch.QueueDeclare(
		name,
		durable,
		autoDelete,
		exclusive,
		noWait,
		args,
	)
	FailOnError(err, func() string {
		return fmt.Sprintf("Failed to declare queue [%s]", name)
	})
	return q
}

func MustConsume(ch *amqp.Channel,
queue string,
consumer string,
autoAck bool,
exclusive bool,
noLocal bool,
noWait bool,
args amqp.Table) <-chan amqp.Delivery {
	c, err := ch.Consume(
		queue,
		consumer,
		autoAck,
		exclusive,
		noLocal,
		noWait,
		args,
	)
	FailOnError(err, func() string {
		return fmt.Sprintf("Failed to open Consumer, queue [%s], consumer [%s]", queue, consumer)
	})
	return c
}

func MustPublish(ch *amqp.Channel,
exchange string,
key string,
mandatory bool,
immediate bool,
msg amqp.Publishing) {
	FailOnError(ch.Publish(
		exchange,
		key,
		mandatory,
		immediate,
		msg,
	), func() string {
		return fmt.Sprintf("Failed to publish message, exchange [%s],key [%s]",
			exchange,
			key,
		)
	})
}

func MustQos(ch *amqp.Channel,
prefetchCount int,
prefetchSize int,
global bool) {
	FailOnError(ch.Qos(
		prefetchCount,
		prefetchSize,
		global,
	), "Failed to set Qos")
}

func MustExchangeDeclare(ch *amqp.Channel,
name string,
etype string,
durable bool,
autoDeleted bool,
interval bool,
noWait bool,
args amqp.Table) {
	FailOnError(ch.ExchangeDeclare(
		name,
		etype,
		durable,
		autoDeleted,
		interval,
		noWait,
		args,
	), func() string {
		return fmt.Sprintf("Failed to declare exchange: [%s],type=[%s]",
			name,
			etype,
		)
	})
}

func MustQueueBind(ch *amqp.Channel,
qname string,
routingKey string,
exh string,
noWait bool,
arg amqp.Table) {
	FailOnError(ch.QueueBind(qname, routingKey, exh, noWait, arg), func() string {
		return fmt.Sprintf("Failed to bind queue to an exchange, queuename: [%s], routingKey [%s], exh [%s]",
			qname,
			routingKey,
			exh)
	})
}
