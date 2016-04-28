package main
import (
	"fmt"
	"log"
	"testing"
)

func TestRBMQ(t *testing.T) {
	conn := MustCreateAmqpConnection(url)
	ch := MustCreateAmqpChannel(conn)
	q := MustQueueDeclare(ch, queue, true, false, false, false, nil)
	log.Printf("%+v\n", q)
}

func TestConsumer(t *testing.T) {
	conn := MustCreateAmqpConnection(url)
	ch := MustCreateAmqpChannel(conn)
	q := MustQueueDeclare(ch, queue, true, false, false, false, nil)
	log.Printf("%+v\n", q)
	msgs := MustConsume(ch, queue, "", true, false, false, false, nil)
	i := 0
	for d := range msgs {
		i++
		fmt.Printf("%d. %s\n", i, d.Body)
	}
}

