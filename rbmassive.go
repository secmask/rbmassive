package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

var exh = "mexh"
var url = "amqp://guest:guest@localhost:5672/"
var queue = "k1"
var wg *sync.WaitGroup
var msgCount uint64 = 0

func publishMessage(ch *amqp.Channel) {
	defer wg.Done()
	defer ch.Close()
	for i := 0; i < 1000000; i++ {
		err := ch.Publish(exh, queue, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte("github.com/payfriendz/messaging/rbmq"),
		})
		if err != nil {
			log.Panicln(err)
		}
		val := atomic.AddUint64(&msgCount, 1)
		if val % 1000 == 0{
			fmt.Printf("%d\n",val)
		}
	}
	ch.Confirm(false)
}

func tick() {
	t := time.NewTicker(time.Second)
	for _ = range t.C {
		fmt.Errorf("jrllo")
	}
}

func main() {
	numWorker := 10
	wg = &sync.WaitGroup{}
	wg.Add(numWorker)
	conn := MustCreateAmqpConnection(url)
	ch := MustCreateAmqpChannel(conn)
	MustExchangeDeclare(ch, exh, "topic", true, false, false, false, nil)
	q := MustQueueDeclare(ch, queue, true, false, false, false, nil)
	log.Printf("%+v\n", q)
	MustQueueBind(ch, q.Name, queue, exh, false, nil)
	start := time.Now()
	for i := 0; i < numWorker; i++ {
		go publishMessage(MustCreateAmqpChannel(conn))
	}

	wg.Wait()

	log.Println(time.Since(start).Seconds())
}
