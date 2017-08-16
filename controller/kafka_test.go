package controller

import (
	. "github.com/Shopify/sarama"
	. "k2db/util"
	"log"
	"os"
	"os/signal"
	"testing"
)

//
func Test_kafka(t *testing.T) {
	//consumer, err := NewConsumer([]string{"111.206.135.105:9092","111.206.135.106:9092","111.206.135.107:9092"}, nil)
	consumer, err := NewConsumer([]string{"192.168.112.132:9092"}, nil)

	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition("go_topic", 0, OffsetNewest)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			Debug("Consumed message", msg.Offset, string(msg.Value))
			consumed++
		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)
}
