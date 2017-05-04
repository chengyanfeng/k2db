package controller

import (
	. "datahunter.cn/util"
	. "github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"testing"
)

func Test_kafka(t *testing.T) {
	//consumer, err := NewConsumer([]string{"datahunter.cn:16080"}, nil)
	consumer, err := NewConsumer([]string{"localhost:9092"}, nil)
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
