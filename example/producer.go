package main

import (
	"time"
	"context"
	"github.com/Shopify/sarama"
	"strconv"
	"fmt"
	"github.com/imkuqin-zw/kafka"
)

func main() {
	config := &kafka.ProducerConfig{
		Zookeeper: &kafka.ZookeeperConfig{
			Root: "/brokers",
			Addrs: []string{"zoo1:2181","zoo2:2181","zoo3:2181"},
			Timeout: time.Second * 2,
		},
		Brokers: []string{"kafka1:9092", "kafka2:9092", "kafka3:9092"},
		Sync: false,
	}
	producer := kafka.NewProducer(config, nil, nil)
	var value []byte
	var i int
	for {
		value = []byte(strconv.Itoa(i))
		fmt.Println(i)
		msg := &sarama.ProducerMessage{
			Topic: "zhangwei",
			//Key: sarama.StringEncoder("test2"),
			Value: sarama.ByteEncoder(value),
		}
		producer.Input(context.Background(), msg)
		i++
		time.Sleep(time.Millisecond * 100)
	}
	producer.Close()

}