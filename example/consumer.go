package main

import (
	"time"
	"github.com/golang/glog"
	"fmt"
	"github.com/imkuqin-zw/kafka"
)

func main() {
	conf := &kafka.ConsumerConf{
		Group: "test3",
		Topics: []string{"zhangwei"},
		Offset: false,
		Zookeeper: &kafka.ZookeeperConfig{
			Root: "/kafka",
			Addrs: []string{"zoo1:2181","zoo2:2181","zoo3:2181"},
			Timeout: time.Second * 2,
		},
	}
	consumer := kafka.NewConsumer(conf)
	defer consumer.Close()
	for {
		select {
			case msg, ok := <-consumer.ConsumerGroup.Messages():
				if !ok {
					glog.Error("consumeproc exit")
					return
				}
				fmt.Println(string(msg.Value))
				consumer.ConsumerGroup.CommitUpto(msg)
				time.Sleep(10 * time.Millisecond)
		case err, ok := <-consumer.ConsumerGroup.Errors() :
			if !ok {
				glog.Error("consumeproc exit")
				return
			}
			glog.Error(err)
			fmt.Println(err.Error())
			time.Sleep(time.Second)
		}
	}

}