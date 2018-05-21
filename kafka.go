package kafka

import (
	"github.com/Shopify/sarama"
	"time"
	"github.com/golang/glog"
	"context"
	"errors"
	"github.com/wvanbergen/kafka/consumergroup"
)

var (
	ErrProducerNil = errors.New("kafka producer nil")
	ErrConsumerNil = errors.New("kafka consumer nil")
)

type ZookeeperConfig struct {
	Root    string
	Addrs   []string
	Timeout time.Duration
}

type ProducerConfig struct {
	Zookeeper *ZookeeperConfig
	Brokers   []string
	Sync      bool
}

type errHandle func(*sarama.ProducerError)
type sucHandle func(*sarama.ProducerMessage)

type Producer struct {
	sarama.AsyncProducer
	sarama.SyncProducer
	conf *ProducerConfig
	errDeal errHandle
	sucDeal sucHandle
}

func NewProducer(conf *ProducerConfig, errDeal errHandle, sucDeal sucHandle) *Producer {
	p := &Producer{
		conf: conf,
		errDeal: errDeal,
		sucDeal: sucDeal,
	}
	if !conf.Sync {
		if err := p.asyncDial(); err != nil {
			go p.reAsyncDial()
		}
	} else {
		if err := p.syncDial(); err != nil {
			go p.reSyncDial()
		}
	}
	return p
}

func (p *Producer) syncDial() (err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	p.SyncProducer, err = sarama.NewSyncProducer(p.conf.Brokers, config)
	return
}

func (p *Producer) reSyncDial() {
	var err error
	for {
		if err = p.syncDial(); err == nil {
			glog.Info("kafka retry new sync producer ok")
			return
		} else {
			glog.Error("dial kafka producer error: ", err)
		}
		time.Sleep(time.Second)
	}
}

func (p *Producer) asyncDial() (err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
	if p.AsyncProducer, err = sarama.NewAsyncProducer(p.conf.Brokers, config); err == nil {
		go p.errProcess(p.errDeal)
		go p.successProcess(p.sucDeal)
	}
	return
}

func (p *Producer) reAsyncDial() {
	var err error
	for {
		if err = p.asyncDial(); err == nil {
			glog.Info("kafka retry new async producer ok")
			return
		} else {
			glog.Error("dial kafka producer error: ", err)
		}
		time.Sleep(time.Second)
	}
}

func (p *Producer) errProcess(deal errHandle) {
	err := p.Errors()
	for {
		e, ok := <- err
		if !ok {
			return
		}
		glog.Error("kafka producer send message(%v) failed error(%v)", e.Msg, e.Err)
		if deal != nil {
			deal(e)
		}
	}
}

func (p *Producer) successProcess(deal sucHandle) {
	suc := p.Successes()
	for {
		msg, ok := <-suc
		if !ok {
			return
		}
		glog.Error("kafka producer send message(%v) sucsess", msg)
		if deal != nil {
			deal(msg)
		}
	}
}

func (p *Producer) Input(c context.Context, msg *sarama.ProducerMessage) (err error) {
	if !p.conf.Sync {
		if p.AsyncProducer == nil {
			err = ErrProducerNil
		} else {
			msg.Metadata = c
			p.AsyncProducer.Input() <- msg
		}
	} else {
		if p.SyncProducer == nil {
			err = ErrProducerNil
		} else {
			if _, _, err = p.SyncProducer.SendMessage(msg); err != nil {
				glog.Error(err)
			}
		}
	}
	return
}

func (p *Producer) Close() (err error) {
	if !p.conf.Sync {
		if p.AsyncProducer != nil {
			return p.AsyncProducer.Close()
		}
	}
	if p.SyncProducer != nil {
		return p.SyncProducer.Close()
	}
	return
}

type ConsumerConf struct {
	Group 		string
	Topics		[]string
	Offset		bool
	Zookeeper	*ZookeeperConfig
}

type Consumer struct {
	ConsumerGroup *consumergroup.ConsumerGroup
	conf          *ConsumerConf
}

func NewConsumer(conf *ConsumerConf) *Consumer {
	kc := &Consumer{
		conf: conf,
	}
	if err := kc.dial(); err != nil {
		glog.Error("redial zk: ", err)
		go kc.redial()
	}
	return kc
}

func (c *Consumer) dial() (err error) {
	cfg := consumergroup.NewConfig()
	if c.conf.Offset {
		cfg.Offsets.Initial = sarama.OffsetNewest
	} else {
		cfg.Offsets.Initial = sarama.OffsetOldest
	}
	c.ConsumerGroup, err = consumergroup.JoinConsumerGroup(c.conf.Group, c.conf.Topics, c.conf.Zookeeper.Addrs, cfg)
	return
}

func (c *Consumer) redial() {
	var err error
	for {
		if err = c.dial(); err == nil {
			glog.Info("kafka retry new consumer ok")
			return
		} else {
			glog.Error("dial kafka consumer error: ", err)
		}
		time.Sleep(time.Second)
	}
}

func (c *Consumer) Close() error {
	if c.ConsumerGroup != nil {
		return c.ConsumerGroup.Close()
	}
	return nil
}