package gnsq

import (
	"github.com/nsqio/go-nsq"
	"time"
)

type Producer struct {
	Id       int
	Config   *nsq.Config
	LookUpd  string
	Producer *nsq.Producer
}

func NewProducer(config *Config) (*Producer, error) {
	cf := nsq.NewConfig()

	p := Producer{
		Config:  cf,
		LookUpd: config.LookUpd[0],
	}
	p.init()
	return &p, nil
}

func (current *Producer) init() {

}

func (current *Producer) Connect() error {
	producer, err := nsq.NewProducer(current.LookUpd, current.Config)
	if err != nil {
		return err
	}
	current.Producer = producer
	return nil
}

func (current *Producer) DeferredPublish(topic string, delay time.Duration, body []byte) error {
	return current.Producer.DeferredPublish(topic, delay, body)
}
func (current *Producer) Defer() {
	current.Producer.Stop()
}
