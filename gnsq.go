package gnsq

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"sync/atomic"
	"time"
)

type Config struct {
	// lookup地址  支持多个
	LookUpd []string
	// topic 名称
	Topic string
	// 要消费的名称
	Channel string
	// 每个连接的最大消息处理数量
	MaxInFlight int
	// 最大连接数
	MaxConnectNum int

	// 开启弹性伸缩
	AutoESS bool

	CallContext map[string]interface{}
	CallFunc    func(*nsq.Message, map[string]interface{}) error
}

type Consumer struct {
	Id     int
	Config *Config
	// 实际存活的工作连接数
	ConnectNum int
	// 保存nsq连接
	Connect []*nsq.Consumer

	// 实时统计累计消息处理量
	FinishCount int64

	debug bool
	// 消息处理数量超过时退出
	debugNum int64
}

func NewConsumer(c *Config) (*Consumer, error) {
	if c.MaxInFlight < 1 {
		c.MaxInFlight = 1
	}
	if c.MaxConnectNum < 1 {
		c.MaxConnectNum = 1
	}

	r := Consumer{
		Config:   c,
		debug:    false,
		debugNum: 0,
	}

	r.init()
	return &r, nil
}

func (current *Consumer) init() {
	//
	go func() {
		for {
			time.Sleep(time.Second * 1)
			// step1 开始自动统计实时连接数
			current.ConnectNum = len(current.Connect)
			//fmt.Printf("实时连接数： %d \n", current.ConnectNum)
			//fmt.Printf("配置连接数： %d \n", current.Config.MaxConnectNum)

			// step2 开始弹性伸缩
			if current.Config.AutoESS {
				current.ess()
			}
		}
	}()

}

func (current *Consumer) Debug(n int64) {
	current.debug = true
	current.debugNum = n
	current.Config.MaxInFlight = 1
	current.Config.MaxConnectNum = 1
	_ = current.Start()
}

func (current *Consumer) Start() error {
	for i := 0; i < current.Config.MaxConnectNum; i++ {
		consumer, _ := current.new()
		current.Connect = append(current.Connect, consumer)
	}
	return nil
}
func (current *Consumer) ReStart() error {
	_ = current.Stop()
	_ = current.Start()
	return nil
}
func (current *Consumer) Stop() error {
	for k, consumer := range current.Connect {
		consumer.Stop()
		current.Connect = append(current.Connect[:k], current.Connect[k+1:]...)
	}
	return nil
}
func (current *Consumer) new() (*nsq.Consumer, error) {
	ncf := nsq.NewConfig()
	ncf.MaxInFlight = current.Config.MaxInFlight
	consumer, err := nsq.NewConsumer(current.Config.Topic, current.Config.Channel, ncf)
	if err != nil {
		return nil, err
	}
	consumer.AddHandler(nsq.HandlerFunc(current.Call))
	if err := consumer.ConnectToNSQLookupds(current.Config.LookUpd); err != nil { // 建立连接
		return nil, err
	}
	return consumer, nil
}
func (current *Consumer) Call(msg *nsq.Message) error {
	err := current.Config.CallFunc(msg, current.Config.CallContext)

	atomic.AddInt64(&current.FinishCount, 1)
	if current.debug && current.FinishCount >= current.debugNum {
		_ = current.Stop()
	}
	if err != nil {
		return err
	}
	msg.Finish()
	return nil
}

// 弹性伸缩
func (current *Consumer) ess() {
	defer func() {

	}()
	length := 0
	i := 0
	for k, _ := range current.Connect {
		length++
		i = k
	}

	fmt.Println(fmt.Sprintf("%d, %d", length, current.Config.MaxConnectNum))
	if length != current.Config.MaxConnectNum {
		fc := current.Config.MaxConnectNum - length
		for {
			if fc == 0 {
				break
			} else if fc > 0 {
				fc--
				i++
				consumer, _ := current.new()
				current.Connect = append(current.Connect, consumer)
			} else if fc < 0 {
				fc++
				current.Connect[i].Stop()
				current.Connect = append(current.Connect[:i], current.Connect[i+1:]...)
				i--
			}
		}
		time.Sleep(time.Second * 1)
	}

}
