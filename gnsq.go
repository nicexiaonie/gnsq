package gnsq

import (
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
	Connect map[int]*nsq.Consumer

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
		Connect:  make(map[int]*nsq.Consumer),
	}

	r.init()
	return &r, nil
}

func (current *Consumer) init() {
	//
	go func() {
		for {
			time.Sleep(time.Second)
			// step1 开始自动统计实时连接数
			current.ConnectNum = len(current.Connect)
			//fmt.Printf("实时连接数： %d \n", current.ConnectNum)
			//fmt.Printf("配置连接数： %d \n", current.Config.MaxConnectNum)

			// step2 开始弹性伸缩
			current.ess()
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
	for i := 1; i <= current.Config.MaxConnectNum; i++ {
		current.Connect[i], _ = current.new()
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
		delete(current.Connect, k)
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

	for {
		//fmt.Printf("ess. len:%d \n", len(current.Connect))
		//fmt.Printf("ess. %d \n", current.Config.MaxConnectNum)
		if current.Config.AutoESS && len(current.Connect) != current.Config.MaxConnectNum {
			i := 0
			for k, _ := range current.Connect {
				i = k
			}
			i++
			if len(current.Connect) > current.Config.MaxConnectNum {
				current.Connect[i].Stop()
				delete(current.Connect, i)
			} else {
				current.Connect[i], _ = current.new()
			}
			//fmt.Printf("ddd  ess. len:%d \n", len(current.Connect))
			//time.Sleep(time.Second*3)
		} else {
			break
		}
	}

}
