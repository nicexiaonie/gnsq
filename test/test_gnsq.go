package main

import (
	"fmt"
	"github.com/nicexiaonie/gnsq"
	"github.com/nsqio/go-nsq"
	"time"
)

func main() {


	testDebug()

	select {}
}
func testDebug()  {
	config := gnsq.Config{
		LookUpd: []string{
			"172.25.20.245:4161",
		},
		Topic:         "content_audit_gateway_input_common",
		Channel:       "test",
		MaxInFlight: 5,
		MaxConnectNum: 3,
		AutoESS: true,
		CallFunc: func(message *nsq.Message, ctx map[string]interface{}) error {
			return nil
		},
	}


	fmt.Println(config)
	consumer, _ := gnsq.NewConsumer(&config)
	consumer.Start()
	time.Sleep(time.Second * 1)
	config.MaxConnectNum = 10
	time.Sleep(time.Second * 5)
	config.MaxConnectNum = 1
	time.Sleep(time.Second * 5)
	config.MaxConnectNum = 200
	time.Sleep(time.Second * 5)
	config.MaxConnectNum = 1
	time.Sleep(time.Second * 5)
	config.MaxConnectNum = 130
	time.Sleep(time.Second * 5)
	config.MaxConnectNum = 10
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Printf("监控连接数： ConnectNum:%d, len:%d \n", consumer.ConnectNum, len(consumer.Connect))
			fmt.Printf("监控消息数： %d \n", consumer.FinishCount)
		}
	}()

}

func testEss()  {
	config := gnsq.Config{
		LookUpd: []string{
			"172.25.20.245:4161",
		},
		Topic:         "content_audit_gateway_input_common",
		Channel:       "test",
		MaxConnectNum: 1,
		CallFunc: func(message *nsq.Message, ctx map[string]interface{}) error {
			return nil
		},
	}




	fmt.Println(config)
	consumer, _ := gnsq.NewConsumer(&config)

	_ = consumer.Start()

	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Println(consumer.ConnectNum)
		}

	}()

	time.Sleep(time.Second*3)

	config.AutoESS = true
	config.MaxConnectNum = 2

	time.Sleep(time.Second*3)

	config.AutoESS = true
	config.MaxConnectNum = 2
}
