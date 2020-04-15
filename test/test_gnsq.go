package main

import (
	"fmt"
	"github.com/nicexiaonie/gnsq"
	"github.com/nsqio/go-nsq"
	"time"
)

func main() {

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

	select {}
}
