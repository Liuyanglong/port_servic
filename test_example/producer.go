package main

import (
	"amqp"
	"fmt"
	"time"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@10.81.64.109:5672/")
	if err != nil {
		fmt.Printf("connection.open: %s", err)
	}
	fmt.Println("amqp connect ok")

	defer conn.Close()

	c, err := conn.Channel()
	if err != nil {
		fmt.Printf("channel.open: %s", err)
	}
	fmt.Println("amqp create channel ok")

	err = c.ExchangeDeclare("apollo_topic_act", "topic", true, false, false, false, nil)
	if err != nil {
		fmt.Printf("exchange.declare: %v", err)
	}
	fmt.Println("declare exchange ok")

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "text/plain",
		Body:         []byte(`{"logid":"123412341234","cmd":"show_proxy_load","context":{"taskid":"71771771716","actname":"create","actnum":"2","uiIp":"192.168.110.225:10051","port":"10029"}}`),
	}

	err = c.Publish("apollo_topic_act", "apollo.192.168.110.223_10001_10050.send", false, false, msg)
	if err != nil {
		fmt.Printf("basic.publish: %v", err)
	}
	fmt.Println("amqp publish ok")
}
