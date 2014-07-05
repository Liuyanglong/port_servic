package main

import (
	"amqp"
	"fmt"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@10.81.64.109:5672/")
	if err != nil {
		fmt.Printf("connection.open: %s", err)
	}
	defer conn.Close()

	c, err := conn.Channel()
	if err != nil {
		fmt.Printf("channel.open: %s", err)
	}

	err = c.ExchangeDeclare("apollo_direct", "direct", true, false, false, false, nil)
	if err != nil {
		fmt.Printf("exchange.declare: %s", err)
	}

	type bind struct {
		queue string
		key   string
	}

	bindings := []bind{
		bind{"page", "topic.xxx"},
		bind{"email", "topic.*"},
		bind{"firehose", "apollo.scheduler"},
	}

	for _, b := range bindings {
		_, err = c.QueueDeclare(b.queue, true, false, false, false, nil)
		if err != nil {
			fmt.Printf("queue.declare: %v", err)
		}

		err = c.QueueBind(b.queue, b.key, "apollo_direct", false, nil)
		if err != nil {
			fmt.Printf("queue.bind: %v", err)
		}
	}

	err = c.Qos(3, 0, false)
	if err != nil {
		fmt.Printf("basic.qos: %v", err)
	}

	pages, err := c.Consume("page", "info_check", false, false, false, false, nil)
	if err != nil {
		fmt.Printf("basic.consume: %v", err)
	}

	go func() {
		for d := range pages {
			fmt.Printf(
				"page got %dB delivery: [%v] %q\n\n",
				len(d.Body),
				d.DeliveryTag,
				d.Body,
			)
			d.Ack(false)
			fmt.Println("page here?")
		}
	}()

	emails, err := c.Consume("email", "info_shit", false, false, false, false, nil)
	if err != nil {
		fmt.Printf("basic.consume: %v", err)
	}

	go func() {
		for d := range emails {
			fmt.Printf(
				"email got %dB delivery: [%v] %q\n\n",
				len(d.Body),
				d.DeliveryTag,
				d.Body,
			)

			d.Ack(false)
		}
	}()

	firehose, err := c.Consume("firehose", "apollo.scheduler", true, false, false, false, nil)
	if err != nil {
		fmt.Printf("basic.consume: %v", err)
	}

	for d := range firehose {
		fmt.Printf(
			"firehose got %dB delivery: [%v] %q\n\n",
			len(d.Body),
			d.DeliveryTag,
			d.Body,
		)

	}

	err = c.Cancel("pager", false)
	if err != nil {
		fmt.Printf("basic.cancel: %v", err)
	}

}
