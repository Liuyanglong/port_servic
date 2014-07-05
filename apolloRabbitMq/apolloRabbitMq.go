package apolloRabbitMq

import (
	"apollo/amqp"
	"log"
)

type RabbitMQ struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	Logn    *log.Logger
	Loge    *log.Logger
	done    chan error
}

func (r *RabbitMQ) Connect(amqpUri string) (err error) {
	r.conn, err = amqp.Dial(amqpUri)
	if err != nil {
		r.Loge.Printf("[amqp] connect error: %s\n", err)
		return err
	}
	r.channel, err = r.conn.Channel()
	if err != nil {
		r.Loge.Printf("[amqp] get channel error: %s\n", err)
		return err
	}
	r.done = make(chan error)
	return nil
}

func (r *RabbitMQ) Publish(exchange, key, reply, body string) (err error) {
	err = r.channel.Publish(exchange, key, false, false,
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			//DeliveryMode:    deliverymode,
			DeliveryMode: amqp.Persistent,
			ReplyTo:      reply,
			Body:         []byte(body),
		},
	)
	if err != nil {
		r.Loge.Printf("[amqp] publish message error: %s\n", err)
		return err
	}
	return nil
}

func (r *RabbitMQ) DeclareExchange(name, typ string, durable bool) (err error) {
	err = r.channel.ExchangeDeclare(name, typ, durable, false, false, false, nil)
	if err != nil {
		r.Loge.Printf("[amqp] declare exchange error: %s\n", err)
		return err
	}
	return nil
}

func (r *RabbitMQ) DeleteExchange(name string) (err error) {
	err = r.channel.ExchangeDelete(name, false, false)
	if err != nil {
		r.Loge.Printf("[amqp] delete exchange error: %s\n", err)
		return err
	}
	return nil
}

func (r *RabbitMQ) DeclareQueue(name string) (err error) {
	_, err = r.channel.QueueDeclare(name, true, false, false, false, nil)
	if err != nil {
		r.Loge.Printf("[amqp] declare queue error: %s\n", err)
		return err
	}
	return nil
}

func (r *RabbitMQ) DeleteQueue(name string) (err error) {
	// TODO: other property wrapper
	_, err = r.channel.QueueDelete(name, false, false, false)
	if err != nil {
		r.Loge.Printf("[amqp] delete queue error: %s\n", err)
		return err
	}
	return nil
}

func (r *RabbitMQ) BindQueue(queue, exchange string, key string) (err error) {
	if err := r.channel.QueueBind(queue, key, exchange, false, nil); err != nil {
		r.Loge.Printf("[amqp] bind queue error: %s\n", err)
		return err
	}
	return nil
}

func (r *RabbitMQ) UnBindQueue(queue, exchange string, keys []string) (err error) {
	for _, key := range keys {
		if err = r.channel.QueueUnbind(queue, key, exchange, nil); err != nil {
			r.Loge.Printf("[amqp] unbind queue error: %s\n", err)
			return err
		}
	}
	return nil
}

func (r *RabbitMQ) ConsumeQueue(queue string, message chan []byte) (err error) {
	deliveries, err := r.channel.Consume(queue, "", true, false, false, false, nil)
	if err != nil {
		r.Loge.Printf("[amqp] consume queue error: %s\n", err)
		return err
	}
	go func(deliveries <-chan amqp.Delivery, done chan error, message chan []byte) {
		for d := range deliveries {
			message <- d.Body
		}
		done <- nil
	}(deliveries, r.done, message)
	return nil
}

func (r *RabbitMQ) Close() (err error) {
	err = r.conn.Close()
	if err != nil {
		r.Loge.Printf("[amqp] close error: %s\n", err)
		return err
	}
	return nil
}
