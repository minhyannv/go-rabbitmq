package pkg

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

// Rabbitmq 初始化rabbitmq连接
type Rabbitmq struct {
	Conn *amqp.Connection
	err  error
}

// NewRabbitmq 创建rabbitmq连接
func NewRabbitmq(username, password, ip, vhost string) (*Rabbitmq, error) {
	mqURL := fmt.Sprintf("amqp://%s:%s@%s:5672/%s", username, password, ip, vhost)
	conn, err := amqp.Dial(mqURL)
	if err != nil {
		return nil, err
	}
	rabbitmq := &Rabbitmq{
		Conn: conn,
	}
	return rabbitmq, nil
}

// CreateQueue 创建queue队列
/*
  如果只有一方声明队列，可能会导致下面的情况：
   a)消费者是无法订阅或者获取不存在的MessageQueue中信息
   b)消息被Exchange接受以后，如果没有匹配的Queue，则会被丢弃

  为了避免上面的问题，所以最好选择两方一起声明
  ps:如果生产者/消费者尝试建立一个已经存在的消息队列，Rabbit MQ不会做任何事情，并返回建立成功
*/
func (rabbitmq *Rabbitmq) CreateQueue(queueName string) error {
	ch, err := rabbitmq.Conn.Channel()
	defer ch.Close()
	if err != nil {
		return err
	}
	_, err = ch.QueueDeclare(
		queueName, // 队列名
		true,      //  是否持久化
		false,     // 是否自动删除(前提是至少有一个消费者连接到这个队列，之后所有与这个队列连接的消费者都断开时，才会自动删除。注意：生产者创建这个队列，或者没有消费者与这个队列连接时，都不会自动删除这个队列)
		false,     // 是否为排他队列（排他的队列仅对“首次”声明的conn可见[一个conn中的其他channel也能访问该队列]，conn结束后队列删除）
		false,     // 是否阻塞
		nil,       // 额外属性
	)
	if err != nil {
		return err
	}
	return nil
}

// DeleteQueue 删除queue队列
func (rabbitmq *Rabbitmq) DeleteQueue(queueName string) error {
	ch, err := rabbitmq.Conn.Channel()
	defer ch.Close()
	if err != nil {
		return err
	}
	_, err = ch.QueueDelete(
		queueName, // name
		false,     // IfUnused
		false,     // ifEmpty
		true,      // noWait
	)
	if err != nil {
		return err
	}
	return nil
}

// CreateExchange 创建交换器
func (rabbitmq *Rabbitmq) CreateExchange(exchangeName, exchangeType string) error {
	ch, err := rabbitmq.Conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	err = ch.ExchangeDeclare(
		exchangeName, // 交换器名
		exchangeType, // exchange type：一般用fanout、direct、topic
		true,         // 是否持久化
		false,        // 是否自动删除（自动删除的前提是至少有一个队列或者交换器与这和交换器绑定，之后所有与这个交换器绑定的队列或者交换器都与此解绑）
		false,        // 设置是否内置的。true表示是内置的交换器，客户端程序无法直接发送消息到这个交换器中，只能通过交换器路由到交换器这种方式
		false,        // 是否阻塞
		nil,          // 额外属性
	)
	if err != nil {
		return err
	}
	return nil
}

// BindQueue 绑定队列
func (rabbitmq *Rabbitmq) BindQueue(queueName, routingKey, exchangeName string) error {
	ch, err := rabbitmq.Conn.Channel()
	defer ch.Close()
	if err != nil {
		return err
	}
	err = ch.QueueBind(
		queueName,    // 绑定的队列名称
		routingKey,   // 用于消息路由分发的key
		exchangeName, // 绑定的exchange名
		false,        // 是否阻塞
		nil,          // 额外属性
	)
	if err != nil {
		return err
	}
	return nil
}

// PublishQueue 上传消息到queue队列中
func (rabbitmq *Rabbitmq) PublishQueue(exchangeName, routingKey string, body string) error {
	ch, err := rabbitmq.Conn.Channel()
	defer ch.Close()
	if err != nil {
		return err
	}
	err = ch.Publish(
		exchangeName, // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		})
	if err != nil {
		return err
	}
	log.Printf(" [x] Sent %s", body)
	return nil
}

// ConsumeQueue 从队列中取出数据并且消费
func (rabbitmq *Rabbitmq) ConsumeQueue(queueName string) error {
	ch, err := rabbitmq.Conn.Channel()
	defer ch.Close()
	if err != nil {
		return err
	}
	err = ch.Qos(
		3,     // 消息预取数设置
		0,     // 预取大小设置，单位为字节
		false, // 标志位，用来指明上述设置是只对当前的 Channel 有效（如果设置为 false），还是对整个 Connection 有效（如果设置为 true）
	)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return err
	}
	forever := make(chan bool)
	go func() {
		for d := range msgs {
			// 消费数据
			log.Printf("Received a message: %s", d.Body)
			// 标记消费
			err = d.Ack(false)
			if err != nil {
				log.Panic(err)
			}
		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
	return nil
}

// GetReadyCount 统计正在队列中准备且还未消费的数据
func (rabbitmq *Rabbitmq) GetReadyCount(queueName string) (int, error) {
	count := 0
	ch, err := rabbitmq.Conn.Channel()
	defer ch.Close()
	if err != nil {
		return count, err
	}
	state, err := ch.QueueInspect(queueName)
	if err != nil {
		return count, err
	}
	return state.Messages, nil
}

// GetConsumCount 获取到队列中正在消费的数据，这里指的是正在有多少数据被消费
func (rabbitmq *Rabbitmq) GetConsumCount(queueName string) (int, error) {
	count := 0
	ch, err := rabbitmq.Conn.Channel()
	defer ch.Close()
	if err != nil {
		return count, err
	}
	state, err := ch.QueueInspect(queueName)
	if err != nil {
		return count, err
	}
	return state.Consumers, nil
}

// ClearQueue 清理队列
func (rabbitmq *Rabbitmq) ClearQueue(queueName string) (string, error) {
	ch, err := rabbitmq.Conn.Channel()
	defer ch.Close()
	if err != nil {
		return "", err
	}
	_, err = ch.QueuePurge(queueName, false)
	if err != nil {
		return "", err
	}
	return "Delete queue success", nil
}

// Close 释放资源,建议NewRabbitMQ获取实例后 配合defer使用
func (rabbitmq *Rabbitmq) Close() {
	if rabbitmq.Conn == nil {
		return
	}
	if rabbitmq.Conn.IsClosed() {
		return
	}
	rabbitmq.Conn.Close()
}
