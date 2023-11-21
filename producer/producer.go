package main

import (
	"fmt"
	"go-rabbitmq/pkg"
	"go-rabbitmq/utils"
	"time"
)

func main() {
	// 创建Rabbitmq对象
	rabbitmq, err := pkg.NewRabbitmq("test", "8PKtAj9TDIuqAuAAL5w7", "192.168.158.132", "test")
	utils.CheckErr(err, "Failed to connect to RabbitMQ")
	// 完成任务释放资源
	defer rabbitmq.Close()
	// 创建队列
	err = rabbitmq.CreateQueue("my_queue")
	utils.CheckErr(err, "Failed to create queue")
	// 创建交换器
	err = rabbitmq.CreateExchange("my_exchange", "direct")
	utils.CheckErr(err, "Failed to create exchange")
	// 绑定队列到交换器
	err = rabbitmq.BindQueue("my_queue", "my_routing_key", "my_exchange")
	utils.CheckErr(err, "Failed to bind queue")
	// 生产者生产数据
	for i := 1; i <= 10; i++ {
		message := fmt.Sprintf("Message %d", i)
		err = rabbitmq.PublishQueue("my_exchange", "my_routing_key", message)
		utils.CheckErr(err, "Failed to publish message")
		time.Sleep(time.Second) // 每秒发送一条消息
	}
}
