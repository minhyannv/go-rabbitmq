package main

import (
	"go-rabbitmq/pkg"
	"go-rabbitmq/utils"
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
	// 消费者消费数据
	err = rabbitmq.ConsumeQueue("my_queue")
	utils.CheckErr(err, "Failed to consume queue")
}
