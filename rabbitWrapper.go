package rabbit

import (
	"fmt"
	"log"

	rabbitmq "github.com/rabbitmq/amqp091-go"
)

//  замысел: сделать обертку для библиотеки реббита, чтобы скрыть лишние подробности (подключения, создания каналов, инициализация очередей)
//  сделать несколько объектов, каждый из которых отвечает за свой обменник (Fanout; Direct; Topic; Headers.)
//  конструктор принимает конфиг, в котором параметры подключения и структуры для декларирования сущностей в рэббите
//  для каждой очереди будет создаваться свой коннект, далее на основе эих  очередей воркеры буду создавать себе каналы 1 канал - 1 горутина
//  эта идея провалилась. Реббит вываливает всю нагрзку в 1-2 канала, как итог работают не все горутины, а 1-2, читающие из этих каналов.
//  не знаю насколько это правильно, но решил сделать 1 коннект к очереди и в ней 1 канал, который пользуют все горутины
//  пакет должен восстанавливать соединение, если оно утеряно
//  закрывает соединения после отработки программы



type ChannelType struct {
	deliveryCh <-chan rabbitmq.Delivery
	ch         *rabbitmq.Channel
}

type rabbitDirect struct {
	consumerConn  *rabbitmq.Connection
	publisherConn *rabbitmq.Connection

	consumerList  map[string]*ChannelType // ключ название очереди, значение структура с данными для этой очереди
	publisherList map[string]*ChannelType
}

func NewDirect(config *configType) *rabbitDirect {
	rabbit := &rabbitDirect{
		consumerList:  make(map[string]*ChannelType, len(config.ConsumerList)),
		publisherList: make(map[string]*ChannelType, len(config.PublisherList)),
	}

	// 1 создаем конекты. на просторах нашел такую рекомендацию
	// Возможно, имелся в виду RabbitMQ. При работе с этим брокером рекомендуется придерживаться правила «один процесс — одно соединение».
	// Для процессов, которые занимаются и публикацией, и потреблением,
	// правило расширяется до «один процесс — два соединения» (одно для публикации, другое для потребления).
	var err error

	if config.ConsumerList != nil && len(config.ConsumerList) != 0 {
		rabbit.consumerConn, err = rabbitmq.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", config.User, config.Pass, config.Host, config.Port))
		if err != nil {
			log.Fatal(err)
		}
	}

	if config.PublisherList != nil && len(config.PublisherList) != 0 {
		rabbit.publisherConn, err = rabbitmq.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", config.User, config.Pass, config.Host, config.Port))
		if err != nil {
			log.Fatal(err)
		}
	}

	//2 создаем каналы
	for k := range config.ConsumerList {
		qName := config.ConsumerList[k].QueueName
        pCount := config.ConsumerList[k].PrefetchCount
		rabbit.consumerList[qName] = rabbit.getConsumerChan(qName,int(pCount))
	}

	for k := range config.PublisherList {
		routingKey := config.PublisherList[k].RoutingKey
		rabbit.publisherList[routingKey] = rabbit.getPublisherChan(routingKey)
	}

	return rabbit
}

const (
	Publisher = 1
	Consumer  = 2
)

func (r *rabbitDirect) getConsumerChan(key string, pCount int) *ChannelType {

	ch, err := r.consumerConn.Channel()
	if err != nil {
		log.Fatal("Ошибка получения канала ", err)
		return nil
	}

	_, err = ch.QueueDeclare(
		key,
		true,  // durable - сохранить или не сохранить сообщения на диск при перезапуске из этой очереди
		false, // autoDelete - автоудаление очереди после закрытия соединения
		false, // exclusive - очереди будут автоматически удаляться, когда потребитель отключается
		false, //noWait - не совсем понял что делает
		nil,   // какие-то параметры
	)
	if err != nil {
		log.Fatal(err)
	}

    err = ch.Qos(pCount, 0, false)
    if err != nil {
		log.Fatal(err)
	}

	consumeCh, err := ch.Consume(key, "", false, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	return &ChannelType{ch: ch, deliveryCh: consumeCh}
}

func (r *rabbitDirect) getPublisherChan(key string) *ChannelType {

	ch, err := r.publisherConn.Channel()
	if err != nil {
		log.Fatal("Ошибка получения канала", err)
		return nil
	}

	_, err = ch.QueueDeclare(
		key,
		true,  // durable - сохранить или не сохранить сообщения на диск при перезапуске из этой очереди
		false, // autoDelete - автоудаление очереди после закрытия соединения
		false, // exclusive - очереди будут автоматически удаляться, когда потребитель отключается
		false, //noWait - не совсем понял что делает
		nil,   // какие-то параметры
	)
	if err != nil {
		log.Fatal(err)
	}

	return &ChannelType{ch: ch}
}

func (r *rabbitDirect) GetChan(key string, typeCh uint) *ChannelType {
	if typeCh != 1 && typeCh != 2 {
		log.Fatal("Ошибка получения канала. Получен несуществующий тип ", typeCh)
	}

	if typeCh == 1 {
		ch, ok := r.publisherList[key]
		if !ok {
			log.Fatal("Такого паблишер канала нет", key)
		}

		return ch
	}

	if typeCh == 2 {
		ch, ok := r.consumerList[key]
		if !ok {
			log.Fatal("Такого консюмер канала нет", key)
		}

		return ch
	}

	return nil
}

// закрывает все ранее созданные коннекты
// насколько я понял, каналы закрывать отдельно не нужно
// если убивешь соединение, то и каналы внутри этого соединения рипаются
func (r *rabbitDirect) Close() {
	
    err := r.consumerConn.Close()
    if err != nil {
        log.Fatal("Ошибка закрытия соединения консюмера")
        return
    }


    err = r.publisherConn.Close()
    if err != nil {
        log.Fatal("Ошибка закрытия соединения паблишера")
        return
    }
	
}


func (r *rabbitDirect) Reconnect() {

}

//учесть, что может быть ситуация, при которой сообщение не будет доставлено
//в документации сказано что нужно слушать специальный канал, чтобы избежать этого
func (c *ChannelType) Publish(key string, data []byte) {
	err := c.ch.Publish(
		"",
		key,
		false,
		false,
		rabbitmq.Publishing{
			ContentType: "text/plain",
			Body:        data,
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}
