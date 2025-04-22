package rabbit

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestWrapper(t *testing.T) {
	// развернуть рэббит в докере с панелью управления sudo docker run --rm -d -p 15672:15672 -p 5672:5672 rabbitmq:3-management

	// cL := make(map[string]ConsumerType)
	// cL["test"] = ConsumerType{QueueName: "test", PrefetchCount: 10}

	// pL := make(map[string]PublisherType)
	// pL["test"] = PublisherType{Exchange: "", RoutingKey: "test"}
	// pL["prod"] = PublisherType{Exchange: "", RoutingKey: "prod"}

	// config := configType{
	// 	Host:          "127.0.0.1",
	// 	Port:          "5672",
	// 	Pass:          "guest",
	// 	User:          "guest",
	// 	ConsumerList:  cL,
	// 	PublisherList: pL,
	// }

	ParseConfig("./config.yaml")
	// fmt.Println(GetConfig())

	rabbitDirect := NewDirect(GetConfig())
	defer rabbitDirect.Close()

	// ch := rabbitDirect.GetPublisherChan("test")
	pCh := rabbitDirect.GetChan("test", Publisher)

	for i := 0; i < 1000000; i++ {
		go pCh.Publish("test", []byte("Сообщение test "+strconv.Itoa(i+1)))
		go pCh.Publish("prod", []byte("Сообщение prod"+strconv.Itoa(i+1)))
	}

	//если мы создаем один канал для всех горутин, то проблемы описанной ниже не возникает и нагрузка распределяется на все горутины
	cCh := rabbitDirect.GetChan("test", Consumer)

	var counter uint
	m := &sync.Mutex{}
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < 10; i++ {
		// если мы создаем на каждую горутину канал,
		// то рэббит один хрен вываливает их в один или несколько каналов
		// и работают всего-лишь 1-2 горутины
		// поэксперементировать с параметром префечКаунт
		// chCons :=	rabbitDirect.GetConsumerChan("test")
		w := NewWorker(cCh, i+1, m, ctx)
		go w.run(&counter)
	}

	time.Sleep(time.Second * 60)
	cancel()
	fmt.Println("counter", counter)
}

type worker struct {
	consCh *ChannelType
	i      int
	m      *sync.Mutex
	ctx    context.Context
}

func NewWorker(consCh *ChannelType, i int, m *sync.Mutex, ctx context.Context) *worker {
	return &worker{
		consCh: consCh,
		i:      i,
		m:      m,
		ctx:    ctx,
	}
}

func (w *worker) run(counter *uint) {
	fmt.Println("стартовал воркер ", w.i)

	for {
		select {
		case msg := <-w.consCh.deliveryCh:
			fmt.Println("Получил", string(msg.Body), "воркер ", w.i)

			w.m.Lock()
			*counter++
			w.m.Unlock()
			msg.Ack(false) // подтвердить обработку сообщения — механизм Acknowledge (ack).
			// msg.Nack(false, true) вернуть сообщение в Queue при неудачной обработке — механизм Negative acknowledge (nack)

		case <-w.ctx.Done():
			return
		}
	}
}


func TestConfig(t *testing.T){
	ParseConfig("./config.yaml")
	fmt.Println(GetConfig())
}