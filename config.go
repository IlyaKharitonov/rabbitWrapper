package rabbit

import (
	"log"

	"github.com/ilyakaznacheev/cleanenv"
)

type ConfigType struct {
	Host          string                   `yaml:"Host"`
	Port          string                   `yaml:"Port"`
	User          string                   `yaml:"User"`
	Pass          string                   `yaml:"Pass"`
	ConsumerList  map[string]ConsumerType  `yaml:"ConsumerList"`
	PublisherList map[string]PublisherType `yaml:"PublisherList"`
}

type ConsumerType struct {
	PrefetchCount uint `yaml:"PrefetchCount"`
	QueueName     string `yaml:"QueueName"`
}

type PublisherType struct {
	Exchange   string `yaml:"Exchange"`
	RoutingKey string `yaml:"RoutingKey"`
}

var globalConfig *ConfigType

func GetConfig() *ConfigType {
	if globalConfig == nil {
		globalConfig = &ConfigType{}
	}

	return globalConfig
}

func ParseConfig(path ...string) {
	if len(path) != 0 {	
		//если передан путь к файлу, то парсим файл, если нет, то читаем переменные окружения
		err := cleanenv.ReadConfig(path[0], GetConfig())
		if err != nil {
			log.Fatal(err)
		}

		return
	}

	err := cleanenv.ReadEnv(GetConfig())
	if err != nil {
		log.Fatal(err)
	}

	return
}
