package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/ian-kent/gofigure"
	"github.com/mgutz/ansi"
)

type config struct {
	gofigure   interface{} `order:"env,flag"`
	BrokerList string      `env:"BROKER_LIST" flag:"broker-list" flagDesc:"The comma separated list of brokers in the Kafka cluster"`
	Topic      string      `env:"TOPIC" flag:"topic" flagDesc:"The topic to consume"`
	Partitions string      `env:"PARTITIONS" flag:"partitions" flagDesc:"The partitions to consume, can be 'all' or comma-separated numbers"`
	Offset     string      `env:"OFFSET" flag:"offset" flagDesc:"The offset to start with, can be 'oldest' or 'newest'"`
}

var cfg *config

func main() {
	cfg = &config{
		Partitions: "all",
		Offset:     "newest",
	}
	err := gofigure.Gofigure(cfg)
	if err != nil {
		log.Fatal(err)
	}

	if cfg.BrokerList == "" {
		log.Fatal(errors.New("-broker-list or BROKER_LIST is required (as a comma-separated list)"))
	}

	if cfg.Topic == "" {
		log.Fatal(errors.New("-topic or TOPIC is required"))
	}

	var initialOffset int64
	switch cfg.Offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		log.Fatal(errors.New("-offset/OFFSET should be `oldest` or `newest`"))
	}

	c, err := sarama.NewConsumer(strings.Split(cfg.BrokerList, ","), nil)
	if err != nil {
		log.Fatal(fmt.Errorf("Failed to start consumer: %s", err))
	}

	partitionList, err := getPartitions(c)
	if err != nil {
		log.Fatal(fmt.Errorf("Failed to get the list of partitions: %s", err))
	}

	var (
		messages = make(chan *sarama.ConsumerMessage, 256)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		log.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(cfg.Topic, partition, initialOffset)
		if err != nil {
			log.Fatal(fmt.Errorf("Failed to start consumer for partition %d: %s", partition, err))
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}

	go func() {
		for msg := range messages {
			if len(msg.Value) > 0 {
				var m map[string]interface{}
				err := json.Unmarshal(msg.Value, &m)
				if err != nil {
					log.Printf("ERROR: failed parsing json: %s", err)
				}

				prettyPrint(m)
			}
		}
	}()

	wg.Wait()
	log.Println("Done consuming topic", cfg.Topic)
	close(messages)

	if err := c.Close(); err != nil {
		log.Fatal(fmt.Errorf("Failed to close consumer: %s", err))
	}
}

func getPartitions(c sarama.Consumer) ([]int32, error) {
	if cfg.Partitions == "all" {
		return c.Partitions(cfg.Topic)
	}

	tmp := strings.Split(cfg.Partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}

func prettyPrint(m map[string]interface{}) {
	var name, context, message string
	var data map[string]interface{}

	if n, ok := m["event"]; ok {
		if name, ok = n.(string); ok {
		}
	}
	if c, ok := m["context"]; ok {
		if context, ok = c.(string); ok {
		}
	}
	if msg, ok := m["message"]; ok {
		if message, ok = msg.(string); ok {
		}
	}
	if d, ok := m["data"]; ok {
		if data, ok = d.(map[string]interface{}); ok {
		}
	}

	ctx := ""
	if len(context) > 0 {
		ctx = "[" + context + "] "
	}
	msg := ""
	if len(message) > 0 {
		msg = ": " + fmt.Sprintf("%s", message)
		delete(data, "message")
	}
	if name == "error" && len(msg) == 0 {
		if err, ok := data["error"]; ok {
			msg = ": " + fmt.Sprintf("%s", err)
			delete(data, "error")
		}
	}
	col := ansi.DefaultFG
	switch name {
	case "error":
		col = ansi.LightRed
	case "trace":
		col = ansi.Blue
	case "debug":
		col = ansi.Green
	case "request":
		col = ansi.Cyan
	case "data-integrity":
		col = ansi.LightMagenta
	}
	fmt.Fprintf(os.Stdout, "%s%s %s%s%s%s\n", col, m["created"], ctx, name, msg, ansi.DefaultFG)
	if data != nil {
		for k, v := range data {
			fmt.Fprintf(os.Stdout, "  -> %s: %+v\n", k, v)
		}
	}
	return
}
