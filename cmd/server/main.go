package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/hrncacz/boot_dev_rabbitmq_peril/internal/gamelogic"
	"github.com/hrncacz/boot_dev_rabbitmq_peril/internal/pubsub"
	"github.com/hrncacz/boot_dev_rabbitmq_peril/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connString := "amqp://guest:guest@localhost:5672"
	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer conn.Close()

	gamelogic.PrintServerHelp()

	fmt.Println("Connection successfull")
	serverChannel, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
		return
	}

	_, _, err = pubsub.DeclarAndBind(conn, routing.ExchangePerilTopic, "game_logs", routing.GameLogSlug, pubsub.Durable)
	if err != nil {
		log.Fatal(err)
		return
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		} else if input[0] == "pause" {
			log.Println("Pause game...")
			if err = pubsub.PublishJSON(serverChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true}); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		} else if input[0] == "resume" {
			log.Println("Resume game...")
			if err = pubsub.PublishJSON(serverChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false}); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		} else if input[0] == "quit" {
			log.Println("Quiting...")
			break
		} else {
			log.Println("I do not understand the message")
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Closing server...")
}
