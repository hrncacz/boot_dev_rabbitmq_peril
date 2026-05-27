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
	fmt.Println("Starting Peril client...")
	connString := "amqp://guest:guest@localhost:5672"
	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer conn.Close()
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
		return
	}
	gameState := gamelogic.NewGameState(username)

	moveChan, _, err := pubsub.DeclarAndBind(conn, routing.ExchangePerilTopic, fmt.Sprintf("%v.%v", routing.ArmyMovesPrefix, username), fmt.Sprintf("%v.*", routing.ArmyMovesPrefix), pubsub.Transient)
	if err != nil {
		log.Fatal(err)
		return
	}

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, fmt.Sprintf("%v.%v", routing.PauseKey, username), routing.PauseKey, pubsub.Transient, handlerPause(gameState))
	if err != nil {
		log.Fatal(err)
		return
	}

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, fmt.Sprintf("%v.%v", routing.ArmyMovesPrefix, username), fmt.Sprintf("%v.%v", routing.ArmyMovesPrefix, username), pubsub.Transient, handlerMove(gameState))
	if err != nil {
		log.Fatal(err)
		return
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		} else if input[0] == "spawn" {
			log.Println("Spawning units...")
			if err = gameState.CommandSpawn(input); err != nil {
				log.Println(err)
				continue
			}
		} else if input[0] == "move" {
			val, err := gameState.CommandMove(input)
			if err != nil {
				log.Println(err)
				continue
			}
			err = pubsub.PublishJSON(moveChan, routing.ExchangePerilTopic, fmt.Sprintf("%v.%v", routing.ArmyMovesPrefix, username), val)
			if err != nil {
				log.Println(err)
				continue
			}
		} else if input[0] == "status" {
			gameState.CommandStatus()
		} else if input[0] == "help" {
			gamelogic.PrintClientHelp()
		} else if input[0] == "spam" {
			fmt.Println("Spamming not allowed yet!")
		} else if input[0] == "quit" {
			log.Println("Quiting...")
			break
		} else {
			log.Println("I do not understand the message")
			continue
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Closing client...")
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(mv gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(mv)
	}
}
