package middleware

import (
	"errors"
	"context"
	"log/slog"
	"time"
)

const consumerStopCheckInterval = 10 * time.Millisecond

var (
	ErrMessageMiddlewareMessage      = errors.New("message middleware: message error")
	ErrMessageMiddlewareDisconnected = errors.New("message middleware: disconnected")
	ErrMessageMiddlewareClose        = errors.New("message middleware: close error")
)

type Message struct {
	Body string
}

type ConnSettings struct {
	Hostname string
	Port     int
}

type Middleware interface {

	//Comienza a escuchar a la cola/exchange e invoca a callbackFunc tras
	//cada mensaje de datos o de control con el cuerpo del mensaje.
	//callbackFunc tiene como parámetro:
	// msg - El struct tal y como lo recibe el método Send.
	// ack - Una función que hace ACK del mensaje recibido.
	// nack - Una función que hace NACK del mensaje recibido.
	//Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
	//Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
	StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error

	//Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	//no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	//Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
	StopConsuming() error

	//Envía un mensaje a la cola o a los tópicos con el que se inicializó el exchange.
	//Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
	//Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
	Send(msg Message) error

	//Se desconecta de la cola o exchange al que estaba conectado.
	//Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareClose.
	Close() error
}

/*
 * Cancela el consumo con el contexto y espera las entregas en transito antes de retornar
 */
func StartConsumingContext(ctx context.Context, input Middleware, callback func(Message, func(), func())) error {
	finished := make(chan error, 1)
	go func() {
		finished <- input.StartConsuming(callback)
	}()
	select {
	case err := <-finished:
		return err
	case <-ctx.Done():
	}

	slog.Info("middleware: Apagado solicitado")
	ticker := time.NewTicker(consumerStopCheckInterval)
	defer ticker.Stop()
	for {
		// Repito porque StopConsuming, si no esta consumiendo, no devuelve error y no cierra el canal finished
		if err := input.StopConsuming(); err != nil {
			closeErr := input.Close()
			return errors.Join(err, closeErr, <-finished)
		}
		select {
		case err := <-finished:
			return err
		case <-ticker.C:
		}
	}
}
