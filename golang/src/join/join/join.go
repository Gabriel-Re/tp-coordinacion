package join

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, errors.Join(err, inputQueue.Close())
	}

	return &Join{inputQueue: inputQueue, outputQueue: outputQueue}, nil
}

/*
 * Confirma resultados enviados y EOF consumidos y devuelve los errores junto con los de cierre.
 */
func (join *Join) Run() (err error) {
	defer func() {
		err = errors.Join(err, join.inputQueue.Close(), join.outputQueue.Close())
	}()

	var processingErr error
	consumeErr := join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		if processingErr != nil {
			return
		}
		if err := join.handleMessage(msg); err != nil {
			processingErr = err
			// Cerrar libera la entrega sin ACK, pero una reentrega puede duplicar el resultado.
			if closeErr := join.inputQueue.Close(); closeErr != nil {
				processingErr = errors.Join(processingErr, fmt.Errorf("cerrar entrada tras error: %w", closeErr))
			}
			return
		}
		ack()
	})
	return errors.Join(processingErr, consumeErr)
}

/*
 * Reenvia el resultado identificado, incluso vacio, y consume el EOF sin reenviarlo
 */
func (join *Join) handleMessage(msg middleware.Message) error {
	message, err := inner.DeserializeMessage(&msg)
	if err != nil {
		return err
	}
	if message.EOF {
		slog.Info("join: EOF recibido", "client_id", message.ClientID)
		return nil
	}
	if err := join.outputQueue.Send(msg); err != nil {
		return fmt.Errorf("enviar resultado del cliente %d: %w", message.ClientID, err)
	}
	slog.Info("join: Resultado enviado", "client_id", message.ClientID, "records", len(message.Records))
	return nil
}
