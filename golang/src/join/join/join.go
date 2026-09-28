package join

import (
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
		inputQueue.Close()
		return nil, err
	}

	return &Join{inputQueue: inputQueue, outputQueue: outputQueue}, nil
}

/*
 * Confirma resultados enviados y EOF consumidos 
 * detiene el consumo en caso de errores
 */
func (join *Join) Run() {
	err := join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		if err := join.handleMessage(msg); err != nil {
			slog.Error("Error al procesar mensaje", "join", "err", err)
			// No reintento, porque el resultado podria haber llegado al destino
			if stopErr := join.inputQueue.StopConsuming(); stopErr != nil {
				slog.Error("Error al detener consumo", "join", "err", stopErr)
			}
			return
		}
		ack()
	})
	if err != nil {
		slog.Error("Error de consumo", "join", "err", err)
	}
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
