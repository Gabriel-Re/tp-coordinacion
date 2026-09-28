package sum

import (
	"fmt"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	fruitItemMap   map[string]fruititem.FruitItem
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		fruitItemMap:   map[string]fruititem.FruitItem{},
	}, nil
}

/*
 * Consume y confirma mensajes procesados correctamente
 * Se para el consumo en caso de encontrar errores
 */
func (sum *Sum) Run() {
	err := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		if err := sum.handleMessage(msg); err != nil {
			slog.Error("Error al procesar mensaje", "sum", "err", err)
			// Ojo aca, que puedo duplicar datos enviados
			if stopErr := sum.inputQueue.StopConsuming(); stopErr != nil {
				slog.Error("Error al detener consumo", "sum", "err", stopErr)
			}
			return
		}
		ack()
	})
	if err != nil {
		slog.Error("Error de consumo", "sum", "err", err)
	}
}

/*
 * Procesa datos o EOF y pasa explicitamente el ID al emitir los acumulados
 */
func (sum *Sum) handleMessage(msg middleware.Message) error {
	message, err := inner.DeserializeMessage(&msg)
	if err != nil {
		return err
	}

	if message.EOF {
		slog.Info("EOF recibido", "sum", "client_id", message.ClientID)
		if err := sum.handleEndOfRecordMessage(message.ClientID); err != nil {
			return fmt.Errorf("procesar EOF del cliente %d: %w", message.ClientID, err)
		}
		return nil
	}

	return sum.handleDataMessage(message.Records)
}

func (sum *Sum) handleEndOfRecordMessage(clientID uint64) error {
	for key := range sum.fruitItemMap {
		fruitRecord := []fruititem.FruitItem{sum.fruitItemMap[key]}
		message, err := inner.SerializeMessage(clientID, false, fruitRecord)
		if err != nil {
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			return fmt.Errorf("enviar acumulado: %w", err)
		}
	}
	slog.Info("Acumulados enviados", "sum", "client_id", clientID, "records", len(sum.fruitItemMap))

	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(clientID, true, eofMessage)
	if err != nil {
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		return fmt.Errorf("enviar EOF: %w", err)
	}
	slog.Info("EOF enviado", "sum", "client_id", clientID)
	return nil
}

func (sum *Sum) handleDataMessage(fruitRecords []fruititem.FruitItem) error {
	for _, fruitRecord := range fruitRecords {
		_, ok := sum.fruitItemMap[fruitRecord.Fruit]
		if ok {
			sum.fruitItemMap[fruitRecord.Fruit] = sum.fruitItemMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.fruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}
