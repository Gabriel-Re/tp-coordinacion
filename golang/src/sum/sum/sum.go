package sum

import (
	"errors"
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
	inputQueue         middleware.Middleware
	outputQueues       []middleware.Middleware
	fruitItemsByClient map[uint64]map[string]fruititem.FruitItem
}

/*
 * Prepara la entrada y las colas de salida antes de comenzar a consumir
 */
func NewSum(config SumConfig) (*Sum, error) {
	if config.AggregationAmount < 1 {
		return nil, errors.New("la cantidad de destinos debe ser mayor que cero")
	}
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	sum := &Sum{
		inputQueue:         inputQueue,
		outputQueues:       make([]middleware.Middleware, 0, config.AggregationAmount),
		fruitItemsByClient: map[uint64]map[string]fruititem.FruitItem{},
	}
	for i := range config.AggregationAmount {
		queueName := fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
		outputQueue, err := middleware.CreateQueueMiddleware(queueName, connSettings)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("preparar cola de salida %q: %w", queueName, err), sum.close())
		}
		sum.outputQueues = append(sum.outputQueues, outputQueue)
		slog.Info("sum: Cola de salida preparada", "queue", queueName)
	}

	return sum, nil
}

/*
 * Confirma mensajes procesados correctamente y devuelve los errores de procesamiento, consumo y cierre.
 */
func (sum *Sum) Run() (err error) {
	defer func() {
		err = errors.Join(err, sum.close())
	}()

	var processingErr error
	consumeErr := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		if processingErr != nil {
			return
		}
		if err := sum.handleMessage(msg); err != nil {
			processingErr = err
			// Cerrar libera la entrega sin ACK, pero una reentrega puede duplicar envios parciales.
			if closeErr := sum.inputQueue.Close(); closeErr != nil {
				processingErr = errors.Join(processingErr, fmt.Errorf("cerrar entrada tras error: %w", closeErr))
			}
			return
		}
		ack()
	})
	return errors.Join(processingErr, consumeErr)
}

/*
 * Cierra todos los recursos
 */
func (sum *Sum) close() (err error) {
	if closeErr := sum.inputQueue.Close(); closeErr != nil {
		err = fmt.Errorf("cerrar cola de entrada: %w", closeErr)
	}
	for i, outputQueue := range sum.outputQueues {
		if closeErr := outputQueue.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("cerrar cola de salida %d: %w", i, closeErr))
		}
	}
	return err
}

/*
 * Envia el mensaje secuencialmente a cada destino
 */
func (sum *Sum) sendToOutputs(message middleware.Message) error {
	for i, outputQueue := range sum.outputQueues {
		if err := outputQueue.Send(message); err != nil {
			return fmt.Errorf("enviar a cola de salida %d: %w", i, err)
		}
	}
	return nil
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
		slog.Info("sum: EOF recibido", "client_id", message.ClientID)
		if err := sum.handleEndOfRecordMessage(message.ClientID); err != nil {
			return fmt.Errorf("procesar EOF del cliente %d: %w", message.ClientID, err)
		}
		return nil
	}

	return sum.handleDataMessage(message.ClientID, message.Records)
}

/*
 * Envia los acumulados y el EOF del cliente
 * elimina su estado solo si todos los envios fueron correctos
 */
func (sum *Sum) handleEndOfRecordMessage(clientID uint64) error {
	fruitItemMap := sum.fruitItemsByClient[clientID]
	for key := range fruitItemMap {
		fruitRecord := []fruititem.FruitItem{fruitItemMap[key]}
		message, err := inner.SerializeMessage(clientID, false, fruitRecord)
		if err != nil {
			return err
		}
		if err := sum.sendToOutputs(*message); err != nil {
			return fmt.Errorf("enviar acumulado: %w", err)
		}
	}
	slog.Info("sum: Acumulados enviados", "client_id", clientID, "records", len(fruitItemMap))

	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(clientID, true, eofMessage)
	if err != nil {
		return err
	}
	if err := sum.sendToOutputs(*message); err != nil {
		return fmt.Errorf("enviar EOF: %w", err)
	}
	delete(sum.fruitItemsByClient, clientID)
	slog.Info("sum: EOF enviado", "client_id", clientID)
	return nil
}

/*
 * Crea el acumulador del cliente si hace falta y combina sus registros llamando a Sum
 */
func (sum *Sum) handleDataMessage(clientID uint64, fruitRecords []fruititem.FruitItem) error {
	fruitItemMap, ok := sum.fruitItemsByClient[clientID]
	if !ok {
		fruitItemMap = map[string]fruititem.FruitItem{}
		sum.fruitItemsByClient[clientID] = fruitItemMap
		slog.Info("sum: Acumulador creado", "client_id", clientID)
	}
	for _, fruitRecord := range fruitRecords {
		_, ok := fruitItemMap[fruitRecord.Fruit]
		if ok {
			fruitItemMap[fruitRecord.Fruit] = fruitItemMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			fruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}
