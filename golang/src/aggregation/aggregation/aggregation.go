package aggregation

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue        middleware.Middleware
	inputQueue         middleware.Middleware
	fruitItemsByClient map[uint64]map[string]fruititem.FruitItem
	topSize            int
}

/*
 * Prepara la cola de entrada compartida con Sum y la cola de resultados
 */
func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	queueName := fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)
	inputQueue, err := middleware.CreateQueueMiddleware(queueName, connSettings)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("iniciando cola de entrada %q: %w", queueName, err), outputQueue.Close())
	}
	slog.Info("aggregation: Cola de entrada lista", "queue", queueName)

	return &Aggregation{
		outputQueue:        outputQueue,
		inputQueue:         inputQueue,
		fruitItemsByClient: map[uint64]map[string]fruititem.FruitItem{},
		topSize:            config.TopSize,
	}, nil
}

/*
 * Confirma mensajes procesados correctamente 
 */
func (aggregation *Aggregation) Run() (err error) {
	// retorno los distintos errores
	defer func() {
		err = errors.Join(err, aggregation.inputQueue.Close(), aggregation.outputQueue.Close())
	}()

	var processingErr error
	consumeErr := aggregation.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		if processingErr != nil {
			return
		}
		if err := aggregation.handleMessage(msg); err != nil {
			processingErr = err
			// No reintento un envio parcial, podria duplicar data ya enviada
			if closeErr := aggregation.inputQueue.Close(); closeErr != nil {
				processingErr = errors.Join(processingErr, fmt.Errorf("cerrar entrada por error: %w", closeErr))
			}
			return
		}
		ack()
	})
	return errors.Join(processingErr, consumeErr)
}

/*
 * Procesa datos o EOF y conserva el ID al producir el resultado
 */
func (aggregation *Aggregation) handleMessage(msg middleware.Message) error {
	message, err := inner.DeserializeMessage(&msg)
	if err != nil {
		return err
	}

	if message.EOF {
		slog.Info("aggregation: EOF recibido", "client_id", message.ClientID)
		if err := aggregation.handleEndOfRecordsMessage(message.ClientID); err != nil {
			return fmt.Errorf("procesar EOF del cliente %d: %w", message.ClientID, err)
		}
		return nil
	}

	aggregation.handleDataMessage(message.ClientID, message.Records)
	return nil
}

/*
 * Envia el top y el EOF del cliente  
 * elimina su estado solo si ambos envios son correctos
 */
func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID uint64) error {
	fruitTopRecords := aggregation.buildFruitTop(aggregation.fruitItemsByClient[clientID])
	message, err := inner.SerializeMessage(clientID, false, fruitTopRecords)
	if err != nil {
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		return fmt.Errorf("enviar resultado: %w", err)
	}
	slog.Info("aggregation: Resultado enviado", "client_id", clientID, "records", len(fruitTopRecords))

	eofMessage := []fruititem.FruitItem{}
	message, err = inner.SerializeMessage(clientID, true, eofMessage)
	if err != nil {
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		return fmt.Errorf("enviar EOF: %w", err)
	}
	delete(aggregation.fruitItemsByClient, clientID)
	slog.Info("aggregation: EOF enviado", "client_id", clientID)
	return nil
}

/*
 * Crea el acumulador del cliente si hace falta y combina los registros usando Sum
 */
func (aggregation *Aggregation) handleDataMessage(clientID uint64, fruitRecords []fruititem.FruitItem) {
	fruitItemMap, ok := aggregation.fruitItemsByClient[clientID]
	if !ok {
		fruitItemMap = map[string]fruititem.FruitItem{}
		aggregation.fruitItemsByClient[clientID] = fruitItemMap
		slog.Info("aggregation: Acumulador creado", "client_id", clientID)
	}
	for _, fruitRecord := range fruitRecords {
		if _, ok := fruitItemMap[fruitRecord.Fruit]; ok {
			fruitItemMap[fruitRecord.Fruit] = fruitItemMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			fruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(fruitItemMap map[string]fruititem.FruitItem) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(fruitItemMap))
	for _, item := range fruitItemMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
