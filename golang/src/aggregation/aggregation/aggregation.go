package aggregation

import (
	"context"
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
	id                   int
	outputQueue          middleware.Middleware
	inputQueue           middleware.Middleware
	fruitItemsByClient   map[uint64]map[string]fruititem.FruitItem
	finishedSumsByClient map[uint64]map[int]struct{}
	sumAmount            int
	topSize              int
}

/*
 * Prepara la cola de entrada compartida con Sum y la cola de resultados
 */
func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	if config.SumAmount < 1 {
		return nil, errors.New("la cantidad de instancias Sum debe ser mayor que cero")
	}
	if config.AggregationAmount < 1 {
		return nil, errors.New("la cantidad de instancias Aggregation debe ser mayor que cero")
	}
	if config.Id < 0 || config.Id >= config.AggregationAmount {
		return nil, fmt.Errorf("el ID de Aggregation %d debe estar entre 0 y %d", config.Id, config.AggregationAmount-1)
	}
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
		id:                   config.Id,
		outputQueue:          outputQueue,
		inputQueue:           inputQueue,
		fruitItemsByClient:   map[uint64]map[string]fruititem.FruitItem{},
		finishedSumsByClient: map[uint64]map[int]struct{}{},
		sumAmount:            config.SumAmount,
		topSize:              config.TopSize,
	}, nil
}

/*
 * Confirma mensajes procesados correctamente 
 */
func (aggregation *Aggregation) Run(ctx context.Context) (err error) {
	// retorno los distintos errores
	defer func() {
		slog.Info("aggregation: Consumidor finalizado", "aggregation_id", aggregation.id)
		err = errors.Join(err, aggregation.inputQueue.Close(), aggregation.outputQueue.Close())
	}()

	var processingErr error
	consumeErr := middleware.StartConsumingContext(ctx, aggregation.inputQueue, func(msg middleware.Message, ack, nack func()) {
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
 * Procesa datos o EOF y valida la instancia Sum antes de registrar su finalizacion
 */
func (aggregation *Aggregation) handleMessage(msg middleware.Message) error {
	message, err := inner.DeserializeMessage(&msg)
	if err != nil {
		return err
	}

	if message.EOF {
		if message.SumID == nil {
			return fmt.Errorf("procesar EOF del cliente %d: falta sum_id", message.ClientID)
		}
		sumID := *message.SumID
		if sumID < 0 || sumID >= aggregation.sumAmount {
			return fmt.Errorf("procesar EOF del cliente %d: sum_id %d fuera de rango [0, %d)", message.ClientID, sumID, aggregation.sumAmount)
		}
		if err := aggregation.handleEndOfRecordsMessage(message.ClientID, sumID); err != nil {
			return fmt.Errorf("procesar EOF del cliente %d de Sum %d: %w", message.ClientID, sumID, err)
		}
		return nil
	}

	aggregation.handleDataMessage(message.ClientID, message.Records)
	return nil
}

/*
 * Registra cada Sum una vez por cliente y envia el top y el EOF cuando terminaron todos
 * Elimina ambos estados del cliente solo si los dos envios son correctos
 */
func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID uint64, sumID int) error {
	finishedSums, ok := aggregation.finishedSumsByClient[clientID]
	if !ok {
		finishedSums = map[int]struct{}{}
		aggregation.finishedSumsByClient[clientID] = finishedSums
	}
	if _, ok := finishedSums[sumID]; ok {
		return nil
	}
	finishedSums[sumID] = struct{}{}
	slog.Info("aggregation: EOF registrado", "client_id", clientID, "sum_id", sumID, "aggregation_id", aggregation.id, "received", len(finishedSums), "expected", aggregation.sumAmount)
	if len(finishedSums) < aggregation.sumAmount {
		return nil
	}

	fruitTopRecords := aggregation.buildFruitTop(aggregation.fruitItemsByClient[clientID])
	message, err := inner.SerializeMessage(clientID, false, fruitTopRecords)
	if err != nil {
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		return fmt.Errorf("enviar resultado: %w", err)
	}
	slog.Info("aggregation: Resultado enviado", "client_id", clientID, "aggregation_id", aggregation.id, "records", len(fruitTopRecords))

	message, err = inner.SerializeAggregationEOF(clientID, aggregation.id)
	if err != nil {
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		return fmt.Errorf("enviar EOF: %w", err)
	}
	delete(aggregation.fruitItemsByClient, clientID)
	delete(aggregation.finishedSumsByClient, clientID)
	slog.Info("aggregation: EOF enviado", "client_id", clientID, "aggregation_id", aggregation.id)
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
