package join

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
	inputQueue                   middleware.Middleware
	outputQueue                  middleware.Middleware
	aggregationAmount            int
	topSize                      int
	candidatesByClient           map[uint64][]fruititem.FruitItem
	finishedAggregationsByClient map[uint64]map[int]struct{}
}

/*
 * Valida la configuracion y prepara las colas y el estado separado por cliente
 */
func NewJoin(config JoinConfig) (*Join, error) {
	if config.AggregationAmount < 1 {
		return nil, errors.New("la cantidad de instancias Aggregation debe ser mayor que cero")
	}
	if config.TopSize < 0 {
		return nil, errors.New("el tamaño del top no puede ser negativo")
	}
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, errors.Join(err, inputQueue.Close())
	}

	return &Join{
		inputQueue:                   inputQueue,
		outputQueue:                  outputQueue,
		aggregationAmount:            config.AggregationAmount,
		topSize:                      config.TopSize,
		candidatesByClient:           map[uint64][]fruititem.FruitItem{},
		finishedAggregationsByClient: map[uint64]map[int]struct{}{},
	}, nil
}

/*
 * Confirma resultados enviados y EOF consumidos y devuelve los errores junto con los de cierre.
 */
func (join *Join) Run(ctx context.Context) (err error) {
	defer func() {
		slog.Info("join: Consumidor finalizado")
		err = errors.Join(err, join.inputQueue.Close(), join.outputQueue.Close())
	}()

	var processingErr error
	consumeErr := middleware.StartConsumingContext(ctx, join.inputQueue, func(msg middleware.Message, ack, nack func()) {
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
 * Procesa tops parciales
 */
func (join *Join) handleMessage(msg middleware.Message) error {
	message, err := inner.DeserializeMessage(&msg)
	if err != nil {
		return err
	}
	if message.EOF {
		if message.AggregationID == nil {
			return fmt.Errorf("procesar EOF del cliente %d: falta aggregation_id", message.ClientID)
		}
		aggregationID := *message.AggregationID
		if aggregationID < 0 || aggregationID >= join.aggregationAmount {
			return fmt.Errorf("procesar EOF del cliente %d: aggregation_id %d fuera de rango [0, %d)", message.ClientID, aggregationID, join.aggregationAmount)
		}
		if err := join.handleEndOfRecordsMessage(message.ClientID, aggregationID); err != nil {
			return fmt.Errorf("procesar EOF del cliente %d de Aggregation %d: %w", message.ClientID, aggregationID, err)
		}
		return nil
	}
	join.handleDataMessage(message.ClientID, message.Records)
	return nil
}

/*
 * Combina el top parcial
 */
func (join *Join) handleDataMessage(clientID uint64, records []fruititem.FruitItem) {
	candidates := append(join.candidatesByClient[clientID], records...)
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[j].Less(candidates[i])
	})
	candidates = candidates[:min(join.topSize, len(candidates))]
	join.candidatesByClient[clientID] = candidates
	slog.Info("join: Top parcial recibido", "client_id", clientID, "records", len(records), "candidates", len(candidates))
}

/*
 * Registra cada Aggregation una vez por cliente
 * envia el top global cuando terminaron todas
 */
func (join *Join) handleEndOfRecordsMessage(clientID uint64, aggregationID int) error {
	finishedAggregations, ok := join.finishedAggregationsByClient[clientID]
	if !ok {
		finishedAggregations = map[int]struct{}{}
		join.finishedAggregationsByClient[clientID] = finishedAggregations
	}
	if _, ok := finishedAggregations[aggregationID]; ok {
		return nil
	}
	finishedAggregations[aggregationID] = struct{}{}
	slog.Info("join: EOF recibido", "client_id", clientID, "aggregation_id", aggregationID, "remaining", join.aggregationAmount-len(finishedAggregations))
	if len(finishedAggregations) < join.aggregationAmount {
		return nil
	}

	candidates := join.candidatesByClient[clientID]
	message, err := inner.SerializeMessage(clientID, false, candidates)
	if err != nil {
		return err
	}
	if err := join.outputQueue.Send(*message); err != nil {
		return fmt.Errorf("enviar resultado del cliente %d: %w", clientID, err)
	}
	delete(join.candidatesByClient, clientID)
	delete(join.finishedAggregationsByClient, clientID)
	slog.Info("join: Resultado enviado", "client_id", clientID, "records", len(candidates))
	return nil
}
