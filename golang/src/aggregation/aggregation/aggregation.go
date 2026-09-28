package aggregation

import (
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
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	fruitItemMap  map[string]fruititem.FruitItem
	topSize       int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		fruitItemMap:  map[string]fruititem.FruitItem{},
		topSize:       config.TopSize,
	}, nil
}

/*
 * Consume y confirma mensajes procesados correctamente 
 * detiene el consumo en caso de error
 */
func (aggregation *Aggregation) Run() {
	err := aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		if err := aggregation.handleMessage(msg); err != nil {
			slog.Error("Error al procesar mensaje", "aggregation", "err", err)
			// No reintento un envio parcial, podria duplicar data ya enviada
			if stopErr := aggregation.inputExchange.StopConsuming(); stopErr != nil {
				slog.Error("Error al detener consumo", "aggregation", "err", stopErr)
			}
			return
		}
		ack()
	})
	if err != nil {
		slog.Error("Error de consumo", "aggregation", "err", err)
	}
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
		slog.Info("EOF recibido", "aggregation", "client_id", message.ClientID)
		if err := aggregation.handleEndOfRecordsMessage(message.ClientID); err != nil {
			return fmt.Errorf("procesar EOF del cliente %d: %w", message.ClientID, err)
		}
		return nil
	}

	aggregation.handleDataMessage(message.Records)
	return nil
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID uint64) error {
	fruitTopRecords := aggregation.buildFruitTop()
	message, err := inner.SerializeMessage(clientID, false, fruitTopRecords)
	if err != nil {
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		return fmt.Errorf("enviar resultado: %w", err)
	}
	slog.Info("Resultado enviado", "aggregation", "client_id", clientID, "records", len(fruitTopRecords))

	eofMessage := []fruititem.FruitItem{}
	message, err = inner.SerializeMessage(clientID, true, eofMessage)
	if err != nil {
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		return fmt.Errorf("enviar EOF: %w", err)
	}
	slog.Info("EOF enviado", "aggregation", "client_id", clientID)
	return nil
}

func (aggregation *Aggregation) handleDataMessage(fruitRecords []fruititem.FruitItem) {
	for _, fruitRecord := range fruitRecords {
		if _, ok := aggregation.fruitItemMap[fruitRecord.Fruit]; ok {
			aggregation.fruitItemMap[fruitRecord.Fruit] = aggregation.fruitItemMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			aggregation.fruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop() []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(aggregation.fruitItemMap))
	for _, item := range aggregation.fruitItemMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
