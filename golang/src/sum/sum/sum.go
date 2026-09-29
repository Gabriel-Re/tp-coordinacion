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
	id                 int
	inputQueue         middleware.Middleware
	dispatchQueue      middleware.Middleware
	sumQueues          []middleware.Middleware
	outputQueues       []middleware.Middleware
	fruitItemsByClient map[uint64]map[string]fruititem.FruitItem
}

/*
 * Prepara la cola de trabajo, las salidas y la distribucion de Sum 0 antes de consumir
 */
func NewSum(config SumConfig) (*Sum, error) {
	if config.SumAmount < 1 {
		return nil, errors.New("la cantidad de instancias Sum debe ser mayor que cero")
	}
	if config.Id < 0 || config.Id >= config.SumAmount {
		return nil, fmt.Errorf("el ID de Sum %d debe estar entre 0 y %d", config.Id, config.SumAmount-1)
	}
	if config.AggregationAmount < 1 {
		return nil, errors.New("la cantidad de destinos debe ser mayor que cero")
	}
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	queueName := fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)
	inputQueue, err := middleware.CreateQueueMiddleware(queueName, connSettings)
	if err != nil {
		return nil, err
	}

	sum := &Sum{
		id:                 config.Id,
		inputQueue:         inputQueue,
		outputQueues:       make([]middleware.Middleware, 0, config.AggregationAmount),
		fruitItemsByClient: map[uint64]map[string]fruititem.FruitItem{},
	}
	slog.Info("sum: Cola de trabajo preparada", "sum_id", sum.id, "queue", queueName)
	for i := range config.AggregationAmount {
		queueName := fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
		outputQueue, err := middleware.CreateQueueMiddleware(queueName, connSettings)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("preparar cola de salida %q: %w", queueName, err), sum.close())
		}
		sum.outputQueues = append(sum.outputQueues, outputQueue)
		slog.Info("sum: Cola de salida preparada", "queue", queueName)
	}
	if sum.id == dispatcherID {
		if err := sum.prepareDispatcher(config, connSettings); err != nil {
			return nil, errors.Join(err, sum.close())
		}
	}

	return sum, nil
}

/*
 * Ejecuta el trabajador y, en Sum 0, el distribuidor hasta que termine alguno
 * Cierra las entradas y espera ambos consumidores antes de cerrar los publicadores
 */
func (sum *Sum) Run() (err error) {
	defer func() {
		err = errors.Join(err, sum.close())
	}()

	if sum.dispatchQueue == nil {
		return consumeMessages(sum.inputQueue, sum.handleMessage)
	}

	results := make(chan error, 2)
	go func() {
		results <- consumeMessages(sum.inputQueue, sum.handleMessage)
	}()
	go func() {
		results <- sum.runDispatcher()
	}()

	err = <-results
	err = errors.Join(err, sum.closeInputs())
	return errors.Join(err, <-results)
}

/*
 * Confirma cada mensaje solo despues del procesamiento exitoso y cierra su entrada ante errores
 */
func consumeMessages(inputQueue middleware.Middleware, handleMessage func(middleware.Message) error) error {
	var processingErr error
	consumeErr := inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		if processingErr != nil {
			return
		}
		if err := handleMessage(msg); err != nil {
			processingErr = err
			// Cerrar libera la entrega sin ACK, pero una reentrega puede duplicar envios parciales
			if closeErr := inputQueue.Close(); closeErr != nil {
				processingErr = errors.Join(processingErr, fmt.Errorf("cerrar entrada tras error: %w", closeErr))
			}
			return
		}
		ack()
	})
	return errors.Join(processingErr, consumeErr)
}

/*
 * Cierra las entradas para impedir nuevos consumos y desbloquear los que ya comenzaron
 */
func (sum *Sum) closeInputs() (err error) {
	if closeErr := sum.inputQueue.Close(); closeErr != nil {
		err = fmt.Errorf("cerrar cola de trabajo: %w", closeErr)
	}
	if sum.dispatchQueue != nil {
		if closeErr := sum.dispatchQueue.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("cerrar entrada del distribuidor: %w", closeErr))
		}
	}
	return err
}

/*
 * Cierra las entradas y los publicadores adquiridos, teniendo todos los errores de cierre
 */
func (sum *Sum) close() (err error) {
	err = sum.closeInputs()
	for i, outputQueue := range sum.outputQueues {
		if closeErr := outputQueue.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("cerrar cola de salida %d: %w", i, closeErr))
		}
	}
	for i, sumQueue := range sum.sumQueues {
		if closeErr := sumQueue.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("cerrar destino Sum %d: %w", i, closeErr))
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
		slog.Info("sum: EOF recibido", "client_id", message.ClientID, "sum_id", sum.id)
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
	slog.Info("sum: Acumulados enviados", "client_id", clientID, "sum_id", sum.id, "records", len(fruitItemMap))

	message, err := inner.SerializeSumEOF(clientID, sum.id)
	if err != nil {
		return err
	}
	if err := sum.sendToOutputs(*message); err != nil {
		return fmt.Errorf("enviar EOF: %w", err)
	}
	delete(sum.fruitItemsByClient, clientID)
	slog.Info("sum: EOF enviado", "client_id", clientID, "sum_id", sum.id)
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
