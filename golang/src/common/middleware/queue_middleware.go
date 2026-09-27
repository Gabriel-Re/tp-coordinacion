package middleware

import (
	"fmt"
	//"log"

)

type QueueMiddleware struct {
	name string
	brokerClient *BrokerClient
}

/*
 * Crea el broker y declara la cola compartida
 * Recibe name con el nombre de la cola y settings con hostname y puerto
 */
func NewQueueMiddleware(name string, settings ConnSettings) (Middleware, error) {
	// Abro un canal para trabajar con la cola y declaro la cola
	brokerClient, err := NewBrokerClient(settings)
	if err != nil {
		return nil, err
	}

	q := &QueueMiddleware{name: name, brokerClient: brokerClient}

	err = brokerClient.DeclareQueue(name)

	if err != nil {
		if closeErr := q.Close(); closeErr != nil {
			return nil, fmt.Errorf("set up queue: %w; cleanup: %w", err, closeErr)
		}
		return nil, fmt.Errorf("set up queue: %w", err)
	}
	return q, nil
}

/*
 * Envia un mensaje a la cola configurada usando el exchange predeterminado
 * Recibe msg con el cuerpo a enviar
 */
func (q *QueueMiddleware) Send(msg Message) error {
	if err := q.brokerClient.Publish("", []string{q.name}, msg); err != nil {
		return err
	}
	//log.Printf("queue %q: published without error, body=%q", q.name, msg.Body)
	return nil
}

/*
 * Consume mensajes de la cola configurada hasta detenerse o encontrar un error
 * Recibe un callback con mensaje, ack y nack
 */
func (q *QueueMiddleware) StartConsuming(callback func(Message, func(), func())) error {
	return q.brokerClient.StartConsuming(q.name, callback)
}

/*
 * Detiene el consumo de la cola mediante el broker
 */
func (q *QueueMiddleware) StopConsuming() error {
	return q.brokerClient.StopConsuming()
}

/*
 * Libera el canal y las conexiones usadas
 */
func (q *QueueMiddleware) Close() error {
	return q.brokerClient.Close()
}
