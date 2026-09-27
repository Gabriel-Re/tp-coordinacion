package middleware

import (
	"fmt"

)

type ExchangeMiddleware struct {
	name string
	keys []string
	brokerClient *BrokerClient
}

/*
 * Crea el broker y declara el exchange
 * Recibe name, las routing keys para enviar o consumir y settings con hostname y puerto
 */
func NewExchangeMiddleware(name string, keys []string, settings ConnSettings) (Middleware, error) {
	// Abro un canal para trabajar con el exchange
	brokerClient, err := NewBrokerClient(settings)
	if err != nil {
		return nil, err
	}

	keysCopy := make([]string, len(keys))
	copy(keysCopy, keys)
	
	e := &ExchangeMiddleware{name: name, keys: keysCopy, brokerClient: brokerClient}

	err = brokerClient.DeclareExchange(name)

	if err != nil {
		if closeErr := e.Close(); closeErr != nil {
			return nil, fmt.Errorf("set up exchange: %w; cleanup: %w", err, closeErr)
		}
		return nil, fmt.Errorf("set up exchange: %w", err)
	}
	return e, nil
}

/*
 * Publica el mensaje en cada routing key configurada del exchange
 * Recibe msg con el cuerpo a enviar
 */
func (e *ExchangeMiddleware) Send(msg Message) error {
	return e.brokerClient.Publish(e.name, e.keys, msg)
}

/*
 * Crea una suscripcion privada y consume mensajes de las keys configuradas
 * Recibe un callback con mensaje, ack y nack. Debe confirmar antes de retornar
 */
func (e *ExchangeMiddleware) StartConsuming(callback func(Message, func(), func())) error {
	// Creo la cola privada y los binds
	return e.brokerClient.StartExchangeConsuming(e.name, e.keys, callback)
}

/*
 * Detiene el consumo de la suscripcion mediante el broker
 */
func (e *ExchangeMiddleware) StopConsuming() error {
	return e.brokerClient.StopConsuming()
}

/*
 * Libera el canal y las conexiones usadas
 */
func (e *ExchangeMiddleware) Close() error {
	return e.brokerClient.Close()
}
