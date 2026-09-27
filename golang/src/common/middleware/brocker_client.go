package middleware

import (
	"context"
	"errors"
	"fmt"
	//"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

const consumerTag = "queue-consumer"

type BrokerClient struct {
	mu        sync.Mutex
	consuming bool
	stopping  bool
	conn    *amqp.Connection
	channel *amqp.Channel
	closed  bool
}

/*
 * Abre una conexion y un canal con RabbitMQ
 * Recibe settings con el hostname y el puerto del broker
 */
func NewBrokerClient(settings ConnSettings) (*BrokerClient, error) {
	address := fmt.Sprintf("amqp://guest:guest@%s:%d/", settings.Hostname, settings.Port)
	// Conexion con RabbitMQ
	conn, err := amqp.Dial(address)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	c := &BrokerClient{conn: conn}
	c.channel, err = conn.Channel()
	if err != nil {
		if closeErr := c.Close(); closeErr != nil {
			return nil, fmt.Errorf("open channel: %w; cleanup: %w", err, closeErr)
		}
		return nil, fmt.Errorf("open channel: %w", err)
	}
	return c, nil
}

/*
 * Declara una cola compartida para enviar y consumir mensajes
 * Recibe el nombre de la cola
 */
func (c *BrokerClient) DeclareQueue(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.channel.QueueDeclare(
		name,  // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	return err
}

/*
 * Declara un exchange de tipo direct para enrutar por coincidencia de key
 * Recibe el nombre del exchange
 */
func (c *BrokerClient) DeclareExchange(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	return c.channel.ExchangeDeclare(
		name,                // name
		amqp.ExchangeDirect, // type
		false,               // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
}

/*
 * Publica el mensaje una vez por cada routing key
 * Recibe exchange, keys de destino y msg a enviar
 */
func (c *BrokerClient) Publish(exchange string, keys []string, msg Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed || c.conn.IsClosed() {
		return fmt.Errorf("send: %w", ErrMessageMiddlewareDisconnected)
	}

	// Cada key recibe una publicaciom, si alguna falla, las anteriores ya se enviaron
	for _, key := range keys {
		err := c.channel.PublishWithContext(context.Background(),
			exchange, // exchange
			key,    // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			})
		if err != nil {
			if c.conn.IsClosed() {
				return fmt.Errorf("send key %q: %w: %w", key, ErrMessageMiddlewareDisconnected, err)
			}
			return fmt.Errorf("send key %q: %w: %w", key, ErrMessageMiddlewareMessage, err)
		}
		//log.Printf("exchange %q: published without error, key=%q, body=%q", exchange, key, msg.Body)
	}
	return nil
}

/*
 * Consume una cola hasta que pare el consumo o haya un error
 * Recibe queueName y un callback que debe llamar a ack o nack antes de retornar
 */
func (c *BrokerClient) StartConsuming(queueName string, callback func(Message, func(), func())) error {
	return c.consume(queueName, "", nil, callback)
}

/*
 * Consume el exchange mediante una cola privada vinculada a las keys indicadas
 * Recibe exchange, keys y un callback que debe llamar a ack o nack antes de retornar
 */
func (c *BrokerClient) StartExchangeConsuming(exchange string, keys []string, callback func(Message, func(), func())) error {
	return c.consume("", exchange, keys, callback)
}

/*
 * Procesa las entregas en serie y comprueba los errores de ACK/NACK
 * Recibe queueName o exchange con keys, y un callback
 */
func (c *BrokerClient) consume(queueName string, exchange string, keys []string, callback func(Message, func(), func())) error {
	if callback == nil {
		return fmt.Errorf("consume: %w: nil callback", ErrMessageMiddlewareMessage)
	}
	
	messages, err := c.startConsumer(queueName, exchange, keys)
	if err != nil {
		return err
	}

	defer func() {
		c.mu.Lock()
		c.consuming = false
		c.mu.Unlock()
	}()

	for delivery := range messages {
		// Callback debe llamar a ack o nack antes de retornar, en la misma goroutine
		var confirmationErr error
		confirmed := false
		// Solo la primera cuenta
		ack := func() {
			if !confirmed {
				confirmed = true
				confirmationErr = delivery.Ack(false)
			}
		}
		nack := func() {
			if !confirmed {
				confirmed = true
				// Reencolo solo esta entrega para que pueda procesarse otra vez
				confirmationErr = delivery.Nack(false, true)
			}
		}
		//log.Printf("routing key %q: recibe body=%q", delivery.RoutingKey, delivery.Body)
		callback(Message{Body: string(delivery.Body)}, ack, nack)

		if confirmationErr != nil {
			c.mu.Lock()
			closed := c.closed
			c.mu.Unlock()

			if closed {
				// El cierre local corta la confirmacion
				// RabbitMQ reencola las entregas que quedaron sin confirmar al cerrar el canal
				return nil
			}

			if c.conn.IsClosed() {
				return fmt.Errorf("confirm delivery: %w: %w", ErrMessageMiddlewareDisconnected, confirmationErr)
			}
			return fmt.Errorf("confirm delivery: %w: %w", ErrMessageMiddlewareMessage, confirmationErr)
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.closed {
		return nil
	}
	if c.conn.IsClosed() {
		return fmt.Errorf("consume: %w: deliveries channel closed", ErrMessageMiddlewareDisconnected)
	}
	if c.stopping {
		return nil
	}
	return fmt.Errorf("consume: %w: deliveries channel closed", ErrMessageMiddlewareMessage)
}

/*
 * Configura el prefetch e inicia el consumidor
 * Recibe queueName para una cola existente, o exchange y keys para una suscripcion privada
 */
func (c *BrokerClient) startConsumer(queueName string, exchange string, keys []string) (<-chan amqp.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.closed || c.conn.IsClosed() {
		return nil, fmt.Errorf("consume: %w", ErrMessageMiddlewareDisconnected)
	}
	if c.consuming {
		return nil, fmt.Errorf("consume: %w: consumer already running", ErrMessageMiddlewareMessage)
	}

	// Canal AMQP
	err := c.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		if c.conn.IsClosed() {
			return nil, fmt.Errorf("set qos: %w: %w", ErrMessageMiddlewareDisconnected, err)
		}
		return nil, fmt.Errorf("set qos: %w: %w", ErrMessageMiddlewareMessage, err)
	}

	if exchange != "" {
		queueName, err = c.declareSubscription(exchange, keys)
		if err != nil {
			if c.conn.IsClosed() {
				return nil, fmt.Errorf("subscribe: %w: %w", ErrMessageMiddlewareDisconnected, err)
			}
			return nil, fmt.Errorf("subscribe: %w: %w", ErrMessageMiddlewareMessage, err)
		}
	}

	// Canal de go donde recibo los mensajes
	messages, err := c.channel.Consume(
		queueName,      // queue
		consumerTag, // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		if exchange != ""{
			if cleanupErr := c.deleteSubscription(queueName); cleanupErr != nil {
				err = fmt.Errorf("%w; cleanup: %w", err, cleanupErr)
			}
		}
		if c.conn.IsClosed() {
			return nil, fmt.Errorf("consume: %w: %w", ErrMessageMiddlewareDisconnected, err)
		}
		return nil, fmt.Errorf("consume: %w: %w", ErrMessageMiddlewareMessage, err)
	}

	c.consuming = true
	c.stopping = false
	return messages, nil
}

/*
 * Crea una cola exclusiva y la vincula al exchange
 * Recibe el nombre del exchange y las routing keys que debe escuchar
 */
func (c *BrokerClient) declareSubscription(exchange string, keys []string) (string, error) {
	queue, err := c.channel.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return "", err
	}

	// Cada suscriptor tiene su cola; todas sus keys apuntan a ella.
	for _, key := range keys {
		err = c.channel.QueueBind(
			queue.Name, // queue name
			key,        // routing key
			exchange,   // exchange
			false,      // no-wait
			nil,        // arguments
		)
		if err != nil {
			if cleanupErr := c.deleteSubscription(queue.Name); cleanupErr != nil {
				err = fmt.Errorf("%w; cleanup: %w", err, cleanupErr)
			}
			return "", err
		}
	}
	return queue.Name, nil
}

/*
 * Elimina una cola de suscripcion, abriendo otro canal si hace falta
 * Recibe name con el nombre de la cola a eliminar
 */
func (c *BrokerClient) deleteSubscription(name string) (err error) {
	if c.conn.IsClosed() {
		// RabbitMQ elimina las colas exclusivas al cerrar su conexion.
		return nil
	}
	channel := c.channel
	if channel.IsClosed() {
		channel, err = c.conn.Channel()
		if err != nil {
			return err
		}
		defer func() {
			if closeErr := channel.Close(); closeErr != nil && !errors.Is(closeErr, amqp.ErrClosed) {
				err = errors.Join(err, closeErr)
			}
		}()
	}
	_, err = channel.QueueDelete(name, false, false, false)
	return err
}

/*
 * Cancela el consumidor activo sin cerrar la conexion ni esperar al callback
 * Si ya esta detenido, no hace nada
 */
func (c *BrokerClient) StopConsuming() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.closed {
		return nil
	}
	if c.conn.IsClosed() {
		return fmt.Errorf("stop consuming: %w", ErrMessageMiddlewareDisconnected)
	}
	if !c.consuming || c.stopping {
		return nil
	}

	// Cancel cierra el canal
	// no espero al callback con el lock tomado
	if err := c.channel.Cancel(consumerTag, false); err != nil {
		if c.conn.IsClosed() {
			return fmt.Errorf("stop consuming: %w: %w", ErrMessageMiddlewareDisconnected, err)
		}
		return fmt.Errorf("stop consuming: %w: %w", ErrMessageMiddlewareMessage, err)
	}
	c.stopping = true
	return nil
}

/*
 * Cierra el canal y la conexion, informando los errores de cierre
 */
func (c *BrokerClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	// Si no se pudo abrir el canal, solo hay una conex
	if c.channel == nil {
		if err := c.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			return fmt.Errorf("close connection: %w: %w", ErrMessageMiddlewareClose, err)
		}
		return nil
	}

	// Intento cerrar ambos
	channelErr := c.channel.Close()
	connectionErr := c.conn.Close()
	// Si RabbitMQ ya los cerro, no queda ningun recurso pendiente de cerrar.
	if errors.Is(channelErr, amqp.ErrClosed) {
		channelErr = nil
	}
	if errors.Is(connectionErr, amqp.ErrClosed) {
		connectionErr = nil
	}
	if channelErr != nil && connectionErr != nil {
		return fmt.Errorf("%w: close channel: %w; close connection: %w", ErrMessageMiddlewareClose, channelErr, connectionErr)
	}
	if channelErr != nil {
		return fmt.Errorf("close channel: %w: %w", ErrMessageMiddlewareClose, channelErr)
	}
	if connectionErr != nil {
		return fmt.Errorf("close connection: %w: %w", ErrMessageMiddlewareClose, connectionErr)
	}
	return nil
}
