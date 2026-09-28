package messagehandler

import (
	"log/slog"
	"sync/atomic"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var nextClientID atomic.Uint64

type MessageHandler struct {
	clientID uint64
}

/*
 * Crea un handler con ID mediante el cual lo voy a identificar en las distintas etapas
 */
func NewMessageHandler() MessageHandler {
	handler := MessageHandler{clientID: nextClientID.Add(1)}
	slog.Info("Handler creado", "messagehandler", "client_id", handler.clientID)
	return handler
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeMessage(messageHandler.clientID, false, data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	message, err := inner.SerializeMessage(messageHandler.clientID, true, nil)
	if err != nil {
		return nil, err
	}
	slog.Info("EOF preparado para envio", "messagehandler", "client_id", messageHandler.clientID)
	return message, nil
}

/*
 * Devuelve los registros propios, nil para mensajes de otras instancias o EOF, y errores de validacion
 */
func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	result, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	if result.ClientID != messageHandler.clientID || result.EOF {
		return nil, nil
	}
	// Devuelve un slice
	return result.Records, nil
}
