package inner

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type Message struct {
	ClientID uint64                `json:"client_id"`
	EOF      bool                  `json:"eof"`
	Records  []fruititem.FruitItem `json:"records"`
}

/*
 * Valida el contenido del mensaje
 */
func (message Message) validate() error {
	if message.ClientID == 0 {
		return errors.New("client_id debe ser mayor que cero")
	}
	if message.EOF && len(message.Records) != 0 {
		return errors.New("un EOF no puede contener registros")
	}
	for i, record := range message.Records {
		if record.Fruit == "" {
			return fmt.Errorf("registro %d: falta el nombre de la fruta", i)
		}
		//Deberia chequear si tiene cantidad?
	}
	return nil
}

/*
 * Serializa los registros con su cliente y un indicador de EOF
 */
func SerializeMessage(clientID uint64, eof bool, records []fruititem.FruitItem) (*middleware.Message, error) {
	message := Message{ClientID: clientID, EOF: eof, Records: records}
	if err := message.validate(); err != nil {
		return nil, fmt.Errorf("serializar mensaje interno: %w", err)
	}
	if message.Records == nil {
		message.Records = []fruititem.FruitItem{}
	}
	body, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("serializar mensaje interno: %w", err)
	}
	return &middleware.Message{Body: string(body)}, nil
}

/*
 * Lee y valida un mensaje identificado
 * Los registros vacios no implican EOF
 */
func DeserializeMessage(message *middleware.Message) (Message, error) {
	if message == nil {
		return Message{}, errors.New("deserializar mensaje interno: mensaje nil")
	}
	var data struct {
		ClientID *uint64 `json:"client_id"`
		EOF      *bool   `json:"eof"`
		Records  *[]struct {
			Fruit  *string
			Amount *uint32
		} `json:"records"`
	}
	decoder := json.NewDecoder(strings.NewReader(message.Body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&data); err != nil {
		return Message{}, fmt.Errorf("deserializar mensaje interno: %w", err)
	}
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		if err == nil {
			err = errors.New("se esperaba un unico objeto JSON")
		}
		return Message{}, fmt.Errorf("deserializar mensaje interno: contenido adicional: %w", err)
	}
	if data.ClientID == nil || data.EOF == nil || data.Records == nil {
		return Message{}, errors.New("deserializar mensaje interno: client_id, eof y records son obligatorios y no pueden ser null")
	}
	decoded := Message{ClientID: *data.ClientID, EOF: *data.EOF, Records: make([]fruititem.FruitItem, len(*data.Records))}
	for i, record := range *data.Records {
		if record.Fruit == nil || record.Amount == nil {
			return Message{}, fmt.Errorf("deserializar mensaje interno: registro %d: Fruit y Amount son obligatorios y no pueden ser null", i)
		}
		decoded.Records[i] = fruititem.FruitItem{Fruit: *record.Fruit, Amount: *record.Amount}
	}
	if err := decoded.validate(); err != nil {
		return Message{}, fmt.Errorf("deserializar mensaje interno: %w", err)
	}
	return decoded, nil
}
