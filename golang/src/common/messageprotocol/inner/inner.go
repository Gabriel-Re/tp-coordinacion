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
	ClientID      uint64                `json:"client_id"`
	EOF           bool                  `json:"eof"`
	SumID         *int                  `json:"sum_id,omitempty"`
	AggregationID *int                  `json:"aggregation_id,omitempty"`
	Records       []fruititem.FruitItem `json:"records"`
}

/*
 * Valida el contenido y que un EOF identifique como maximo una etapa
 */
func (message Message) validate() error {
	if message.ClientID == 0 {
		return errors.New("client_id debe ser mayor que cero")
	}
	if message.EOF && len(message.Records) != 0 {
		return errors.New("un EOF no puede contener registros")
	}
	if message.SumID != nil {
		if *message.SumID < 0 {
			return errors.New("sum_id no puede ser negativo")
		}
		if !message.EOF {
			return errors.New("sum_id solo puede aparecer en un EOF")
		}
	}
	if message.AggregationID != nil {
		if *message.AggregationID < 0 {
			return errors.New("aggregation_id no puede ser negativo")
		}
		if !message.EOF {
			return errors.New("aggregation_id solo puede aparecer en un EOF")
		}
		if message.SumID != nil {
			return errors.New("un mensaje no puede incluir sum_id y aggregation_id a la vez")
		}
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
	return serializeMessage(Message{ClientID: clientID, EOF: eof, Records: records})
}

/*
 * Serializa un EOF con el cliente y la instancia Sum que lo envia
 */
func SerializeSumEOF(clientID uint64, sumID int) (*middleware.Message, error) {
	return serializeMessage(Message{ClientID: clientID, EOF: true, SumID: &sumID})
}

/*
 * Serializa un EOF con el cliente y el Aggregation que lo envia
 */
func SerializeAggregationEOF(clientID uint64, aggregationID int) (*middleware.Message, error) {
	return serializeMessage(Message{ClientID: clientID, EOF: true, AggregationID: &aggregationID})
}

/*
 * Valida y serializa el mensaje
 */
func serializeMessage(message Message) (*middleware.Message, error) {
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
		ClientID      *uint64 `json:"client_id"`
		EOF           *bool   `json:"eof"`
		SumID         *int    `json:"sum_id,omitempty"`
		AggregationID *int    `json:"aggregation_id,omitempty"`
		Records       *[]struct {
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
	decoded := Message{ClientID: *data.ClientID, EOF: *data.EOF, SumID: data.SumID, AggregationID: data.AggregationID, Records: make([]fruititem.FruitItem, len(*data.Records))}
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
