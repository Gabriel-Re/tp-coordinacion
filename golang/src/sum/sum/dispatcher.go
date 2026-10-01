package sum

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const dispatcherID = 0

/*
 * Prepara la entrada del gateway y declara todos los destinos antes de distribuir
 */
func (sum *Sum) prepareDispatcher(config SumConfig, connSettings middleware.ConnSettings) error {
	dispatchQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return fmt.Errorf("preparar entrada del distribuidor: %w", err)
	}
	sum.dispatchQueue = dispatchQueue
	sum.sumQueues = make([]middleware.Middleware, 0, config.SumAmount)
	for i := range config.SumAmount {
		queueName := fmt.Sprintf("%s_%d", config.SumPrefix, i)
		sumQueue, err := middleware.CreateQueueMiddleware(queueName, connSettings)
		if err != nil {
			return fmt.Errorf("preparar destino Sum %d en cola %q: %w", i, queueName, err)
		}
		sum.sumQueues = append(sum.sumQueues, sumQueue)
	}
	slog.Info("sum: Distribuidor preparado", "sum_id", sum.id, "queue", config.InputQueue, "destinations", len(sum.sumQueues))
	return nil
}

/*
 * Reparte mensajes completos por turnos y envia cada EOF a todas las colas de trabajo
 */
func (sum *Sum) runDispatcher(ctx context.Context) error {
	nextSum := 0
	return consumeMessages(ctx, sum.dispatchQueue, func(msg middleware.Message) error {
		message, err := inner.DeserializeMessage(&msg)
		if err != nil {
			return fmt.Errorf("deserializar mensaje para distribuir: %w", err)
		}
		if message.EOF {
			for sumID, sumQueue := range sum.sumQueues {
				if err := sumQueue.Send(msg); err != nil {
					return fmt.Errorf("distribuir EOF del cliente %d a Sum %d: %w", message.ClientID, sumID, err)
				}
				slog.Info("sum: EOF distribuido", "client_id", message.ClientID, "sum_id", sumID)
			}
			return nil
		}

		if err := sum.sumQueues[nextSum].Send(msg); err != nil {
			return fmt.Errorf("distribuir datos del cliente %d a Sum %d: %w", message.ClientID, nextSum, err)
		}
		nextSum = (nextSum + 1) % len(sum.sumQueues)
		return nil
	})
}
