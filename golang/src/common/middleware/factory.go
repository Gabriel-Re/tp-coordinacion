package middleware

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	return NewQueueMiddleware(queueName, connectionSettings)
}

func CreateExchangeMiddleware(name string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	return NewExchangeMiddleware(name, keys, connectionSettings)
}