package base

const (
	ExchangeKindTopic  = "topic"
	ExchangeKindDirect = "direct"
	ExchangeDurable    = true
	ExchangeAutoDelete = false
	ExchangeInternal   = false
	ExchangeNoWait     = false

	QueueDurable    = true
	QueueAutoDelete = false
	QueueExclusive  = false
	QueueNoWait     = false

	PublishMandatory = false
	PublishImmediate = false

	PrefetchCount  = 1
	PrefetchSize   = 0
	PrefetchGlobal = false

	ConsumeAutoAck   = false
	ConsumeExclusive = false
	ConsumeNoLocal   = false
	ConsumeNoWait    = false

	JWT_ACCESS_TOKEN  int = 1
	JWT_REFRESH_TOKEN int = 2

	ErrDuplicate   = "duplicate key"
	ErrLtreeSyntax = "ltree syntax error"

	RetryCountHeader = "x-retry-count"
)
