package rabbitmq

import (
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"

	"go-init/internal/base"
)

func ensureTopology(ch *amqp.Channel, opts ConsumeOptions) error {
	if opts.Queue == "" {
		return errors.New("queue is required")
	}

	if err := ensureDeadLetterFromArgs(ch, opts.Args); err != nil {
		return err
	}

	if opts.Exchange != "" {
		kind := opts.ExchangeKind
		if kind == "" {
			kind = base.ExchangeKindDirect
		}
		if err := ch.ExchangeDeclare(
			opts.Exchange, kind,
			base.ExchangeDurable, base.ExchangeAutoDelete, base.ExchangeInternal, base.ExchangeNoWait,
			nil,
		); err != nil {
			return errors.Wrap(err, "exchange declare")
		}
	}

	queue, err := ch.QueueDeclare(
		opts.Queue,
		base.QueueDurable, base.QueueAutoDelete, base.QueueExclusive, base.QueueNoWait,
		opts.Args,
	)
	if err != nil {
		return errors.Wrap(err, "queue declare")
	}

	if opts.Exchange != "" {
		bindingKey := opts.BindingKey
		if bindingKey == "" {
			bindingKey = opts.Queue
		}
		if err := ch.QueueBind(queue.Name, bindingKey, opts.Exchange, base.QueueNoWait, nil); err != nil {
			return errors.Wrap(err, "queue bind")
		}
	}

	// Retry queue has no consumer. Messages sit until per-message TTL expires,
	// then are dead-lettered back to opts.Queue via the default exchange.
	_, err = ch.QueueDeclare(
		retryQueueName(opts.Queue),
		base.QueueDurable, base.QueueAutoDelete, base.QueueExclusive, base.QueueNoWait,
		amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": opts.Queue,
		},
	)
	if err != nil {
		return errors.Wrap(err, "retry queue declare")
	}

	return nil
}

func ensureDeadLetterFromArgs(ch *amqp.Channel, args amqp.Table) error {
	if args == nil {
		return nil
	}
	dlx, _ := args["x-dead-letter-exchange"].(string)
	dlqKey, _ := args["x-dead-letter-routing-key"].(string)
	if dlx == "" {
		return nil
	}
	if dlqKey == "" {
		dlqKey = dlx
	}

	if err := ch.ExchangeDeclare(
		dlx,
		base.ExchangeKindDirect,
		base.ExchangeDurable,
		base.ExchangeAutoDelete,
		base.ExchangeInternal,
		base.ExchangeNoWait,
		nil,
	); err != nil {
		return errors.Wrap(err, "dlx declare")
	}
	if _, err := ch.QueueDeclare(
		dlqKey,
		base.QueueDurable,
		base.QueueAutoDelete,
		base.QueueExclusive,
		base.QueueNoWait,
		nil,
	); err != nil {
		return errors.Wrap(err, "dlq declare")
	}
	if err := ch.QueueBind(dlqKey, dlqKey, dlx, base.QueueNoWait, nil); err != nil {
		return errors.Wrap(err, "dlq bind")
	}
	return nil
}

func setupConsumeTopology(ch *amqp.Channel, opts ConsumeOptions) error {
	if err := ensureTopology(ch, opts); err != nil {
		return err
	}
	if err := ch.Qos(opts.Prefetch, base.PrefetchSize, base.PrefetchGlobal); err != nil {
		return errors.Wrap(err, "qos")
	}
	return nil
}

func retryQueueName(queue string) string {
	return queue + ".retry"
}
