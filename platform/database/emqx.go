package database

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const mqttConnectTimeout = 5 * time.Second

// MQTTPool is the EMQX client used by hana and oneauxilia.
// Publish borrows a connection from the pool. Subscribe stays on a dedicated connection.
type MQTTPool struct {
	broker     string
	username   string
	password   string
	clients    chan mqtt.Client
	subscriber mqtt.Client

	mu     sync.Mutex
	closed bool
	once   sync.Once
}

// OpenMQTT dials poolSize connections. raw is mqtt://user:pass@host:1883 or tcp://host:1883.
func OpenMQTT(raw string, poolSize int) (*MQTTPool, error) {
	broker, username, password, err := parseMQTT(raw)
	if err != nil {
		return nil, err
	}
	if poolSize <= 0 {
		poolSize = 4
	}
	pool := &MQTTPool{
		broker:   broker,
		username: username,
		password: password,
		clients:  make(chan mqtt.Client, poolSize),
	}
	subscriber, err := pool.dial()
	if err != nil {
		return nil, err
	}
	pool.subscriber = subscriber
	for i := 0; i < poolSize; i++ {
		client, dialErr := pool.dial()
		if dialErr != nil {
			pool.Close()
			return nil, dialErr
		}
		pool.clients <- client
	}
	return pool, nil
}

func (p *MQTTPool) Publish(ctx context.Context, topic string, qos byte, retained bool, payload []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	client, err := p.Get()
	if err != nil {
		return err
	}
	defer p.Release(client)

	token := client.Publish(topic, qos, retained, payload)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-token.Done():
		if err := token.Error(); err != nil {
			return errors.Wrap(err, "mqtt publish")
		}
		return nil
	}
}

func (p *MQTTPool) Subscribe(topic string, qos byte, handler mqtt.MessageHandler) error {
	if p == nil || p.subscriber == nil {
		return errors.New("mqtt is not connected")
	}
	token := p.subscriber.Subscribe(topic, qos, handler)
	if !token.WaitTimeout(mqttConnectTimeout) {
		return errors.New("mqtt subscribe timed out")
	}
	if err := token.Error(); err != nil {
		return errors.Wrap(err, "mqtt subscribe")
	}
	return nil
}

func (p *MQTTPool) Unsubscribe(topics ...string) error {
	if p == nil || p.subscriber == nil {
		return errors.New("mqtt is not connected")
	}
	token := p.subscriber.Unsubscribe(topics...)
	if !token.WaitTimeout(mqttConnectTimeout) {
		return errors.New("mqtt unsubscribe timed out")
	}
	return token.Error()
}

func (p *MQTTPool) Ping(context.Context) error {
	if p == nil || p.subscriber == nil || !p.subscriber.IsConnected() {
		return errors.New("mqtt is not connected")
	}
	return nil
}

func (p *MQTTPool) Get() (mqtt.Client, error) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return nil, errors.New("mqtt pool is closed")
	}
	select {
	case client := <-p.clients:
		if client != nil && client.IsConnected() {
			return client, nil
		}
		if client != nil {
			client.Disconnect(250)
		}
		return p.dial()
	default:
		return p.dial()
	}
}

func (p *MQTTPool) Release(client mqtt.Client) {
	if client == nil {
		return
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed || !client.IsConnected() {
		client.Disconnect(250)
		return
	}
	select {
	case p.clients <- client:
	default:
		client.Disconnect(250)
	}
}

func (p *MQTTPool) Close() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		if p.subscriber != nil {
			p.subscriber.Disconnect(250)
		}
		close(p.clients)
		for client := range p.clients {
			if client != nil {
				client.Disconnect(250)
			}
		}
	})
}

func (p *MQTTPool) dial() (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(p.broker).
		SetClientID(uuid.NewString()).
		SetUsername(p.username).
		SetPassword(p.password).
		SetConnectTimeout(mqttConnectTimeout).
		SetAutoReconnect(true).
		SetConnectRetry(false).
		SetKeepAlive(30 * time.Second).
		SetCleanSession(true)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(mqttConnectTimeout) {
		client.Disconnect(250)
		return nil, fmt.Errorf("mqtt connect timed out: %s", p.broker)
	}
	if err := token.Error(); err != nil {
		client.Disconnect(250)
		return nil, errors.Wrap(err, "mqtt connect")
	}
	return client, nil
}

func parseMQTT(raw string) (broker, username, password string, err error) {
	if raw == "" {
		return "", "", "", errors.New("mqtt url is required")
	}
	parsed, parseErr := url.Parse(raw)
	if parseErr != nil {
		return "", "", "", errors.Wrap(parseErr, "parse mqtt url")
	}
	if parsed.Host == "" {
		return "", "", "", errors.New("mqtt url is missing a host")
	}
	switch parsed.Scheme {
	case "mqtt":
		parsed.Scheme = "tcp"
	case "mqtts":
		parsed.Scheme = "ssl"
	case "tcp", "ssl", "ws", "wss":
	default:
		return "", "", "", fmt.Errorf("unsupported mqtt scheme %q", parsed.Scheme)
	}
	if parsed.User != nil {
		username = parsed.User.Username()
		password, _ = parsed.User.Password()
		parsed.User = nil
	}
	return parsed.String(), username, password, nil
}
