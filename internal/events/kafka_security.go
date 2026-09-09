package events

import (
	"crypto/tls"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// KafkaSecurity — транспортная безопасность и авторизация Kafka-клиентов.
// Одна структура на оба клиента сервиса: consumer и DLQ-producer. Если
// параметры задавать в каждом конструкторе отдельно, рано или поздно один
// из клиентов останется в PLAINTEXT — именно это и произошло в main.
//
// Пустая структура — валидная конфигурация: локальный compose поднимает
// брокер без TLS и без SASL.
type KafkaSecurity struct {
	// Username/Password — креды SASL/SCRAM-SHA-256. Пустые = SASL выключен.
	Username string
	Password string
	// TLS включает шифрование соединения с брокером.
	TLS bool
}

// Validate проверяет, что комбинация параметров не создаёт дыру в безопасности.
//
// Два правила:
//  1. Креды принимаются только парой. Один заданный параметр из двух — это
//     всегда опечатка в деплое, а не осознанный анонимный доступ. Молча
//     проигнорировать её означает уйти в PLAINTEXT без авторизации.
//  2. SASL/SCRAM передаёт пароль по схеме challenge-response, но сам трафик
//     остаётся открытым: без TLS креды и все события видны на любом узле
//     между сервисом и брокером. Поэтому креды без TLS запрещены.
func (s KafkaSecurity) Validate() error {
	if (s.Username == "") != (s.Password == "") {
		return fmt.Errorf("kafka username and password must be set together")
	}
	if s.Username != "" && !s.TLS {
		return fmt.Errorf("kafka TLS is required when SASL credentials are set")
	}
	return nil
}

// clientOpts возвращает опции kgo, общие для consumer и producer.
// Порядок опций не важен: kgo применяет их последовательно к одному cfg.
func (s KafkaSecurity) clientOpts() []kgo.Opt {
	opts := make([]kgo.Opt, 0, 2)
	if s.TLS {
		// MinVersion обязателен явно: нулевой tls.Config разрешает TLS 1.0.
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	if s.Username != "" {
		opts = append(opts, kgo.SASL(scram.Auth{
			User: s.Username,
			Pass: s.Password,
		}.AsSha256Mechanism()))
	}
	return opts
}

// String не даёт паролю утечь в лог при форматировании через %v/%s.
func (s KafkaSecurity) String() string {
	return fmt.Sprintf("KafkaSecurity{TLS:%v, SASL:%v}", s.TLS, s.Username != "")
}
