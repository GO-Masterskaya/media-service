package events

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKafkaSecurity_Validate(t *testing.T) {
	tests := []struct {
		name    string
		sec     KafkaSecurity
		wantErr string
	}{
		{
			name: "пустая конфигурация допустима: локальный брокер без авторизации",
			sec:  KafkaSecurity{},
		},
		{
			name: "TLS без кредов допустим: шифрование есть, авторизации нет",
			sec:  KafkaSecurity{TLS: true},
		},
		{
			name: "полный набор",
			sec:  KafkaSecurity{Username: "u", Password: "p", TLS: true},
		},
		{
			name:    "username без password",
			sec:     KafkaSecurity{Username: "u", TLS: true},
			wantErr: "must be set together",
		},
		{
			name:    "password без username",
			sec:     KafkaSecurity{Password: "p", TLS: true},
			wantErr: "must be set together",
		},
		{
			name:    "креды без TLS уходят по открытому каналу",
			sec:     KafkaSecurity{Username: "u", Password: "p"},
			wantErr: "TLS is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.sec.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestKafkaSecurity_ClientOpts проверяет только количество опций: kgo.Opt —
// непрозрачный интерфейс с неэкспортированным методом apply, заглянуть
// внутрь невозможно. Тест ловит главную регрессию — молча потерянную опцию.
func TestKafkaSecurity_ClientOpts(t *testing.T) {
	require.Empty(t, KafkaSecurity{}.clientOpts(),
		"пустая конфигурация не должна добавлять опций")
	require.Len(t, KafkaSecurity{TLS: true}.clientOpts(), 1,
		"только TLS")
	require.Len(t, KafkaSecurity{Username: "u", Password: "p", TLS: true}.clientOpts(), 2,
		"TLS и SASL")
}

func TestKafkaSecurity_StringHidesPassword(t *testing.T) {
	s := KafkaSecurity{Username: "media", Password: "s3cr3t", TLS: true}.String()

	require.NotContains(t, s, "s3cr3t")
	require.NotContains(t, s, "media")
	require.Contains(t, s, "SASL:true")
	require.Contains(t, s, "TLS:true")
	require.False(t, strings.Contains(s, "Password"))
}
