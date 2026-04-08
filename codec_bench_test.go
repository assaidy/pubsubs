package pubsub

import (
	"testing"
	"time"
)

type benchMessage struct {
	ID        string
	Type      string
	Count     int
	Amount    float64
	Active    bool
	Tags      []string
	Metadata  map[string]any
	CreatedAt time.Time
	User      benchUser
	Scores    []int
}

type benchUser struct {
	ID    string
	Name  string
	Roles []string
}

func newBenchMessage() benchMessage {
	return benchMessage{
		ID:        "msg-12345",
		Type:      "user_created",
		Count:     42,
		Amount:    99.99,
		Active:    true,
		Tags:      []string{"important", "urgent"},
		Metadata:  map[string]any{"source": "api", "version": 2},
		CreatedAt: time.Now(),
		User: benchUser{
			ID:    "user-67890",
			Name:  "John Doe",
			Roles: []string{"admin", "editor"},
		},
		Scores: []int{10, 20, 30, 40, 50},
	}
}

func BenchmarkCodec_Encode_Json(b *testing.B) {
	msg := newBenchMessage()

	for b.Loop() {
		CodecJson.Encode(msg)
	}
}

func BenchmarkCodec_Encode_MessagePack(b *testing.B) {
	msg := newBenchMessage()

	for b.Loop() {
		CodecMessagePack.Encode(msg)
	}
}

func BenchmarkCodec_Decode_Json(b *testing.B) {
	data, err := CodecJson.Encode(newBenchMessage())
	if err != nil {
		b.Fatal(err)
	}

	var msg benchMessage
	for b.Loop() {
		CodecJson.Decode(data, &msg)
	}
}

func BenchmarkCodec_Decode_MessagePack(b *testing.B) {
	data, err := CodecMessagePack.Encode(newBenchMessage())
	if err != nil {
		b.Fatal(err)
	}

	var msg benchMessage
	for b.Loop() {
		CodecMessagePack.Decode(data, &msg)
	}
}
