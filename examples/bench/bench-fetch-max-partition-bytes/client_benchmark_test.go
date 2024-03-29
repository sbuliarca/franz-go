package benchmark_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"crypto/rand"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"io"
	"log"
)

const (
	totalEntries = 500000
)

func BenchmarkPollRecords(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	b.Cleanup(cancel)

	testcontainers.Logger = log.New(io.Discard, "", log.LstdFlags)

	ctnr, err := redpanda.RunContainer(ctx, redpanda.WithAutoCreateTopics())
	require.NoError(b, err)

	broker, err := ctnr.KafkaSeedBroker(ctx)
	require.NoError(b, err)

	b.Cleanup(func() {
		if err := ctnr.Terminate(context.Background()); err != nil {
			b.Logf("failed terminating container: %v", err)
		}
	})

	topicName := newRandomName("test-topic")
	require.NoError(b, populateTopic(ctx, b, broker, topicName))

	b.ResetTimer()
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(newRandomName("test-consumer")),
		kgo.ConsumeTopics(topicName),
		kgo.BlockRebalanceOnPoll(),
		//	use 10MB
		//kgo.FetchMaxPartitionBytes(10*1024*1024),
	)

	require.NoError(b, err)
	defer consumer.CloseAllowingRebalance()

	consumedTotal := 0

	for i := 0; i < b.N; i++ {
		fetches := consumer.PollRecords(ctx, 1000)
		require.NoError(b, fetches.Err0())
		consumedTotal += len(fetches.Records())
		if consumedTotal >= totalEntries {
			return
		}
	}
}

func populateTopic(ctx context.Context, t *testing.B, broker string, topicName string) error {
	t.Log("Start topic population")
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.DefaultProduceTopic(topicName),
	)
	require.NoError(t, err)
	defer producer.Close()

	adm := kadm.NewClient(producer)
	partitions := 10
	tr, err := adm.CreateTopic(ctx, int32(partitions), 1, nil, topicName)
	require.NoError(t, tr.Err)
	require.NoError(t, err)

	data := make([]byte, 10240) // 10KB per message
	_, err = rand.Read(data)
	require.NoError(t, err)

	batchLen := 5000
	for it := 0; it < totalEntries/batchLen; it++ {
		recs := make([]*kgo.Record, batchLen)
		for i := 0; i < batchLen; i++ {
			recs[i] = &kgo.Record{
				Value: data,
				Key:   []byte(fmt.Sprintf("key-%d", i%partitions)),
			}
		}
		require.NoError(t, producer.ProduceSync(ctx, recs...).FirstErr())
	}
	return err
}

func newRandomName(baseName string) string {
	return baseName + "-" + uuid.NewString()
}
