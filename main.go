package main

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	_ "github.com/joho/godotenv/autoload"
)

func main() {
	topic := "alif_test_nifi_2"
	amount := 8

	// Create Kafka producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_HOST"),
		"sasl.mechanisms":   os.Getenv("KAFKA_SASL_MECHANISM"),
		"security.protocol": os.Getenv("KAFKA_PROTOCOL"),
		"sasl.username":     os.Getenv("KAFKA_USERNAME"),
		"sasl.password":     os.Getenv("KAFKA_PASSWORD"),
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to create producer: %s", err))
	}
	defer producer.Close()

	var wg sync.WaitGroup
	wg.Add(amount)

	// Goroutine to listen to delivery reports
	msgCount := 0
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("❌ Failed to deliver: %v\n", ev.TopicPartition.Error)
				} else {
					msgCount++
				}
				wg.Done()
			}
		}
	}()

	tenantSchemas := []string{"tw6ppon7rtda54ic1i3crjw", "tw6ppon7rtda54ic1i3coba", "tw6ppon7rtda54ic1i3coba2", "tw6ppon7rtda54ic1i3coba3", "tw6ppon7rtda54ic1i3coba4", "hh3h3", "hshshs", "hshsh"}

	// Produce messages
	for i := 0; i < amount; i++ {
		// schema := tenantSchemas[i%2]
		schema := tenantSchemas[i]
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(SetData(6+i, schema)),
		}, nil)

		// Optional pacing
		if i%100 == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Wait for all deliveries to complete
	wg.Wait()
	fmt.Printf("✅ Delivered %d messages at %s\n", msgCount, time.Now())
	producer.Flush(5000)
	producer.Close()
}

func SetData(itemMovementId int, tenantSchema string) string {
	return `{
					"op": "c",
					"after": {
						"qty": "2PA=",
						"amount": "+oq3Mg==",
						"bin_id": 5,
						"ref_no": "LZ-1299783616172957-44578",
						"trx_id": 39018,
						"trx_no": "INV-000086551",
						"item_id": 388,
						"store_id": 44578,
						"order_qty": "AA==",
						"archive_id": null,
						"location_id": 2,
						"created_date": "2025-03-19T08:04:25.766325Z",
						"last_modified": "2025-03-19T08:04:25.766325Z",
						"bill_detail_id": null,
						"item_movement_id": ` + strconv.Itoa(itemMovementId) + `,
						"transaction_date": "2025-03-19T08:04:25.766325Z",
						"invoice_detail_id": 62585,
						"item_adj_detail_id": null,
						"purch_ret_detail_id": null,
						"sales_analytic_date": "2025-03-19T08:04:26Z",
						"sales_ret_detail_id": null,
						"channel_promotion_id": "0",
						"putaway_reference_id": null,
						"salesorder_detail_id": null,
						"serial_analytic_date": null,
						"inventory_analytic_date": "2025-03-19T08:04:26Z",
						"item_transfer_detail_id": null,
						"invoice_detail_bundle_id": null,
						"salesorder_transaction_date": "2025-03-19T08:04:25.766325Z"
					},
					"ts_ms": 1742371466258,
					"ts_ns": 1742371466258038500,
					"ts_us": 1742371466258038,
					"before": null,
					"source": {
						"db": "jb_tenant",
						"lsn": 19876615940512,
						"name": "db200",
						"txId": 39595175,
						"xmin": null,
						"table": "item_movement",
						"ts_ms": 1742371465768,
						"ts_ns": 1742371465768052000,
						"ts_us": 1742371465768052,
						"schema": "` + tenantSchema + `",
						"version": "3.0.7.Final",
						"sequence": "[\"19876590629600\",\"19876615940512\"]",
						"snapshot": "false",
						"connector": "postgresql"
					},
					"hostname": "db200.jubelio.com",
					"transaction": null
				}
	`
}
