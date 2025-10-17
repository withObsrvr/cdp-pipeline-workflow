package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// BenchmarkDuckLake_Insert benchmarks INSERT performance
func BenchmarkDuckLake_Insert(b *testing.B) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "ducklake_bench_insert_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	catalogPath := filepath.Join(tmpDir, "bench.ducklake")
	dataPath := filepath.Join(tmpDir, "data")
	os.MkdirAll(dataPath, 0755)

	// Create consumer WITHOUT upsert (pure INSERT)
	proc, err := NewSaveToDuckLake(map[string]interface{}{
		"catalog_path":   fmt.Sprintf("ducklake:%s", catalogPath),
		"catalog_name":   "stellar_lake",
		"data_path":      dataPath,
		"schema_name":    "main",
		"use_upsert":     false, // Pure INSERT
		"create_indexes": false, // No indexes for pure insert performance
		"batch_size":     100,
	})
	if err != nil {
		b.Fatal(err)
	}
	consumer := proc.(*SaveToDuckLake)
	defer consumer.Close()

	ctx := context.Background()

	// Create test invocation
	invocation := &processor.ContractInvocation{
		Timestamp:       time.Now(),
		LedgerSequence:  0, // Will be set in loop
		TransactionHash: "",
		ContractID:      "CCBENCH",
		InvokingAccount: "GABC123",
		FunctionName:    "transfer",
		Successful:      true,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		invocation.LedgerSequence = uint32(i)
		invocation.TransactionHash = fmt.Sprintf("tx_%d", i)

		jsonBytes, _ := json.Marshal(invocation)
		msg := processor.Message{
			Payload: jsonBytes,
			Metadata: map[string]interface{}{
				"processor_type":  string(processor.ProcessorTypeContractInvocation),
				"ledger_sequence": invocation.LedgerSequence,
			},
		}

		if err := consumer.Process(ctx, msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDuckLake_Upsert benchmarks MERGE/UPSERT performance
func BenchmarkDuckLake_Upsert(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "ducklake_bench_upsert_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	catalogPath := filepath.Join(tmpDir, "bench.ducklake")
	dataPath := filepath.Join(tmpDir, "data")
	os.MkdirAll(dataPath, 0755)

	// Create consumer WITH upsert (MERGE)
	proc, err := NewSaveToDuckLake(map[string]interface{}{
		"catalog_path":   fmt.Sprintf("ducklake:%s", catalogPath),
		"catalog_name":   "stellar_lake",
		"data_path":      dataPath,
		"schema_name":    "main",
		"use_upsert":     true, // MERGE/UPSERT
		"create_indexes": false,
		"batch_size":     100,
	})
	if err != nil {
		b.Fatal(err)
	}
	consumer := proc.(*SaveToDuckLake)
	defer consumer.Close()

	ctx := context.Background()

	invocation := &processor.ContractInvocation{
		Timestamp:       time.Now(),
		LedgerSequence:  0,
		TransactionHash: "",
		ContractID:      "CCBENCH",
		InvokingAccount: "GABC123",
		FunctionName:    "transfer",
		Successful:      true,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		invocation.LedgerSequence = uint32(i)
		invocation.TransactionHash = fmt.Sprintf("tx_%d", i)

		jsonBytes, _ := json.Marshal(invocation)
		msg := processor.Message{
			Payload: jsonBytes,
			Metadata: map[string]interface{}{
				"processor_type":  string(processor.ProcessorTypeContractInvocation),
				"ledger_sequence": invocation.LedgerSequence,
			},
		}

		if err := consumer.Process(ctx, msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDuckLake_UpsertDuplicates benchmarks UPSERT with duplicate keys
func BenchmarkDuckLake_UpsertDuplicates(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "ducklake_bench_upsert_dup_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	catalogPath := filepath.Join(tmpDir, "bench.ducklake")
	dataPath := filepath.Join(tmpDir, "data")
	os.MkdirAll(dataPath, 0755)

	proc, err := NewSaveToDuckLake(map[string]interface{}{
		"catalog_path":   fmt.Sprintf("ducklake:%s", catalogPath),
		"catalog_name":   "stellar_lake",
		"data_path":      dataPath,
		"schema_name":    "main",
		"use_upsert":     true,
		"create_indexes": false,
		"batch_size":     100,
	})
	if err != nil {
		b.Fatal(err)
	}
	consumer := proc.(*SaveToDuckLake)
	defer consumer.Close()

	ctx := context.Background()

	// Pre-insert 1000 records
	for i := 0; i < 1000; i++ {
		invocation := &processor.ContractInvocation{
			Timestamp:       time.Now(),
			LedgerSequence:  uint32(i),
			TransactionHash: fmt.Sprintf("tx_%d", i),
			ContractID:      "CCBENCH",
			InvokingAccount: "GABC123",
			FunctionName:    "transfer",
			Successful:      true,
		}

		jsonBytes, _ := json.Marshal(invocation)
		msg := processor.Message{
			Payload: jsonBytes,
			Metadata: map[string]interface{}{
				"processor_type":  string(processor.ProcessorTypeContractInvocation),
				"ledger_sequence": invocation.LedgerSequence,
			},
		}
		consumer.Process(ctx, msg)
	}

	// Benchmark updating existing records
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Update existing record (modulo 1000)
		idx := i % 1000
		invocation := &processor.ContractInvocation{
			Timestamp:       time.Now(),
			LedgerSequence:  uint32(idx),
			TransactionHash: fmt.Sprintf("tx_%d", idx),
			ContractID:      "CCBENCH_UPDATED",
			InvokingAccount: "GABC456",
			FunctionName:    "transfer_updated",
			Successful:      true,
		}

		jsonBytes, _ := json.Marshal(invocation)
		msg := processor.Message{
			Payload: jsonBytes,
			Metadata: map[string]interface{}{
				"processor_type":  string(processor.ProcessorTypeContractInvocation),
				"ledger_sequence": invocation.LedgerSequence,
			},
		}

		if err := consumer.Process(ctx, msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDuckLake_WithIndexes benchmarks performance with index creation
func BenchmarkDuckLake_WithIndexes(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "ducklake_bench_indexes_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	catalogPath := filepath.Join(tmpDir, "bench.ducklake")
	dataPath := filepath.Join(tmpDir, "data")
	os.MkdirAll(dataPath, 0755)

	proc, err := NewSaveToDuckLake(map[string]interface{}{
		"catalog_path":   fmt.Sprintf("ducklake:%s", catalogPath),
		"catalog_name":   "stellar_lake",
		"data_path":      dataPath,
		"schema_name":    "main",
		"use_upsert":     true,
		"create_indexes": true, // With indexes
		"batch_size":     100,
	})
	if err != nil {
		b.Fatal(err)
	}
	consumer := proc.(*SaveToDuckLake)
	defer consumer.Close()

	ctx := context.Background()

	invocation := &processor.ContractInvocation{
		Timestamp:       time.Now(),
		LedgerSequence:  0,
		TransactionHash: "",
		ContractID:      "CCBENCH",
		InvokingAccount: "GABC123",
		FunctionName:    "transfer",
		Successful:      true,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		invocation.LedgerSequence = uint32(i)
		invocation.TransactionHash = fmt.Sprintf("tx_%d", i)

		jsonBytes, _ := json.Marshal(invocation)
		msg := processor.Message{
			Payload: jsonBytes,
			Metadata: map[string]interface{}{
				"processor_type":  string(processor.ProcessorTypeContractInvocation),
				"ledger_sequence": invocation.LedgerSequence,
			},
		}

		if err := consumer.Process(ctx, msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDuckLake_MetadataOverhead benchmarks metadata processing overhead
func BenchmarkDuckLake_MetadataOverhead(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "ducklake_bench_metadata_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	catalogPath := filepath.Join(tmpDir, "bench.ducklake")
	dataPath := filepath.Join(tmpDir, "data")
	os.MkdirAll(dataPath, 0755)

	proc, err := NewSaveToDuckLake(map[string]interface{}{
		"catalog_path":   fmt.Sprintf("ducklake:%s", catalogPath),
		"catalog_name":   "stellar_lake",
		"data_path":      dataPath,
		"schema_name":    "main",
		"use_upsert":     false,
		"create_indexes": false,
		"batch_size":     100,
	})
	if err != nil {
		b.Fatal(err)
	}
	consumer := proc.(processor.Processor)
	defer proc.(*SaveToDuckLake).Close()

	ctx := context.Background()

	invocation := &processor.ContractInvocation{
		Timestamp:       time.Now(),
		LedgerSequence:  0,
		TransactionHash: "",
		ContractID:      "CCBENCH",
		InvokingAccount: "GABC123",
		FunctionName:    "transfer",
		Successful:      true,
	}

	b.Run("WithMetadata", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			invocation.LedgerSequence = uint32(i)
			invocation.TransactionHash = fmt.Sprintf("tx_%d", i)

			jsonBytes, _ := json.Marshal(invocation)
			msg := processor.Message{
				Payload: jsonBytes,
				Metadata: map[string]interface{}{
					"processor_type":  string(processor.ProcessorTypeContractInvocation),
					"processor_name":  "ContractInvocationProcessor",
					"version":         "1.0.0",
					"timestamp":       time.Now(),
					"ledger_sequence": invocation.LedgerSequence,
				},
			}
			consumer.Process(ctx, msg)
		}
	})

	b.Run("WithoutMetadata", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			invocation.LedgerSequence = uint32(i + 10000)
			invocation.TransactionHash = fmt.Sprintf("tx_no_meta_%d", i)

			jsonBytes, _ := json.Marshal(invocation)
			msg := processor.Message{
				Payload:  jsonBytes,
				Metadata: nil, // No metadata
			}
			consumer.Process(ctx, msg)
		}
	})
}

// BenchmarkDuckLake_BatchSizes benchmarks different batch sizes
func BenchmarkDuckLake_BatchSizes(b *testing.B) {
	batchSizes := []int{10, 50, 100, 500, 1000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			tmpDir, err := os.MkdirTemp("", fmt.Sprintf("ducklake_bench_batch_%d_*", batchSize))
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(tmpDir)

			catalogPath := filepath.Join(tmpDir, "bench.ducklake")
			dataPath := filepath.Join(tmpDir, "data")
			os.MkdirAll(dataPath, 0755)

			proc, err := NewSaveToDuckLake(map[string]interface{}{
				"catalog_path":   fmt.Sprintf("ducklake:%s", catalogPath),
				"catalog_name":   "stellar_lake",
				"data_path":      dataPath,
				"schema_name":    "main",
				"use_upsert":     false,
				"create_indexes": false,
				"batch_size":     batchSize,
			})
			if err != nil {
				b.Fatal(err)
			}
			consumer := proc.(*SaveToDuckLake)
			defer consumer.Close()

			ctx := context.Background()

			invocation := &processor.ContractInvocation{
				Timestamp:       time.Now(),
				LedgerSequence:  0,
				TransactionHash: "",
				ContractID:      "CCBENCH",
				InvokingAccount: "GABC123",
				FunctionName:    "transfer",
				Successful:      true,
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				invocation.LedgerSequence = uint32(i)
				invocation.TransactionHash = fmt.Sprintf("tx_%d", i)

				jsonBytes, _ := json.Marshal(invocation)
				msg := processor.Message{
					Payload: jsonBytes,
					Metadata: map[string]interface{}{
						"processor_type":  string(processor.ProcessorTypeContractInvocation),
						"ledger_sequence": invocation.LedgerSequence,
					},
				}

				if err := consumer.Process(ctx, msg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
