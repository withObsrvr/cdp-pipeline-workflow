package v2_test

import (
	"testing"
	
	configtesting "github.com/withObsrvr/cdp-pipeline-workflow/internal/config/testing"
	"github.com/withObsrvr/cdp-pipeline-workflow/internal/config/v2"
)

func TestConfigEquivalence(t *testing.T) {
	checker, err := configtesting.NewConfigEquivalenceChecker()
	if err != nil {
		t.Fatalf("Failed to create equivalence checker: %v", err)
	}
	
	tests := []struct {
		name       string
		legacy     map[string]interface{}
		v2         map[string]interface{}
		shouldPass bool
	}{
		{
			name: "Simple payment pipeline",
			legacy: map[string]interface{}{
				"pipelines": map[string]interface{}{
					"PaymentPipeline": map[string]interface{}{
						"source": map[string]interface{}{
							"type": "BufferedStorageSourceAdapter",
							"config": map[string]interface{}{
								"bucket_name": "stellar-data",
								"network":     "mainnet",
								"num_workers": 20,
							},
						},
						"processors": []interface{}{
							map[string]interface{}{
								"type": "FilterPayments",
								"config": map[string]interface{}{
									"min_amount": "100",
								},
							},
						},
						"consumers": []interface{}{
							map[string]interface{}{
								"type": "SaveToParquet",
								"config": map[string]interface{}{
									"path":        "output/payments",
									"compression": "snappy",
								},
							},
						},
					},
				},
			},
			v2: map[string]interface{}{
				"source": map[string]interface{}{
					"bucket":  "stellar-data",
					"network": "mainnet",
				},
				"process": []interface{}{
					map[string]interface{}{
						"payment_filter": map[string]interface{}{
							"min": "100",
						},
					},
				},
				"save_to": []interface{}{
					map[string]interface{}{
						"parquet": map[string]interface{}{
							"path": "output/payments",
						},
					},
				},
			},
			shouldPass: true,
		},
		{
			name: "Contract data pipeline",
			legacy: map[string]interface{}{
				"pipelines": map[string]interface{}{
					"ContractPipeline": map[string]interface{}{
						"source": map[string]interface{}{
							"type": "CaptiveCoreInboundAdapter",
							"config": map[string]interface{}{
								"network":                   "testnet",
								"network_passphrase":        "Test SDF Network ; September 2015",
								"history_archive_urls":      []string{"https://history.stellar.org/prd/core-testnet-001"},
								"binary_path":               "/usr/local/bin/stellar-core",
								"start_ledger":              1000000,
							},
						},
						"processors": []interface{}{
							map[string]interface{}{
								"type": "ContractData",
								"config": map[string]interface{}{
									"network_passphrase": "Test SDF Network ; September 2015",
								},
							},
						},
						"consumers": []interface{}{
							map[string]interface{}{
								"type": "SaveToPostgreSQL",
								"config": map[string]interface{}{
									"connection_string": "postgresql://localhost/stellar",
									"table_name":        "contracts",
									"batch_size":        1000,
								},
							},
						},
					},
				},
			},
			v2: map[string]interface{}{
				"source": map[string]interface{}{
					"stellar": "testnet",
					"ledgers": "1000000",
				},
				"process": "contract_data",
				"save_to": map[string]interface{}{
					"postgres": map[string]interface{}{
						"connection": "postgresql://localhost/stellar",
						"table":      "contracts",
					},
				},
			},
			shouldPass: true,
		},
		{
			name: "Multiple processors",
			legacy: map[string]interface{}{
				"pipelines": map[string]interface{}{
					"ComplexPipeline": map[string]interface{}{
						"source": map[string]interface{}{
							"type": "BufferedStorageSourceAdapter",
							"config": map[string]interface{}{
								"bucket_name": "data",
								"network":     "mainnet",
							},
						},
						"processors": []interface{}{
							map[string]interface{}{
								"type":   "FilterPayments",
								"config": map[string]interface{}{},
							},
							map[string]interface{}{
								"type":   "TransformToAppPayment",
								"config": map[string]interface{}{},
							},
						},
						"consumers": []interface{}{
							map[string]interface{}{
								"type": "SaveToRedis",
								"config": map[string]interface{}{
									"address": "localhost:6379",
								},
							},
						},
					},
				},
			},
			v2: map[string]interface{}{
				"source": map[string]interface{}{
					"bucket":  "data",
					"network": "mainnet",
				},
				"process": []interface{}{
					"payment_filter",
					"payment_transform",
				},
				"save_to": map[string]interface{}{
					"redis": map[string]interface{}{
						"address": "localhost:6379",
					},
				},
			},
			shouldPass: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := checker.CheckEquivalence(tt.legacy, tt.v2)
			if err != nil {
				t.Fatalf("Failed to check equivalence: %v", err)
			}
			
			if tt.shouldPass && !result.Equivalent {
				t.Errorf("Expected configurations to be equivalent, but they are not")
				t.Logf("Report:\n%s", result.GenerateReport())
			}
			
			if !tt.shouldPass && result.Equivalent {
				t.Errorf("Expected configurations to be different, but they are equivalent")
			}
			
			// Log non-critical differences
			if len(result.Differences) > 0 {
				t.Logf("Found %d differences:", len(result.Differences))
				for _, diff := range result.Differences {
					if !diff.IsCritical {
						t.Logf("  - %s: %v -> %v (non-critical)", diff.Path, diff.Legacy, diff.V2)
					}
				}
			}
		})
	}
}

func TestV2ConfigExamples(t *testing.T) {
	loader, err := v2.NewConfigLoader(v2.DefaultLoaderOptions())
	if err != nil {
		t.Fatalf("Failed to create config loader: %v", err)
	}
	
	examples := []struct {
		name   string
		config map[string]interface{}
	}{
		{
			name: "Minimal config",
			config: map[string]interface{}{
				"source":  "stellar://mainnet",
				"process": "latest_ledger",
				"save_to": "stdout",
			},
		},
		{
			name: "GCS to Parquet",
			config: map[string]interface{}{
				"source": map[string]interface{}{
					"bucket": "stellar-mainnet-data",
				},
				"process": "contract_data",
				"save_to": map[string]interface{}{
					"parquet": "gs://output-bucket/contracts",
				},
			},
		},
		{
			name: "Payment filtering with multiple outputs",
			config: map[string]interface{}{
				"source": map[string]interface{}{
					"stellar": "testnet",
					"ledgers": "1000000-2000000",
				},
				"process": []interface{}{
					map[string]interface{}{
						"payment_filter": map[string]interface{}{
							"min": 100,
							"max": 10000,
						},
					},
					"payment_transform",
				},
				"save_to": []interface{}{
					"postgres://payments",
					map[string]interface{}{
						"redis": map[string]interface{}{
							"prefix": "payment:",
							"ttl":    "1h",
						},
					},
				},
			},
		},
		{
			name: "Environment variables",
			config: map[string]interface{}{
				"source": map[string]interface{}{
					"bucket":  "${STELLAR_BUCKET}",
					"network": "${NETWORK:-testnet}",
				},
				"process": "contract_data",
				"save_to": "${DATABASE_URL}",
			},
		},
	}
	
	for _, ex := range examples {
		t.Run(ex.name, func(t *testing.T) {
			result, err := loader.LoadFromData(ex.config)
			if err != nil {
				t.Fatalf("Failed to load config: %v", err)
			}
			
			if result.Format != v2.FormatV2 {
				t.Errorf("Expected V2 format, got %v", result.Format)
			}
			
			// Verify successful transformation
			if len(result.Config.Pipelines) == 0 {
				t.Error("No pipelines generated")
			}
			
			// Log warnings if any
			for _, warning := range result.Warnings {
				t.Logf("Warning: %s", warning)
			}
		})
	}
}