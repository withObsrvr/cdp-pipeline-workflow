package testing

import "testing"

func TestCompareConfigMapsTreatsAliasesAsEquivalent(t *testing.T) {
	checker := &ConfigEquivalenceChecker{}

	diffs := checker.compareConfigMaps(
		map[string]interface{}{
			"path_prefix": "contracts",
			"table_name":  "contract_invocations",
		},
		map[string]interface{}{
			"path":  "contracts",
			"table": "contract_invocations",
		},
		"config",
	)

	if len(diffs) != 0 {
		t.Fatalf("expected aliased configs to be equivalent, got diffs: %#v", diffs)
	}
}

func TestCompareConfigMapsCoercesNumericStringsOnlyForNumericKeys(t *testing.T) {
	checker := &ConfigEquivalenceChecker{}

	t.Run("quoted number on a numeric key is equivalent", func(t *testing.T) {
		diffs := checker.compareConfigMaps(
			map[string]interface{}{"start_ledger": "1000", "batch_size": "500"},
			map[string]interface{}{"start_ledger": 1000, "batch_size": 500},
			"config",
		)
		if len(diffs) != 0 {
			t.Fatalf("expected quoted numerics to compare equal, got diffs: %#v", diffs)
		}
	})

	t.Run("digit string on a string key is a critical difference", func(t *testing.T) {
		// bucket_name is a string field; the loaders reject a numeric bucket, so
		// treating "123" and 123 as equivalent would bless a broken migration.
		diffs := checker.compareConfigMaps(
			map[string]interface{}{"bucket_name": "123"},
			map[string]interface{}{"bucket_name": 123},
			"config",
		)
		if len(diffs) != 1 || !diffs[0].IsCritical {
			t.Fatalf("expected one critical difference for bucket_name, got: %#v", diffs)
		}
	})
}

func TestCompareConfigMapsHistoryArchiveURLs(t *testing.T) {
	checker := &ConfigEquivalenceChecker{}

	t.Run("stock archives on both sides are non-critical", func(t *testing.T) {
		diffs := checker.compareConfigMaps(
			map[string]interface{}{"history_archive_urls": []string{
				"https://history.stellar.org/prd/core-testnet-001",
			}},
			map[string]interface{}{"history_archive_urls": []string{
				"https://history.stellar.org/prd/core-testnet-001",
				"https://history.stellar.org/prd/core-testnet-002",
			}},
			"config",
		)
		if len(diffs) != 1 || diffs[0].IsCritical {
			t.Fatalf("expected one non-critical difference, got: %#v", diffs)
		}
	})

	t.Run("private archive replaced by defaults stays critical", func(t *testing.T) {
		// The ingestion source genuinely changed; this must not be blessed.
		diffs := checker.compareConfigMaps(
			map[string]interface{}{"history_archive_urls": []string{
				"https://archive.internal.example.com/core",
			}},
			map[string]interface{}{"history_archive_urls": []string{
				"https://history.stellar.org/prd/core-live-001",
			}},
			"config",
		)
		if len(diffs) != 1 || !diffs[0].IsCritical {
			t.Fatalf("expected one critical difference, got: %#v", diffs)
		}
	})
}
