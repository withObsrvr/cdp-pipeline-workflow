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
