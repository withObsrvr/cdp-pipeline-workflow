package processor

import (
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/common/types"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/processor/base"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/processor/contract/kale"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/processor/contract/soroswap"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/processor/ledger"
)

// init registers all built-in processors
func init() {
	// Register Kale processor
	base.RegisterProcessor("kale", func(config map[string]interface{}) (types.Processor, error) {
		return kale.NewKaleProcessor(config)
	})

	// Register Soroswap processor
	base.RegisterProcessor("soroswap", func(config map[string]interface{}) (types.Processor, error) {
		return soroswap.NewSoroswapProcessor(config)
	})

	// Register LatestLedger processor
	base.RegisterProcessor("LatestLedger", func(config map[string]interface{}) (types.Processor, error) {
		return ledger.NewLatestLedgerProcessor(config)
	})
}
