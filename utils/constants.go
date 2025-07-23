package utils

// MinAvailableLedger is the minimum ledger sequence that is typically available
// Ledgers 1-2 are often not available in Stellar networks
const MinAvailableLedger = uint32(3)