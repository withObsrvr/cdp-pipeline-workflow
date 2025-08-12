# Stellar SDK Helper Methods Documentation

This directory contains comprehensive documentation for using Stellar Go SDK helper methods in the CDP Pipeline Workflow project.

## 📚 Documentation Files

### 1. [stellar-go-sdk-helper-methods.md](./stellar-go-sdk-helper-methods.md)
**Comprehensive Guide** - Complete documentation of all SDK helper methods
- Transaction processing methods
- Event extraction (V3/V4 compatible)
- Operation-specific helpers
- Asset and account utilities
- Best practices and examples

### 2. [stellar-sdk-quick-reference.md](./stellar-sdk-quick-reference.md)
**Quick Reference** - Side-by-side comparison of old vs new patterns
- Common conversions at a glance
- Quick decision tree
- Copy-paste ready code snippets
- Common pitfalls to avoid

### 3. [stellar-processor-migration-plan.md](./stellar-processor-migration-plan.md)
**Migration Plan** - Roadmap for implementing all Stellar processors
- Complete list of processors to implement
- Priority order and timeline
- Implementation templates
- Testing strategies

## 🎯 Why Use SDK Helpers?

1. **Protocol Version Independence**: Automatically handles V1, V2, V3, and V4 metadata
2. **Future-Proof**: New protocol versions supported via SDK updates
3. **Type Safety**: Compile-time checks with proper error handling
4. **Cleaner Code**: Less boilerplate, more readable
5. **Maintained by Stellar**: Official support and updates

## 🚀 Quick Start

If you're updating an existing processor:

1. Read the [Quick Reference](./stellar-sdk-quick-reference.md) for pattern conversions
2. Check the [Migration Plan](./stellar-processor-migration-plan.md) for your processor
3. Use the [Comprehensive Guide](./stellar-go-sdk-helper-methods.md) for detailed examples

If you're creating a new processor:

1. Start with the implementation template in the [Migration Plan](./stellar-processor-migration-plan.md)
2. Use the [Comprehensive Guide](./stellar-go-sdk-helper-methods.md) for specific helpers
3. Reference the [Quick Reference](./stellar-sdk-quick-reference.md) while coding

## ⚠️ Key Rule

**Never access `tx.UnsafeMeta` directly!** Always use SDK helper methods:

```go
// ❌ NEVER DO THIS
if tx.UnsafeMeta.V == 3 {
    events = tx.UnsafeMeta.V3.SorobanMeta.Events
}

// ✅ ALWAYS DO THIS
events, err := tx.GetTransactionEvents()
```

## 📊 Current Status

- ✅ ContractEventProcessor - Updated to use SDK helpers
- 🔄 ContractInvocationProcessor - Needs V4 update
- 📋 20+ processors to implement (see Migration Plan)

## 🤝 Contributing

When adding or updating processors:

1. Always use SDK helper methods
2. Add tests for V3 and V4 compatibility
3. Update documentation if new patterns emerge
4. Follow the templates in the migration plan

## 📞 Questions?

- Check the comprehensive guide first
- Look for examples in updated processors
- Reference Stellar's official Go SDK documentation
- Ask in project discussions

Remember: The SDK helpers make your code simpler, safer, and future-proof!