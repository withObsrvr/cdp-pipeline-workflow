# PR Review Changes Applied

## Summary

All 5 issues identified in the PR review have been successfully addressed. The changes improve code maintainability, eliminate duplication, enhance error handling, and follow Go best practices.

## Changes Made

### 1. Magic String Constant ✅
**Issue**: Hardcoded string "-config" should be extracted as a constant

**Solution**: 
- Added `legacyConfigFlag` constant in main.go
- Updated usage in main() function

```go
const (
    // legacyConfigFlag is the flag used for backward compatibility with the old CLI
    legacyConfigFlag = "-config"
)
```

### 2. Duplicated buildProcessorChain Logic ✅
**Issue**: The buildProcessorChain function was duplicated between main.go and runner.go

**Solution**:
- Created shared `pkg/pipeline/chain.go` with `BuildProcessorChain` function
- Both main.go and runner.go now use the shared implementation
- Removed duplicate code from both files

```go
// pkg/pipeline/chain.go
func BuildProcessorChain(processors []processor.Processor, consumers []processor.Processor) {
    // Shared implementation
}
```

### 3. Error Handling in SetDefault ✅
**Issue**: WriteConfig() can fail but error was not handled

**Solution**:
- Updated SetDefault to return error
- Callers can now handle configuration write failures

```go
func SetDefault(key, value string) error {
    viper.Set(key, value)
    return viper.WriteConfig()
}
```

### 4. Remove os.Kill Signal ✅
**Issue**: Using os.Kill signal is not recommended as it cannot be caught gracefully

**Solution**:
- Removed os.Kill from signal.NotifyContext in both run.go and main.go
- Now only uses os.Interrupt for graceful shutdown

```go
// Before: ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
// After:
ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
```

### 5. Global Factory Variables Refactored ✅
**Issue**: Global variables for factory functions create hidden dependencies and make testing difficult

**Solution**:
- Introduced `Factories` struct to encapsulate factory functions
- Runner now accepts factories via dependency injection
- Added `SetFactories` function in cmd package for initialization
- Removed global variables from runner.go

```go
// Factories struct for dependency injection
type Factories struct {
    CreateSourceAdapter func(SourceConfig) (SourceAdapter, error)
    CreateProcessor     func(processor.ProcessorConfig) (processor.Processor, error)
    CreateConsumer      func(consumer.ConsumerConfig) (processor.Processor, error)
}

// Runner constructor now accepts factories
func New(opts Options, factories Factories) *Runner {
    return &Runner{
        opts:      opts,
        factories: factories,
    }
}
```

## Files Modified

1. **main.go**
   - Added `legacyConfigFlag` constant
   - Updated initializeFactories() to use dependency injection
   - Removed os.Kill from signal handling

2. **internal/cli/cmd/run.go**
   - Added factories variable and SetFactories function
   - Removed os.Kill from signal handling

3. **internal/cli/runner/runner.go**
   - Added Factories struct
   - Updated New() to accept factories
   - Removed global factory variables
   - Removed duplicate buildProcessorChain function

4. **internal/cli/config/config.go**
   - Updated SetDefault to return error

5. **pkg/pipeline/chain.go** (NEW)
   - Created shared BuildProcessorChain function

## Testing

The project builds successfully with all changes applied:
```bash
make clean && make build
# Successfully builds flowctl binary
```

## Benefits

1. **Better Maintainability**: Constants make the code easier to update
2. **DRY Principle**: No more duplicated processor chain logic
3. **Error Handling**: Configuration write failures are now properly handled
4. **Graceful Shutdown**: Only uses interruptible signals
5. **Testability**: Dependency injection makes unit testing much easier
6. **Clean Architecture**: No hidden dependencies through global variables