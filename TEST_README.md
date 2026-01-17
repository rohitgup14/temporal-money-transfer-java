# Unit Tests

This document describes the unit tests added to the project.

## Test Structure

All tests are located in `core/src/test/java/io/temporal/samples/moneytransfer/`

## Test Files

### 1. LockManagerWorkflowTest

**Location:** `core/src/test/java/io/temporal/samples/moneytransfer/locking/LockManagerWorkflowTest.java`

Tests for the distributed lock manager workflow implementation.

**Test Cases:**
- `testLockAcquisitionAndRelease` - Verifies basic lock acquisition and release
- `testLockQueue` - Tests FIFO queue behavior when multiple locks are requested
- `testLockTimeout` - Verifies automatic lock release after timeout
- `testMultipleGroupIds` - Ensures different group IDs have independent locks
- `testInvalidLockRelease` - Tests handling of invalid lock release attempts

**Usage:**
```bash
./gradlew test --tests LockManagerWorkflowTest
```

### 2. TransactionRepositoryTest

**Location:** `core/src/test/java/io/temporal/samples/moneytransfer/database/TransactionRepositoryTest.java`

Tests for the TransactionRepository using H2 in-memory database.

**Test Cases:**
- `testSaveTransaction` - Tests saving a new transaction
- `testFindById` - Tests finding transactions by ID
- `testFindByWorkflowId` - Tests finding all transactions for a workflow
- `testFindByIdempotencyKey` - Tests finding transactions by idempotency key
- `testUpdateWithOptimisticLocking` - Tests optimistic locking (version conflict detection)
- `testUpdateWithRetry` - Tests retry logic for concurrent updates

**Note:** Uses H2 in-memory database for testing. The test temporarily replaces the DataSource in DatabaseConfig using reflection.

**Usage:**
```bash
./gradlew test --tests TransactionRepositoryTest
```

### 3. TransactionEntityTest

**Location:** `core/src/test/java/io/temporal/samples/moneytransfer/database/TransactionEntityTest.java`

Tests for the TransactionEntity data class.

**Test Cases:**
- `testDefaultConstructor` - Verifies default values
- `testParameterizedConstructor` - Tests constructor with parameters
- `testGettersAndSetters` - Tests all getter/setter methods
- `testTransactionTypeEnum` - Tests TransactionType enum values
- `testTransactionStatusEnum` - Tests TransactionStatus enum values
- `testToString` - Tests toString method

**Usage:**
```bash
./gradlew test --tests TransactionEntityTest
```

### 4. LockRequestTest

**Location:** `core/src/test/java/io/temporal/samples/moneytransfer/locking/LockRequestTest.java`

Tests for the LockRequest data class.

**Test Cases:**
- `testDefaultConstructor` - Verifies default values
- `testParameterizedConstructor` - Tests constructor with parameters
- `testGettersAndSetters` - Tests all getter/setter methods
- `testEquals` - Tests equals method (based on lockId and requesterId)
- `testHashCode` - Tests hashCode consistency
- `testToString` - Tests toString method

**Usage:**
```bash
./gradlew test --tests LockRequestTest
```

### 5. LockStatusTest

**Location:** `core/src/test/java/io/temporal/samples/moneytransfer/locking/LockStatusTest.java`

Tests for the LockStatus data class.

**Test Cases:**
- `testDefaultConstructor` - Verifies default values
- `testParameterizedConstructor` - Tests constructor with parameters
- `testParameterizedConstructorWithNullQueue` - Tests handling of null queue
- `testGettersAndSetters` - Tests all getter/setter methods
- `testIsLocked` - Tests isLocked() method logic
- `testSetQueueWithNull` - Tests handling of null queue in setter
- `testToString` - Tests toString method

**Usage:**
```bash
./gradlew test --tests LockStatusTest
```

## Running All Tests

To run all tests:

```bash
./gradlew test
```

To run tests for a specific package:

```bash
./gradlew test --tests "io.temporal.samples.moneytransfer.locking.*"
./gradlew test --tests "io.temporal.samples.moneytransfer.database.*"
```

## Dependencies

Tests use:
- **JUnit 4** - Test framework (already in project)
- **Mockito** - Mocking framework (already in project)
- **H2 Database** - In-memory database for testing (added to build.gradle)
- **Temporal Testing** - TestWorkflowRule for workflow testing (already in project)

## Test Coverage

The tests cover:
- ✅ Lock manager workflow functionality
- ✅ Database operations (CRUD)
- ✅ Optimistic locking (version-based)
- ✅ Transaction entity model
- ✅ Lock request/status data classes
- ✅ Edge cases and error handling

## Notes

1. **Database Tests**: The TransactionRepositoryTest uses H2 in-memory database and temporarily replaces the DataSource using reflection. This allows testing without requiring a real PostgreSQL database.

2. **Workflow Tests**: LockManagerWorkflowTest uses Temporal's TestWorkflowRule which provides an in-memory test environment for workflows.

3. **Isolation**: Each test class sets up and tears down its own test environment to ensure test isolation.

