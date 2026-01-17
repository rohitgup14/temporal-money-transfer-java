# Distributed Lock Manager using Temporal Workflows

This project implements a distributed locking mechanism using Temporal workflows to serialize database transactions based on group IDs. This ensures that concurrent transactions sharing the same group ID are processed sequentially, preventing race conditions and data overrides.

## Overview

The lock manager uses Temporal workflows as a distributed coordination mechanism. Each group ID has its own lock manager workflow instance that maintains a queue of lock requests and processes them one at a time (FIFO).

## Architecture

### Components

1. **LockManagerWorkflow** - Temporal workflow interface that defines lock management operations
2. **LockManagerWorkflowImpl** - Workflow implementation that manages lock queues and serialization
3. **LockManagerClient** - Client utility for acquiring and releasing locks
4. **LockedTransactionRepository** - Wrapper around TransactionRepository that uses locks for database operations
5. **LockRequest** - Data class representing a lock acquisition request
6. **LockStatus** - Data class representing the current state of a lock manager

### How It Works

1. **Lock Acquisition**: When a transaction needs to be executed, it requests a lock via `LockManagerClient.acquireLock()`
   - The client sends a signal to the lock manager workflow for the group ID
   - If the lock is available, it's granted immediately
   - If the lock is held, the request is queued

2. **Queue Management**: The lock manager workflow maintains a queue of pending requests
   - Requests are processed in FIFO order
   - Only one lock can be held at a time per group ID

3. **Lock Release**: When the transaction completes, it releases the lock via `LockManagerClient.releaseLock()`
   - The workflow grants the lock to the next request in the queue
   - If the queue is empty, the lock becomes available

4. **Automatic Timeout**: Locks can have an optional timeout to prevent deadlocks
   - If a lock isn't released within the timeout period, it's automatically released

## Usage

### Basic Usage

```java
// Acquire a lock
String lockId = LockManagerClient.acquireLock(groupId, requesterId, timeoutSeconds);

try {
    // Perform database operations
    transactionRepository.save(transaction);
} finally {
    // Always release the lock
    LockManagerClient.releaseLock(groupId, lockId);
}
```

### Using LockedTransactionRepository (Recommended)

The `LockedTransactionRepository` automatically handles lock acquisition and release:

```java
LockedTransactionRepository lockedRepo = new LockedTransactionRepository();
String groupId = "account:12345";
String requesterId = workflowId + "-" + activityId;

// Save with automatic locking
lockedRepo.save(transaction, groupId, requesterId);

// Update with automatic locking
lockedRepo.updateWithOptimisticLocking(transaction, groupId, requesterId);
```

### Group ID Derivation

The `LockedTransactionRepository.deriveGroupId()` method automatically derives a group ID from transaction entities:

- If both `fromAccount` and `toAccount` are set: `"account-pair:{from}:{to}"`
- If only `fromAccount` is set: `"account:{from}"`
- If only `toAccount` is set: `"account:{to}"`
- Otherwise: `"workflow:{workflowId}"`

This ensures that transactions affecting the same account(s) are serialized.

## Integration

The lock manager is automatically integrated with `AccountTransferActivitiesImpl`:

- All `withdraw`, `deposit`, and `undoWithdraw` operations use distributed locking
- Group IDs are automatically derived from transaction entities
- Lock acquisition/release is handled automatically

## Configuration

The lock manager uses the following configuration:

- **Task Queue**: `LockManagerTaskQueue` (separate from the main workflow task queue)
- **Workflow ID Pattern**: `lock-manager-{groupId}`
- **Default Lock Timeout**: 300 seconds (5 minutes)

## Benefits

1. **Prevents Race Conditions**: Ensures transactions on the same resource are serialized
2. **Distributed**: Works across multiple worker instances
3. **Fault Tolerant**: Leverages Temporal's durability and reliability
4. **Automatic Queue Management**: No manual queue management needed
5. **Timeout Protection**: Automatic lock release prevents deadlocks

## Example Scenario

Consider two concurrent money transfers from Account A to Account B:

1. **Transfer 1** requests lock for group ID `"account-pair:A:B"`
2. **Transfer 2** requests lock for the same group ID
3. Transfer 1's lock is granted immediately
4. Transfer 2's lock request is queued
5. Transfer 1 completes and releases the lock
6. Transfer 2's lock is granted and processing begins

This ensures both transfers are processed sequentially, preventing account balance inconsistencies.

## Monitoring

You can query the lock status for a group ID:

```java
LockStatus status = LockManagerClient.getLockStatus(groupId);
System.out.println("Current lock holder: " + status.getCurrentLockHolder());
System.out.println("Queue size: " + status.getQueueSize());
```

## Worker Registration

The lock manager workflow must be registered in the worker:

```java
Worker lockManagerWorker = factory.newWorker("LockManagerTaskQueue", workerOptions);
lockManagerWorker.registerWorkflowImplementationTypes(LockManagerWorkflowImpl.class);
```

This is already done in `AccountTransferWorker`.

