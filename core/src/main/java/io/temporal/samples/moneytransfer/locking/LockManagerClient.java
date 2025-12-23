/*
 *  Copyright (c) 2020 Temporal Technologies, Inc. All Rights Reserved
 *
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not
 *  use this file except in compliance with the License. A copy of the License is
 *  located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package io.temporal.samples.moneytransfer.locking;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.samples.moneytransfer.TemporalClient;
import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.UUID;
import javax.net.ssl.SSLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client utility for interacting with lock manager workflows. Provides methods to acquire and
 * release locks based on group IDs, ensuring serialized execution of database transactions.
 */
public class LockManagerClient {
  private static final Logger log = LoggerFactory.getLogger(LockManagerClient.class);
  private static final String LOCK_MANAGER_TASK_QUEUE = "LockManagerTaskQueue";
  private static final long DEFAULT_LOCK_TIMEOUT_SECONDS = 300; // 5 minutes

  /**
   * Acquires a lock for a given group ID. If the lock is currently held, this method will wait
   * until the lock is available. The lock is identified by a unique lock ID.
   *
   * @param groupId the group ID that determines which lock manager workflow to use
   * @param requesterId identifier for the requester (e.g., workflow ID, transaction ID)
   * @param timeoutSeconds timeout in seconds for the lock (0 = no timeout)
   * @return the lock ID that must be used to release the lock
   */
  public static String acquireLock(String groupId, String requesterId, long timeoutSeconds)
      throws FileNotFoundException, SSLException {
    WorkflowClient client = TemporalClient.get();
    String workflowId = "lock-manager-" + groupId;

    // Get or create the lock manager workflow
    WorkflowOptions options =
        WorkflowOptions.newBuilder()
            .setWorkflowId(workflowId)
            .setTaskQueue(LOCK_MANAGER_TASK_QUEUE)
            .setWorkflowExecutionTimeout(Duration.ofHours(24)) // Run for up to 24 hours
            .build();

    LockManagerWorkflow lockManager = client.newWorkflowStub(LockManagerWorkflow.class, options);

    // Start the workflow if not already running (idempotent)
    try {
      WorkflowClient.start(lockManager::manageLocks, groupId);
      log.debug("Started/continued lock manager workflow for group: {}", groupId);
    } catch (Exception e) {
      // Workflow might already be running, which is fine
      log.debug("Lock manager workflow may already be running for group: {}", groupId);
    }

    // Generate unique lock ID
    String lockId = UUID.randomUUID().toString();
    LockRequest lockRequest = new LockRequest(lockId, requesterId, timeoutSeconds);

    // Send acquire lock signal
    lockManager.acquireLock(lockRequest);

    // Poll for lock acquisition using query
    long pollIntervalMs = 100;
    long maxWaitTimeMs = timeoutSeconds > 0 ? timeoutSeconds * 1000 : 60000; // Default 60s max
    long startTime = System.currentTimeMillis();

    while (System.currentTimeMillis() - startTime < maxWaitTimeMs) {
      try {
        LockStatus status = lockManager.getLockStatus();
        if (lockId.equals(status.getCurrentLockId())
            && requesterId.equals(status.getCurrentLockHolder())) {
          log.info("Lock acquired: {} for group: {}", lockId, groupId);
          return lockId;
        }

        // Check if we're in the queue
        if (status.getQueue().contains(lockId)) {
          log.debug("Lock request {} is in queue for group: {}", lockId, groupId);
        }

        Thread.sleep(pollIntervalMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for lock", e);
      } catch (Exception e) {
        log.error("Error checking lock status", e);
        throw new RuntimeException("Failed to acquire lock", e);
      }
    }

    throw new RuntimeException(
        "Timeout waiting for lock acquisition: " + lockId + " for group: " + groupId);
  }

  /**
   * Acquires a lock with default timeout.
   *
   * @param groupId the group ID
   * @param requesterId identifier for the requester
   * @return the lock ID
   */
  public static String acquireLock(String groupId, String requesterId)
      throws FileNotFoundException, SSLException {
    return acquireLock(groupId, requesterId, DEFAULT_LOCK_TIMEOUT_SECONDS);
  }

  /**
   * Releases a lock for a given group ID.
   *
   * @param groupId the group ID
   * @param lockId the lock ID returned from acquireLock
   */
  public static void releaseLock(String groupId, String lockId)
      throws FileNotFoundException, SSLException {
    WorkflowClient client = TemporalClient.get();
    String workflowId = "lock-manager-" + groupId;

    WorkflowOptions options =
        WorkflowOptions.newBuilder()
            .setWorkflowId(workflowId)
            .setTaskQueue(LOCK_MANAGER_TASK_QUEUE)
            .build();

    LockManagerWorkflow lockManager = client.newWorkflowStub(LockManagerWorkflow.class, options);
    lockManager.releaseLock(lockId);
    log.info("Lock released: {} for group: {}", lockId, groupId);
  }

  /**
   * Gets the current lock status for a group ID.
   *
   * @param groupId the group ID
   * @return the current lock status
   */
  public static LockStatus getLockStatus(String groupId)
      throws FileNotFoundException, SSLException {
    WorkflowClient client = TemporalClient.get();
    String workflowId = "lock-manager-" + groupId;

    WorkflowOptions options =
        WorkflowOptions.newBuilder()
            .setWorkflowId(workflowId)
            .setTaskQueue(LOCK_MANAGER_TASK_QUEUE)
            .build();

    LockManagerWorkflow lockManager = client.newWorkflowStub(LockManagerWorkflow.class, options);
    return lockManager.getLockStatus();
  }

  /**
   * Executes a database operation with a lock. This is a convenience method that acquires a lock,
   * executes the operation, and releases the lock automatically.
   *
   * @param groupId the group ID for the lock
   * @param requesterId identifier for the requester
   * @param operation the operation to execute
   * @param <T> the return type of the operation
   * @return the result of the operation
   */
  public static <T> T executeWithLock(
      String groupId, String requesterId, LockedOperation<T> operation)
      throws Exception {
    String lockId = null;
    try {
      lockId = acquireLock(groupId, requesterId);
      return operation.execute();
    } finally {
      if (lockId != null) {
        try {
          releaseLock(groupId, lockId);
        } catch (Exception e) {
          log.error("Error releasing lock: {} for group: {}", lockId, groupId, e);
          // Don't throw - lock release errors shouldn't hide operation errors
        }
      }
    }
  }

  /**
   * Functional interface for operations that need to be executed with a lock.
   */
  @FunctionalInterface
  public interface LockedOperation<T> {
    T execute() throws Exception;
  }
}

