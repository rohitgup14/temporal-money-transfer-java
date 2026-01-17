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

import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.slf4j.Logger;

/**
 * Implementation of the distributed lock manager workflow. This workflow serializes database
 * transactions based on group ID by maintaining a queue of lock requests and processing them one
 * at a time (FIFO).
 */
public class LockManagerWorkflowImpl implements LockManagerWorkflow {
  private static final Logger log = Workflow.getLogger(LockManagerWorkflowImpl.class);

  // Workflow state
  private String groupId;
  private String currentLockId;
  private String currentLockHolder;
  private Queue<LockRequest> lockQueue;
  private LockRequest currentLockRequest;


  public LockManagerWorkflowImpl() {
    this.lockQueue = new LinkedList<>();
  }

  @Override
  public void manageLocks(String groupId) {
    this.groupId = groupId;
    log.info("Lock manager workflow started for group ID: {}", groupId);

    // Workflow runs indefinitely, processing lock requests as they arrive
    while (true) {
      // Wait for a lock request or release signal
      Workflow.await(() -> !lockQueue.isEmpty() || currentLockId == null);

      // If lock is available and queue has requests, grant lock to first in queue
      if (currentLockId == null && !lockQueue.isEmpty()) {
        grantNextLock();
      }

      // Small sleep to prevent tight loop and allow signals to be processed
      Workflow.sleep(Duration.ofMillis(100));
    }
  }

  @Override
  public void acquireLock(LockRequest lockRequest) {
    if (lockRequest == null || lockRequest.getLockId() == null) {
      log.warn("Invalid lock request received: {}", lockRequest);
      return;
    }

    log.info("Lock acquisition requested: {}", lockRequest);

    // Check if lock is already held
    if (currentLockId != null) {
      // Add to queue
      lockQueue.offer(lockRequest);
      log.info("Lock currently held, added to queue. Queue size: {}", lockQueue.size());
    } else {
      // Grant lock immediately
      grantLock(lockRequest);
    }
  }

  @Override
  public void releaseLock(String lockId) {
    if (lockId == null) {
      log.warn("Release lock called with null lock ID");
      return;
    }

    log.info("Lock release requested for: {}", lockId);

    // Only release if this is the currently held lock
    if (lockId.equals(currentLockId)) {
      log.info("Releasing lock: {} (holder: {})", lockId, currentLockHolder);
      currentLockId = null;
      currentLockHolder = null;
      currentLockRequest = null;

      // Grant lock to next in queue if available
      if (!lockQueue.isEmpty()) {
        grantNextLock();
      }
    } else {
      log.warn("Attempted to release lock {} but current lock is {}", lockId, currentLockId);
    }
  }

  @Override
  public LockStatus getLockStatus() {
    List<String> queueSnapshot = new ArrayList<>();
    for (LockRequest req : lockQueue) {
      queueSnapshot.add(req.getLockId());
    }

    return new LockStatus(groupId, currentLockHolder, currentLockId, queueSnapshot);
  }

  private void grantNextLock() {
    if (lockQueue.isEmpty()) {
      return;
    }

    LockRequest nextRequest = lockQueue.poll();
    grantLock(nextRequest);
  }

  private void grantLock(LockRequest lockRequest) {
    if (lockRequest == null) {
      return;
    }

    currentLockId = lockRequest.getLockId();
    currentLockHolder = lockRequest.getRequesterId();
    currentLockRequest = lockRequest;

    log.info("Lock granted: {} to requester: {}", currentLockId, currentLockHolder);

    // Set up automatic timeout if specified
    if (lockRequest.getTimeoutSeconds() > 0) {
      Workflow.newTimer(Duration.ofSeconds(lockRequest.getTimeoutSeconds()))
          .thenApply(
              v -> {
                // Auto-release after timeout
                if (currentLockId != null && currentLockId.equals(lockRequest.getLockId())) {
                  log.warn(
                      "Lock {} timed out after {} seconds, auto-releasing",
                      currentLockId,
                      lockRequest.getTimeoutSeconds());
                  releaseLock(currentLockId);
                }
                return null;
              });
    }
  }
}

