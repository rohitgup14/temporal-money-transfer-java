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

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Workflow interface for distributed locking mechanism based on group IDs. This workflow acts as a
 * lock manager that serializes database transactions sharing the same group ID, ensuring no
 * concurrent overrides occur.
 */
@WorkflowInterface
public interface LockManagerWorkflow {

  /**
   * Workflow method that starts the lock manager for a specific group ID. This workflow runs
   * indefinitely, managing lock acquisition and release.
   *
   * @param groupId the group ID that this lock manager manages
   */
  @WorkflowMethod(name = "lockManagerWorkflow")
  void manageLocks(String groupId);

  /**
   * Signal to request a lock. The request will be queued if the lock is currently held.
   *
   * @param lockRequest the lock request containing lock ID and other metadata
   */
  @SignalMethod(name = "acquireLock")
  void acquireLock(LockRequest lockRequest);

  /**
   * Signal to release a lock. After release, the next queued request will be granted the lock.
   *
   * @param lockId the ID of the lock to release
   */
  @SignalMethod(name = "releaseLock")
  void releaseLock(String lockId);

  /**
   * Query to check the current lock status.
   *
   * @return the current lock state including who holds the lock and queue status
   */
  @QueryMethod(name = "getLockStatus")
  LockStatus getLockStatus();
}

