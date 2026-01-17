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

import java.util.Objects;

/**
 * Request to acquire a lock through the lock manager workflow.
 */
public class LockRequest {
  private String lockId;
  private String requesterId;
  private long timeoutSeconds;

  public LockRequest() {}

  public LockRequest(String lockId, String requesterId, long timeoutSeconds) {
    this.lockId = lockId;
    this.requesterId = requesterId;
    this.timeoutSeconds = timeoutSeconds;
  }

  public String getLockId() {
    return lockId;
  }

  public void setLockId(String lockId) {
    this.lockId = lockId;
  }

  public String getRequesterId() {
    return requesterId;
  }

  public void setRequesterId(String requesterId) {
    this.requesterId = requesterId;
  }

  public long getTimeoutSeconds() {
    return timeoutSeconds;
  }

  public void setTimeoutSeconds(long timeoutSeconds) {
    this.timeoutSeconds = timeoutSeconds;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LockRequest that = (LockRequest) o;
    return Objects.equals(lockId, that.lockId) && Objects.equals(requesterId, that.requesterId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lockId, requesterId);
  }

  @Override
  public String toString() {
    return "LockRequest{"
        + "lockId='"
        + lockId
        + '\''
        + ", requesterId='"
        + requesterId
        + '\''
        + ", timeoutSeconds="
        + timeoutSeconds
        + '}';
  }
}

