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

import java.util.ArrayList;
import java.util.List;

/**
 * Status of the lock manager workflow, showing current lock holder and queue of pending requests.
 */
public class LockStatus {
  private String groupId;
  private String currentLockHolder;
  private String currentLockId;
  private List<String> queue;
  private int queueSize;

  public LockStatus() {
    this.queue = new ArrayList<>();
  }

  public LockStatus(String groupId, String currentLockHolder, String currentLockId, List<String> queue) {
    this.groupId = groupId;
    this.currentLockHolder = currentLockHolder;
    this.currentLockId = currentLockId;
    this.queue = queue != null ? queue : new ArrayList<>();
    this.queueSize = this.queue.size();
  }

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public String getCurrentLockHolder() {
    return currentLockHolder;
  }

  public void setCurrentLockHolder(String currentLockHolder) {
    this.currentLockHolder = currentLockHolder;
  }

  public String getCurrentLockId() {
    return currentLockId;
  }

  public void setCurrentLockId(String currentLockId) {
    this.currentLockId = currentLockId;
  }

  public List<String> getQueue() {
    return queue;
  }

  public void setQueue(List<String> queue) {
    this.queue = queue != null ? queue : new ArrayList<>();
    this.queueSize = this.queue.size();
  }

  public int getQueueSize() {
    return queueSize;
  }

  public void setQueueSize(int queueSize) {
    this.queueSize = queueSize;
  }

  public boolean isLocked() {
    return currentLockHolder != null && !currentLockHolder.isEmpty();
  }

  @Override
  public String toString() {
    return "LockStatus{"
        + "groupId='"
        + groupId
        + '\''
        + ", currentLockHolder='"
        + currentLockHolder
        + '\''
        + ", currentLockId='"
        + currentLockId
        + '\''
        + ", queueSize="
        + queueSize
        + '}';
  }
}

