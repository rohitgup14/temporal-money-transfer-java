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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class LockStatusTest {

  @Test
  public void testDefaultConstructor() {
    LockStatus status = new LockStatus();
    assertNull(status.getGroupId());
    assertNull(status.getCurrentLockHolder());
    assertNull(status.getCurrentLockId());
    assertNotNull(status.getQueue());
    assertTrue(status.getQueue().isEmpty());
    assertEquals(0, status.getQueueSize());
    assertFalse(status.isLocked());
  }

  @Test
  public void testParameterizedConstructor() {
    String groupId = "group-123";
    String currentLockHolder = "holder-456";
    String currentLockId = "lock-789";
    List<String> queue = Arrays.asList("lock-1", "lock-2", "lock-3");

    LockStatus status = new LockStatus(groupId, currentLockHolder, currentLockId, queue);

    assertEquals(groupId, status.getGroupId());
    assertEquals(currentLockHolder, status.getCurrentLockHolder());
    assertEquals(currentLockId, status.getCurrentLockId());
    assertEquals(3, status.getQueue().size());
    assertEquals(3, status.getQueueSize());
    assertTrue(status.isLocked());
  }

  @Test
  public void testParameterizedConstructorWithNullQueue() {
    LockStatus status = new LockStatus("group-1", "holder-1", "lock-1", null);

    assertNotNull(status.getQueue());
    assertTrue(status.getQueue().isEmpty());
    assertEquals(0, status.getQueueSize());
  }

  @Test
  public void testGettersAndSetters() {
    LockStatus status = new LockStatus();

    String groupId = "group-123";
    String currentLockHolder = "holder-456";
    String currentLockId = "lock-789";
    List<String> queue = new ArrayList<>(Arrays.asList("lock-1", "lock-2"));

    status.setGroupId(groupId);
    status.setCurrentLockHolder(currentLockHolder);
    status.setCurrentLockId(currentLockId);
    status.setQueue(queue);

    assertEquals(groupId, status.getGroupId());
    assertEquals(currentLockHolder, status.getCurrentLockHolder());
    assertEquals(currentLockId, status.getCurrentLockId());
    assertEquals(2, status.getQueue().size());
    assertEquals(2, status.getQueueSize());
    assertTrue(status.isLocked());
  }

  @Test
  public void testIsLocked() {
    LockStatus status = new LockStatus();

    // No lock holder
    assertFalse(status.isLocked());

    // Has lock holder
    status.setCurrentLockHolder("holder-1");
    status.setCurrentLockId("lock-1");
    assertTrue(status.isLocked());

    // Empty string lock holder
    status.setCurrentLockHolder("");
    assertFalse(status.isLocked());

    // Null lock holder
    status.setCurrentLockHolder(null);
    assertFalse(status.isLocked());
  }

  @Test
  public void testSetQueueWithNull() {
    LockStatus status = new LockStatus();
    status.setQueue(Arrays.asList("lock-1", "lock-2"));
    assertEquals(2, status.getQueueSize());

    status.setQueue(null);
    assertNotNull(status.getQueue());
    assertTrue(status.getQueue().isEmpty());
    assertEquals(0, status.getQueueSize());
  }

  @Test
  public void testToString() {
    LockStatus status = new LockStatus("group-123", "holder-456", "lock-789", Arrays.asList("lock-1", "lock-2"));
    String toString = status.toString();

    assertNotNull(toString);
    assertTrue(toString.contains("group-123"));
    assertTrue(toString.contains("holder-456"));
    assertTrue(toString.contains("lock-789"));
    assertTrue(toString.contains("2")); // queue size
  }
}

