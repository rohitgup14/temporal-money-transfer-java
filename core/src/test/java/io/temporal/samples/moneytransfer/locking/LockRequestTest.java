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

import org.junit.Test;

public class LockRequestTest {

  @Test
  public void testDefaultConstructor() {
    LockRequest request = new LockRequest();
    assertNull(request.getLockId());
    assertNull(request.getRequesterId());
    assertEquals(0, request.getTimeoutSeconds());
  }

  @Test
  public void testParameterizedConstructor() {
    String lockId = "lock-123";
    String requesterId = "requester-456";
    long timeoutSeconds = 300;

    LockRequest request = new LockRequest(lockId, requesterId, timeoutSeconds);

    assertEquals(lockId, request.getLockId());
    assertEquals(requesterId, request.getRequesterId());
    assertEquals(timeoutSeconds, request.getTimeoutSeconds());
  }

  @Test
  public void testGettersAndSetters() {
    LockRequest request = new LockRequest();

    String lockId = "lock-789";
    String requesterId = "requester-012";
    long timeoutSeconds = 600;

    request.setLockId(lockId);
    request.setRequesterId(requesterId);
    request.setTimeoutSeconds(timeoutSeconds);

    assertEquals(lockId, request.getLockId());
    assertEquals(requesterId, request.getRequesterId());
    assertEquals(timeoutSeconds, request.getTimeoutSeconds());
  }

  @Test
  public void testEquals() {
    LockRequest request1 = new LockRequest("lock-1", "requester-1", 100);
    LockRequest request2 = new LockRequest("lock-1", "requester-1", 200);
    LockRequest request3 = new LockRequest("lock-2", "requester-1", 100);
    LockRequest request4 = new LockRequest("lock-1", "requester-2", 100);

    // Same lockId and requesterId should be equal (timeout doesn't matter)
    assertEquals(request1, request2);

    // Different lockId
    assertNotEquals(request1, request3);

    // Different requesterId
    assertNotEquals(request1, request4);

    // Same object
    assertEquals(request1, request1);

    // Null
    assertNotEquals(request1, null);

    // Different type
    assertNotEquals(request1, "not-a-lock-request");
  }

  @Test
  public void testHashCode() {
    LockRequest request1 = new LockRequest("lock-1", "requester-1", 100);
    LockRequest request2 = new LockRequest("lock-1", "requester-1", 200);

    // Equal objects should have same hash code
    assertEquals(request1.hashCode(), request2.hashCode());
  }

  @Test
  public void testToString() {
    LockRequest request = new LockRequest("lock-123", "requester-456", 300);
    String toString = request.toString();

    assertNotNull(toString);
    assertTrue(toString.contains("lock-123"));
    assertTrue(toString.contains("requester-456"));
    assertTrue(toString.contains("300"));
  }
}

