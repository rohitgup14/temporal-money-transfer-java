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

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.testing.TestWorkflowRule;
import java.time.Duration;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class LockManagerWorkflowTest {

  @Rule
  public TestWorkflowRule testWorkflowRule =
      TestWorkflowRule.newBuilder()
          .setWorkflowTypes(LockManagerWorkflowImpl.class)
          .setDoNotStart(true)
          .build();

  private String groupId;

  @Before
  public void setUp() {
    groupId = "test-group-" + UUID.randomUUID().toString();
    testWorkflowRule.getTestEnvironment().start();
  }

  @After
  public void tearDown() {
    testWorkflowRule.getTestEnvironment().shutdown();
  }

  @Test
  public void testLockAcquisitionAndRelease() {
    String workflowId = "lock-manager-" + groupId;
    LockManagerWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                LockManagerWorkflow.class,
                WorkflowOptions.newBuilder()
                    .setWorkflowId(workflowId)
                    .setTaskQueue(testWorkflowRule.getTaskQueue())
                    .setWorkflowExecutionTimeout(Duration.ofMinutes(1))
                    .build());

    // Start the workflow
    WorkflowClient.start(workflow::manageLocks, groupId);

    // Give workflow time to start
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMillis(100));

    // Request lock
    String lockId1 = UUID.randomUUID().toString();
    String requesterId1 = "requester-1";
    LockRequest request1 = new LockRequest(lockId1, requesterId1, 0);
    workflow.acquireLock(request1);

    // Give workflow time to process
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMillis(200));

    // Check lock status
    LockStatus status = workflow.getLockStatus();
    assertNotNull(status);
    assertEquals(groupId, status.getGroupId());
    assertEquals(lockId1, status.getCurrentLockId());
    assertEquals(requesterId1, status.getCurrentLockHolder());
    assertTrue(status.isLocked());
    assertEquals(0, status.getQueueSize());

    // Release lock
    workflow.releaseLock(lockId1);
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMillis(200));

    // Check lock is released
    status = workflow.getLockStatus();
    assertFalse(status.isLocked());
    assertNull(status.getCurrentLockId());
    assertNull(status.getCurrentLockHolder());
  }

  @Test
  public void testLockQueue() {
    String workflowId = "lock-manager-" + groupId;
    LockManagerWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                LockManagerWorkflow.class,
                WorkflowOptions.newBuilder()
                    .setWorkflowId(workflowId)
                    .setTaskQueue(testWorkflowRule.getTaskQueue())
                    .setWorkflowExecutionTimeout(Duration.ofMinutes(1))
                    .build());

    // Start the workflow
    WorkflowClient.start(workflow::manageLocks, groupId);
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMillis(100));

    // Request first lock
    String lockId1 = UUID.randomUUID().toString();
    LockRequest request1 = new LockRequest(lockId1, "requester-1", 0);
    workflow.acquireLock(request1);

    // Request second lock (should be queued)
    String lockId2 = UUID.randomUUID().toString();
    LockRequest request2 = new LockRequest(lockId2, "requester-2", 0);
    workflow.acquireLock(request2);

    // Request third lock (should be queued)
    String lockId3 = UUID.randomUUID().toString();
    LockRequest request3 = new LockRequest(lockId3, "requester-3", 0);
    workflow.acquireLock(request3);

    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMillis(300));

    // Check first lock is held
    LockStatus status = workflow.getLockStatus();
    assertEquals(lockId1, status.getCurrentLockId());
    assertEquals(2, status.getQueueSize());
    assertTrue(status.getQueue().contains(lockId2));
    assertTrue(status.getQueue().contains(lockId3));

    // Release first lock
    workflow.releaseLock(lockId1);
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMillis(300));

    // Check second lock is now held
    status = workflow.getLockStatus();
    assertEquals(lockId2, status.getCurrentLockId());
    assertEquals(1, status.getQueueSize());
    assertTrue(status.getQueue().contains(lockId3));

    // Release second lock
    workflow.releaseLock(lockId2);
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMillis(300));

    // Check third lock is now held
    status = workflow.getLockStatus();
    assertEquals(lockId3, status.getCurrentLockId());
    assertEquals(0, status.getQueueSize());
  }

  @Test
  public void testLockTimeout() {
    String workflowId = "lock-manager-" + groupId;
    LockManagerWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                LockManagerWorkflow.class,
                WorkflowOptions.newBuilder()
                    .setWorkflowId(workflowId)
                    .setTaskQueue(testWorkflowRule.getTaskQueue())
                    .setWorkflowExecutionTimeout(Duration.ofMinutes(1))
                    .build());

    // Start the workflow
    WorkflowClient.start(workflow::manageLocks, groupId);
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMillis(100));

    // Request lock with 1 second timeout
    String lockId = UUID.randomUUID().toString();
    LockRequest request = new LockRequest(lockId, "requester-1", 1);
    workflow.acquireLock(request);

    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMillis(200));

    // Verify lock is held
    LockStatus status = workflow.getLockStatus();
    assertEquals(lockId, status.getCurrentLockId());
    assertTrue(status.isLocked());

    // Sleep past timeout
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofSeconds(2));

    // Verify lock is automatically released
    status = workflow.getLockStatus();
    assertFalse(status.isLocked());
    assertNull(status.getCurrentLockId());
  }

  @Test
  public void testMultipleGroupIds() {
    String groupId1 = "group-1";
    String groupId2 = "group-2";

    String workflowId1 = "lock-manager-" + groupId1;
    String workflowId2 = "lock-manager-" + groupId2;

    LockManagerWorkflow workflow1 =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                LockManagerWorkflow.class,
                WorkflowOptions.newBuilder()
                    .setWorkflowId(workflowId1)
                    .setTaskQueue(testWorkflowRule.getTaskQueue())
                    .setWorkflowExecutionTimeout(Duration.ofMinutes(1))
                    .build());

    LockManagerWorkflow workflow2 =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                LockManagerWorkflow.class,
                WorkflowOptions.newBuilder()
                    .setWorkflowId(workflowId2)
                    .setTaskQueue(testWorkflowRule.getTaskQueue())
                    .setWorkflowExecutionTimeout(Duration.ofMinutes(1))
                    .build());

    // Start both workflows
    WorkflowClient.start(workflow1::manageLocks, groupId1);
    WorkflowClient.start(workflow2::manageLocks, groupId2);
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMillis(100));

    // Acquire locks in both groups
    String lockId1 = UUID.randomUUID().toString();
    String lockId2 = UUID.randomUUID().toString();

    workflow1.acquireLock(new LockRequest(lockId1, "requester-1", 0));
    workflow2.acquireLock(new LockRequest(lockId2, "requester-2", 0));

    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMillis(200));

    // Both locks should be held independently
    LockStatus status1 = workflow1.getLockStatus();
    LockStatus status2 = workflow2.getLockStatus();

    assertEquals(lockId1, status1.getCurrentLockId());
    assertEquals(lockId2, status2.getCurrentLockId());
    assertEquals(groupId1, status1.getGroupId());
    assertEquals(groupId2, status2.getGroupId());
  }

  @Test
  public void testInvalidLockRelease() {
    String workflowId = "lock-manager-" + groupId;
    LockManagerWorkflow workflow =
        testWorkflowRule
            .getWorkflowClient()
            .newWorkflowStub(
                LockManagerWorkflow.class,
                WorkflowOptions.newBuilder()
                    .setWorkflowId(workflowId)
                    .setTaskQueue(testWorkflowRule.getTaskQueue())
                    .setWorkflowExecutionTimeout(Duration.ofMinutes(1))
                    .build());

    // Start the workflow
    WorkflowClient.start(workflow::manageLocks, groupId);
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMillis(100));

    // Try to release a lock that doesn't exist
    workflow.releaseLock("non-existent-lock");
    testWorkflowRule.getTestEnvironment().sleep(Duration.ofMillis(100));

    // Lock status should remain unchanged
    LockStatus status = workflow.getLockStatus();
    assertFalse(status.isLocked());
  }
}

