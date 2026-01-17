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

package io.temporal.samples.moneytransfer.database;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.UUID;
import org.junit.Test;

public class TransactionEntityTest {

  @Test
  public void testDefaultConstructor() {
    TransactionEntity entity = new TransactionEntity();
    assertNotNull(entity.getId());
    assertEquals(0, entity.getVersion());
  }

  @Test
  public void testParameterizedConstructor() {
    String workflowId = "workflow-123";
    String workflowRunId = "run-456";
    TransactionEntity.TransactionType type = TransactionEntity.TransactionType.WITHDRAW;
    BigDecimal amount = BigDecimal.valueOf(100.50);
    TransactionEntity.TransactionStatus status = TransactionEntity.TransactionStatus.PENDING;

    TransactionEntity entity = new TransactionEntity(workflowId, workflowRunId, type, amount, status);

    assertNotNull(entity.getId());
    assertEquals(workflowId, entity.getWorkflowId());
    assertEquals(workflowRunId, entity.getWorkflowRunId());
    assertEquals(type, entity.getTransactionType());
    assertEquals(amount, entity.getAmount());
    assertEquals(status, entity.getStatus());
    assertEquals(0, entity.getVersion());
  }

  @Test
  public void testGettersAndSetters() {
    TransactionEntity entity = new TransactionEntity();

    String id = UUID.randomUUID().toString();
    String workflowId = "workflow-123";
    String workflowRunId = "run-456";
    TransactionEntity.TransactionType type = TransactionEntity.TransactionType.DEPOSIT;
    BigDecimal amount = BigDecimal.valueOf(250.75);
    String fromAccount = "account-from";
    String toAccount = "account-to";
    String idempotencyKey = "idempotency-key-123";
    String chargeId = "charge-789";
    TransactionEntity.TransactionStatus status = TransactionEntity.TransactionStatus.COMPLETED;
    String scenario = "HAPPY_PATH";
    long version = 5;
    Timestamp createdAt = new Timestamp(System.currentTimeMillis());
    Timestamp updatedAt = new Timestamp(System.currentTimeMillis());

    entity.setId(id);
    entity.setWorkflowId(workflowId);
    entity.setWorkflowRunId(workflowRunId);
    entity.setTransactionType(type);
    entity.setAmount(amount);
    entity.setFromAccount(fromAccount);
    entity.setToAccount(toAccount);
    entity.setIdempotencyKey(idempotencyKey);
    entity.setChargeId(chargeId);
    entity.setStatus(status);
    entity.setScenario(scenario);
    entity.setVersion(version);
    entity.setCreatedAt(createdAt);
    entity.setUpdatedAt(updatedAt);

    assertEquals(id, entity.getId());
    assertEquals(workflowId, entity.getWorkflowId());
    assertEquals(workflowRunId, entity.getWorkflowRunId());
    assertEquals(type, entity.getTransactionType());
    assertEquals(amount, entity.getAmount());
    assertEquals(fromAccount, entity.getFromAccount());
    assertEquals(toAccount, entity.getToAccount());
    assertEquals(idempotencyKey, entity.getIdempotencyKey());
    assertEquals(chargeId, entity.getChargeId());
    assertEquals(status, entity.getStatus());
    assertEquals(scenario, entity.getScenario());
    assertEquals(version, entity.getVersion());
    assertEquals(createdAt, entity.getCreatedAt());
    assertEquals(updatedAt, entity.getUpdatedAt());
  }

  @Test
  public void testTransactionTypeEnum() {
    assertEquals(TransactionEntity.TransactionType.WITHDRAW, TransactionEntity.TransactionType.valueOf("WITHDRAW"));
    assertEquals(TransactionEntity.TransactionType.DEPOSIT, TransactionEntity.TransactionType.valueOf("DEPOSIT"));
    assertEquals(TransactionEntity.TransactionType.UNDO_WITHDRAW, TransactionEntity.TransactionType.valueOf("UNDO_WITHDRAW"));
  }

  @Test
  public void testTransactionStatusEnum() {
    assertEquals(TransactionEntity.TransactionStatus.PENDING, TransactionEntity.TransactionStatus.valueOf("PENDING"));
    assertEquals(TransactionEntity.TransactionStatus.COMPLETED, TransactionEntity.TransactionStatus.valueOf("COMPLETED"));
    assertEquals(TransactionEntity.TransactionStatus.FAILED, TransactionEntity.TransactionStatus.valueOf("FAILED"));
    assertEquals(TransactionEntity.TransactionStatus.ROLLED_BACK, TransactionEntity.TransactionStatus.valueOf("ROLLED_BACK"));
  }

  @Test
  public void testToString() {
    TransactionEntity entity = new TransactionEntity();
    entity.setId("test-id");
    entity.setWorkflowId("workflow-123");
    entity.setTransactionType(TransactionEntity.TransactionType.WITHDRAW);
    entity.setAmount(BigDecimal.valueOf(100));
    entity.setStatus(TransactionEntity.TransactionStatus.PENDING);
    entity.setVersion(1);

    String toString = entity.toString();
    assertNotNull(toString);
    assertTrue(toString.contains("test-id"));
    assertTrue(toString.contains("workflow-123"));
    assertTrue(toString.contains("WITHDRAW"));
    assertTrue(toString.contains("PENDING"));
    assertTrue(toString.contains("1"));
  }
}

