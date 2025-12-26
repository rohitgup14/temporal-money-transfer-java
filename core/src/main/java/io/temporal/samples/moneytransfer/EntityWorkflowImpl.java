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

package io.temporal.samples.moneytransfer;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.samples.moneytransfer.database.LockedTransactionRepository;
import io.temporal.samples.moneytransfer.database.TransactionEntity;
import io.temporal.samples.moneytransfer.dataclasses.ChargeResponseObj;
import io.temporal.workflow.Workflow;
import java.math.BigDecimal;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityWorkflowImpl implements EntityWorkflow {

  private static final Logger log = LoggerFactory.getLogger(EntityWorkflowImpl.class);

  // Activity options for database operations
  private final ActivityOptions activityOptions =
      ActivityOptions.newBuilder()
          .setStartToCloseTimeout(Duration.ofSeconds(30))
          .setRetryOptions(
              RetryOptions.newBuilder()
                  .setInitialInterval(Duration.ofSeconds(1))
                  .setMaximumInterval(Duration.ofSeconds(10))
                  .setMaximumAttempts(3)
                  .build())
          .build();

  // Activity stub for database operations
  private final EntityActivities entityActivities =
      Workflow.newActivityStub(EntityActivities.class, activityOptions);

  @Override
  public ChargeResponseObj handleDepositTransaction(
      String idempotencyKey,
      String workflowId,
      float amountDollars,
      String chargeId,
      String scenario) {

    log.info(
        "EntityWorkflow: Handling deposit transaction with idempotency key: {}, workflowId: {}",
        idempotencyKey,
        workflowId);

    String requesterId = Workflow.getInfo().getWorkflowId() + "-" + Workflow.getInfo().getRunId();

    // Check for existing transaction with same idempotency key to ensure idempotency
    TransactionEntity existingTransaction = null;
    try {
      if (idempotencyKey != null && !idempotencyKey.isEmpty()) {
        existingTransaction = entityActivities.findByIdempotencyKey(idempotencyKey);
        if (existingTransaction != null
            && existingTransaction.getStatus() == TransactionEntity.TransactionStatus.COMPLETED) {
          log.info(
              "EntityWorkflow: Found existing completed deposit transaction with idempotency key: {}",
              idempotencyKey);
          return new ChargeResponseObj(existingTransaction.getChargeId());
        }
      }
    } catch (Exception e) {
      log.error("EntityWorkflow: Failed to check for existing deposit transaction", e);
      // Continue with new transaction creation
    }

    // Persist deposit transaction to database with distributed locking
    try {
      TransactionEntity transaction;
      if (existingTransaction != null) {
        // Update existing transaction with lock protection
        transaction = existingTransaction;
        String groupId = LockedTransactionRepository.deriveGroupId(transaction);

        transaction.setStatus(TransactionEntity.TransactionStatus.COMPLETED);
        transaction.setChargeId(chargeId);
        boolean updated =
            entityActivities.updateWithOptimisticLocking(transaction, groupId, requesterId);
        if (!updated) {
          // Retry update in case of version conflict (with lock protection)
          entityActivities.updateWithRetry(
              transaction.getId(),
              TransactionEntity.TransactionStatus.COMPLETED,
              chargeId,
              3,
              groupId,
              requesterId);
        }
      } else {
        // Create new transaction with lock protection
        transaction = new TransactionEntity();
        transaction.setWorkflowId(workflowId);
        transaction.setWorkflowRunId(Workflow.getInfo().getRunId());
        transaction.setTransactionType(TransactionEntity.TransactionType.DEPOSIT);
        transaction.setAmount(BigDecimal.valueOf(amountDollars));
        transaction.setIdempotencyKey(idempotencyKey);
        transaction.setChargeId(chargeId);
        transaction.setStatus(TransactionEntity.TransactionStatus.COMPLETED);
        transaction.setScenario(scenario);

        String groupId = LockedTransactionRepository.deriveGroupId(transaction);
        entityActivities.saveTransaction(transaction, groupId, requesterId);
        log.info(
            "EntityWorkflow: Saved deposit transaction: {} with lock group: {}",
            transaction.getId(),
            groupId);
      }
    } catch (Exception e) {
      log.error("EntityWorkflow: Failed to persist deposit transaction", e);
      // Don't fail the workflow if database write fails, but log the error
    }

    return new ChargeResponseObj(chargeId);
  }

  @Override
  public String handleWithdrawTransaction(String workflowId, float amountDollars, String scenario) {
    log.info("EntityWorkflow: Handling withdraw transaction for workflowId: {}", workflowId);

    String requesterId = Workflow.getInfo().getWorkflowId() + "-" + Workflow.getInfo().getRunId();

    // Persist withdraw transaction to database with distributed locking
    try {
      TransactionEntity transaction = new TransactionEntity();
      transaction.setWorkflowId(workflowId);
      transaction.setWorkflowRunId(Workflow.getInfo().getRunId());
      transaction.setTransactionType(TransactionEntity.TransactionType.WITHDRAW);
      transaction.setAmount(BigDecimal.valueOf(amountDollars));
      transaction.setStatus(TransactionEntity.TransactionStatus.PENDING);
      transaction.setScenario(scenario);

      // Derive group ID for locking (use workflow ID as fallback)
      String groupId = LockedTransactionRepository.deriveGroupId(transaction);

      entityActivities.saveTransaction(transaction, groupId, requesterId);
      log.info(
          "EntityWorkflow: Saved withdraw transaction: {} with lock group: {}",
          transaction.getId(),
          groupId);

      // Update status to completed after successful withdraw (with lock protection)
      transaction.setStatus(TransactionEntity.TransactionStatus.COMPLETED);
      entityActivities.updateWithOptimisticLocking(transaction, groupId, requesterId);
      log.info("EntityWorkflow: Updated withdraw transaction to completed: {}", transaction.getId());

      return transaction.getId();
    } catch (Exception e) {
      log.error("EntityWorkflow: Failed to persist withdraw transaction", e);
      // Don't fail the workflow if database write fails, but log the error
      return null;
    }
  }

  @Override
  public String handleUndoWithdrawTransaction(String workflowId, float amountDollars) {
    log.info("EntityWorkflow: Handling undo withdraw transaction for workflowId: {}", workflowId);

    String requesterId = Workflow.getInfo().getWorkflowId() + "-" + Workflow.getInfo().getRunId();

    // Persist undo withdraw transaction to database with distributed locking
    try {
      TransactionEntity transaction = new TransactionEntity();
      transaction.setWorkflowId(workflowId);
      transaction.setWorkflowRunId(Workflow.getInfo().getRunId());
      transaction.setTransactionType(TransactionEntity.TransactionType.UNDO_WITHDRAW);
      transaction.setAmount(BigDecimal.valueOf(amountDollars));
      transaction.setStatus(TransactionEntity.TransactionStatus.COMPLETED);

      String groupId = LockedTransactionRepository.deriveGroupId(transaction);
      entityActivities.saveTransaction(transaction, groupId, requesterId);
      log.info(
          "EntityWorkflow: Saved undo withdraw transaction: {} with lock group: {}",
          transaction.getId(),
          groupId);
      return transaction.getId();
    } catch (Exception e) {
      log.error("EntityWorkflow: Failed to persist undo withdraw transaction", e);
      // Don't fail the workflow if database write fails, but log the error
      return null;
    }
  }
}

