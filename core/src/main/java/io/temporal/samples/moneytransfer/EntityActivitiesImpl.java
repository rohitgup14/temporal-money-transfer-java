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

import io.temporal.samples.moneytransfer.database.LockedTransactionRepository;
import io.temporal.samples.moneytransfer.database.TransactionEntity;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityActivitiesImpl implements EntityActivities {
  private static final Logger log = LoggerFactory.getLogger(EntityActivitiesImpl.class);
  private final LockedTransactionRepository lockedTransactionRepository;

  public EntityActivitiesImpl() {
    this.lockedTransactionRepository = new LockedTransactionRepository();
  }

  @Override
  public TransactionEntity findByIdempotencyKey(String idempotencyKey) {
    try {
      return lockedTransactionRepository.findByIdempotencyKey(idempotencyKey).orElse(null);
    } catch (SQLException e) {
      log.error("Failed to find transaction by idempotency key: {}", idempotencyKey, e);
      return null;
    }
  }

  @Override
  public TransactionEntity saveTransaction(
      TransactionEntity transaction, String groupId, String requesterId) {
    try {
      return lockedTransactionRepository.save(transaction, groupId, requesterId);
    } catch (SQLException e) {
      log.error(
          "Failed to save transaction: {} with groupId: {}, requesterId: {}",
          transaction.getId(),
          groupId,
          requesterId,
          e);
      throw new RuntimeException("Failed to save transaction", e);
    }
  }

  @Override
  public boolean updateWithOptimisticLocking(
      TransactionEntity transaction, String groupId, String requesterId) {
    try {
      return lockedTransactionRepository.updateWithOptimisticLocking(
          transaction, groupId, requesterId);
    } catch (SQLException e) {
      log.error(
          "Failed to update transaction: {} with groupId: {}, requesterId: {}",
          transaction.getId(),
          groupId,
          requesterId,
          e);
      throw new RuntimeException("Failed to update transaction", e);
    }
  }

  @Override
  public boolean updateWithRetry(
      String transactionId,
      TransactionEntity.TransactionStatus status,
      String chargeId,
      int maxRetries,
      String groupId,
      String requesterId) {
    try {
      return lockedTransactionRepository.updateWithRetry(
          transactionId,
          t -> {
            t.setStatus(status);
            t.setChargeId(chargeId);
          },
          maxRetries,
          groupId,
          requesterId);
    } catch (SQLException e) {
      log.error(
          "Failed to update transaction with retry: {} with groupId: {}, requesterId: {}",
          transactionId,
          groupId,
          requesterId,
          e);
      throw new RuntimeException("Failed to update transaction with retry", e);
    }
  }
}

