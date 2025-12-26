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

import io.temporal.activity.ActivityInterface;
import io.temporal.samples.moneytransfer.database.TransactionEntity;

@ActivityInterface
public interface EntityActivities {
  /**
   * Find a transaction by idempotency key.
   *
   * @param idempotencyKey the idempotency key
   * @return the transaction entity if found, null otherwise
   */
  TransactionEntity findByIdempotencyKey(String idempotencyKey);

  /**
   * Save a transaction with lock protection.
   *
   * @param transaction the transaction entity to save
   * @param groupId the group ID for locking
   * @param requesterId identifier for the requester
   * @return the saved transaction
   */
  TransactionEntity saveTransaction(
      TransactionEntity transaction, String groupId, String requesterId);

  /**
   * Update a transaction with optimistic locking and distributed lock protection.
   *
   * @param transaction the transaction entity to update
   * @param groupId the group ID for locking
   * @param requesterId identifier for the requester
   * @return true if update was successful, false if version conflict occurred
   */
  boolean updateWithOptimisticLocking(
      TransactionEntity transaction, String groupId, String requesterId);

  /**
   * Update a transaction with retry logic, protected by distributed lock.
   *
   * @param transactionId the ID of the transaction to update
   * @param status the new status
   * @param chargeId the new charge ID
   * @param maxRetries maximum number of retries on version conflict
   * @param groupId the group ID for locking
   * @param requesterId identifier for the requester
   * @return true if update was successful
   */
  boolean updateWithRetry(
      String transactionId,
      TransactionEntity.TransactionStatus status,
      String chargeId,
      int maxRetries,
      String groupId,
      String requesterId);
}

