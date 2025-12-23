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

import io.temporal.samples.moneytransfer.locking.LockManagerClient;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import javax.net.ssl.SSLException;

/**
 * Wrapper around TransactionRepository that uses Temporal lock manager workflows to ensure
 * serialized execution of database transactions based on group ID. This prevents concurrent
 * overrides when multiple transactions target the same resource (e.g., same account).
 */
public class LockedTransactionRepository {
  private final TransactionRepository transactionRepository;

  public LockedTransactionRepository() {
    this.transactionRepository = new TransactionRepository();
  }

  /**
   * Save a new transaction with lock protection based on group ID.
   *
   * @param transaction the transaction entity to save
   * @param groupId the group ID for locking (e.g., account ID)
   * @param requesterId identifier for the requester (e.g., workflow ID)
   * @return the saved transaction
   * @throws SQLException if database operation fails
   */
  public TransactionEntity save(TransactionEntity transaction, String groupId, String requesterId)
      throws SQLException {
    try {
      return LockManagerClient.executeWithLock(
          groupId,
          requesterId,
          () -> {
            return transactionRepository.save(transaction);
          });
    } catch (SQLException e) {
      throw e;
    } catch (FileNotFoundException | SSLException e) {
      throw new SQLException("Failed to connect to Temporal for lock management", e);
    } catch (Exception e) {
      throw new SQLException("Failed to execute with lock", e);
    }
  }

  /**
   * Update a transaction with optimistic locking and distributed lock protection.
   *
   * @param transaction the transaction entity to update
   * @param groupId the group ID for locking
   * @param requesterId identifier for the requester
   * @return true if update was successful, false if version conflict occurred
   * @throws SQLException if database operation fails
   */
  public boolean updateWithOptimisticLocking(
      TransactionEntity transaction, String groupId, String requesterId) throws SQLException {
    try {
      return LockManagerClient.executeWithLock(
          groupId,
          requesterId,
          () -> {
            return transactionRepository.updateWithOptimisticLocking(transaction);
          });
    } catch (SQLException e) {
      throw e;
    } catch (FileNotFoundException | SSLException e) {
      throw new SQLException("Failed to connect to Temporal for lock management", e);
    } catch (Exception e) {
      throw new SQLException("Failed to execute with lock", e);
    }
  }

  /**
   * Update a transaction with retry logic, protected by distributed lock.
   *
   * @param transactionId the ID of the transaction to update
   * @param updateFunction a function that modifies the transaction entity
   * @param maxRetries maximum number of retries on version conflict
   * @param groupId the group ID for locking
   * @param requesterId identifier for the requester
   * @return true if update was successful
   * @throws SQLException if database operation fails
   */
  public boolean updateWithRetry(
      String transactionId,
      TransactionRepository.TransactionUpdateFunction updateFunction,
      int maxRetries,
      String groupId,
      String requesterId)
      throws SQLException {
    try {
      return LockManagerClient.executeWithLock(
          groupId,
          requesterId,
          () -> {
            return transactionRepository.updateWithRetry(transactionId, updateFunction, maxRetries);
          });
    } catch (SQLException e) {
      throw e;
    } catch (FileNotFoundException | SSLException e) {
      throw new SQLException("Failed to connect to Temporal for lock management", e);
    } catch (Exception e) {
      throw new SQLException("Failed to execute with lock", e);
    }
  }

  /**
   * Find a transaction by ID (no lock needed for read-only operation, but can be locked for
   * consistency).
   *
   * @param id the transaction ID
   * @return Optional containing the transaction if found
   * @throws SQLException if database operation fails
   */
  public Optional<TransactionEntity> findById(String id) throws SQLException {
    return transactionRepository.findById(id);
  }

  /**
   * Find transactions by workflow ID (no lock needed for read-only operation).
   *
   * @param workflowId the workflow ID
   * @return list of transactions for the workflow
   * @throws SQLException if database operation fails
   */
  public List<TransactionEntity> findByWorkflowId(String workflowId) throws SQLException {
    return transactionRepository.findByWorkflowId(workflowId);
  }

  /**
   * Find a transaction by idempotency key (no lock needed for read-only operation).
   *
   * @param idempotencyKey the idempotency key
   * @return Optional containing the transaction if found
   * @throws SQLException if database operation fails
   */
  public Optional<TransactionEntity> findByIdempotencyKey(String idempotencyKey)
      throws SQLException {
    return transactionRepository.findByIdempotencyKey(idempotencyKey);
  }

  /**
   * Derives a group ID from transaction entity for locking purposes. Uses account IDs if
   * available, otherwise falls back to workflow ID.
   *
   * @param transaction the transaction entity
   * @return a group ID for locking
   */
  public static String deriveGroupId(TransactionEntity transaction) {
    // Use account-based grouping if available
    if (transaction.getFromAccount() != null && transaction.getToAccount() != null) {
      // Use both accounts as group ID to serialize transactions between the same pair
      return "account-pair:" + transaction.getFromAccount() + ":" + transaction.getToAccount();
    } else if (transaction.getFromAccount() != null) {
      return "account:" + transaction.getFromAccount();
    } else if (transaction.getToAccount() != null) {
      return "account:" + transaction.getToAccount();
    }
    // Fallback to workflow ID if no account info
    return "workflow:" + transaction.getWorkflowId();
  }
}

