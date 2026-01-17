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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repository for TransactionEntity that implements Temporal Entity pattern with optimistic
 * locking. Concurrent updates are handled using version-based optimistic locking.
 */
public class TransactionRepository {
  private static final Logger log = LoggerFactory.getLogger(TransactionRepository.class);

  /**
   * Save a new transaction to the database.
   *
   * @param transaction the transaction entity to save
   * @return the saved transaction with generated ID and timestamps
   * @throws SQLException if database operation fails
   */
  public TransactionEntity save(TransactionEntity transaction) throws SQLException {
    String sql =
        "INSERT INTO transactions (id, workflow_id, workflow_run_id, transaction_type, amount, "
            + "from_account, to_account, idempotency_key, charge_id, status, scenario, version, "
            + "created_at, updated_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    try (Connection conn = DatabaseConfig.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      Timestamp now = new Timestamp(System.currentTimeMillis());
      
      stmt.setString(1, transaction.getId());
      stmt.setString(2, transaction.getWorkflowId());
      stmt.setString(3, transaction.getWorkflowRunId());
      stmt.setString(4, transaction.getTransactionType().name());
      stmt.setBigDecimal(5, transaction.getAmount());
      stmt.setString(6, transaction.getFromAccount());
      stmt.setString(7, transaction.getToAccount());
      stmt.setString(8, transaction.getIdempotencyKey());
      stmt.setString(9, transaction.getChargeId());
      stmt.setString(10, transaction.getStatus().name());
      stmt.setString(11, transaction.getScenario());
      stmt.setLong(12, transaction.getVersion());
      stmt.setTimestamp(13, now);
      stmt.setTimestamp(14, now);

      transaction.setCreatedAt(now);
      transaction.setUpdatedAt(now);

      stmt.executeUpdate();
      log.debug("Saved transaction: {}", transaction.getId());
      return transaction;
    }
  }

  /**
   * Update a transaction using optimistic locking (Temporal Entity pattern). This method checks
   * the version field and only updates if the version matches, preventing lost updates from
   * concurrent modifications.
   *
   * @param transaction the transaction entity to update
   * @return true if update was successful, false if version conflict occurred
   * @throws SQLException if database operation fails
   */
  public boolean updateWithOptimisticLocking(TransactionEntity transaction) throws SQLException {
    String sql =
        "UPDATE transactions SET workflow_id=?, workflow_run_id=?, transaction_type=?, amount=?, "
            + "from_account=?, to_account=?, idempotency_key=?, charge_id=?, status=?, scenario=?, "
            + "version=version+1, updated_at=? "
            + "WHERE id=? AND version=?";

    try (Connection conn = DatabaseConfig.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      Timestamp now = new Timestamp(System.currentTimeMillis());
      long expectedVersion = transaction.getVersion();

      stmt.setString(1, transaction.getWorkflowId());
      stmt.setString(2, transaction.getWorkflowRunId());
      stmt.setString(3, transaction.getTransactionType().name());
      stmt.setBigDecimal(4, transaction.getAmount());
      stmt.setString(5, transaction.getFromAccount());
      stmt.setString(6, transaction.getToAccount());
      stmt.setString(7, transaction.getIdempotencyKey());
      stmt.setString(8, transaction.getChargeId());
      stmt.setString(9, transaction.getStatus().name());
      stmt.setString(10, transaction.getScenario());
      stmt.setTimestamp(11, now);
      stmt.setString(12, transaction.getId());
      stmt.setLong(13, expectedVersion);

      int rowsAffected = stmt.executeUpdate();

      if (rowsAffected == 0) {
        log.warn(
            "Update failed due to version conflict for transaction: {} (expected version: {})",
            transaction.getId(),
            expectedVersion);
        return false;
      }

      // Update the version in the entity object
      transaction.setVersion(expectedVersion + 1);
      transaction.setUpdatedAt(now);
      log.debug("Updated transaction: {} (new version: {})", transaction.getId(), transaction.getVersion());
      return true;
    }
  }

  /**
   * Update a transaction with retry logic for optimistic locking conflicts. This is useful when
   * handling concurrent updates.
   *
   * @param transactionId the ID of the transaction to update
   * @param updateFunction a function that modifies the transaction entity
   * @param maxRetries maximum number of retries on version conflict
   * @return true if update was successful, false if max retries exceeded
   * @throws SQLException if database operation fails
   */
  public boolean updateWithRetry(
      String transactionId, TransactionUpdateFunction updateFunction, int maxRetries)
      throws SQLException {
    for (int attempt = 0; attempt < maxRetries; attempt++) {
      Optional<TransactionEntity> transactionOpt = findById(transactionId);
      if (!transactionOpt.isPresent()) {
        log.warn("Transaction not found: {}", transactionId);
        return false;
      }

      TransactionEntity transaction = transactionOpt.get();
      updateFunction.apply(transaction);

      if (updateWithOptimisticLocking(transaction)) {
        return true;
      }

      // Version conflict - retry
      log.debug("Version conflict on attempt {} for transaction: {}", attempt + 1, transactionId);
    }

    log.error("Failed to update transaction after {} retries: {}", maxRetries, transactionId);
    return false;
  }

  /**
   * Find a transaction by ID.
   *
   * @param id the transaction ID
   * @return Optional containing the transaction if found
   * @throws SQLException if database operation fails
   */
  public Optional<TransactionEntity> findById(String id) throws SQLException {
    String sql = "SELECT * FROM transactions WHERE id=?";

    try (Connection conn = DatabaseConfig.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, id);

      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return Optional.of(mapResultSetToEntity(rs));
        }
      }
    }

    return Optional.empty();
  }

  /**
   * Find transactions by workflow ID.
   *
   * @param workflowId the workflow ID
   * @return list of transactions for the workflow
   * @throws SQLException if database operation fails
   */
  public List<TransactionEntity> findByWorkflowId(String workflowId) throws SQLException {
    String sql = "SELECT * FROM transactions WHERE workflow_id=? ORDER BY created_at ASC";
    List<TransactionEntity> transactions = new ArrayList<>();

    try (Connection conn = DatabaseConfig.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);

      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          transactions.add(mapResultSetToEntity(rs));
        }
      }
    }

    return transactions;
  }

  /**
   * Find a transaction by idempotency key.
   *
   * @param idempotencyKey the idempotency key
   * @return Optional containing the transaction if found
   * @throws SQLException if database operation fails
   */
  public Optional<TransactionEntity> findByIdempotencyKey(String idempotencyKey)
      throws SQLException {
    String sql = "SELECT * FROM transactions WHERE idempotency_key=?";

    try (Connection conn = DatabaseConfig.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, idempotencyKey);

      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return Optional.of(mapResultSetToEntity(rs));
        }
      }
    }

    return Optional.empty();
  }

  private TransactionEntity mapResultSetToEntity(ResultSet rs) throws SQLException {
    TransactionEntity entity = new TransactionEntity();
    entity.setId(rs.getString("id"));
    entity.setWorkflowId(rs.getString("workflow_id"));
    entity.setWorkflowRunId(rs.getString("workflow_run_id"));
    entity.setTransactionType(
        TransactionEntity.TransactionType.valueOf(rs.getString("transaction_type")));
    entity.setAmount(rs.getBigDecimal("amount"));
    entity.setFromAccount(rs.getString("from_account"));
    entity.setToAccount(rs.getString("to_account"));
    entity.setIdempotencyKey(rs.getString("idempotency_key"));
    entity.setChargeId(rs.getString("charge_id"));
    entity.setStatus(TransactionEntity.TransactionStatus.valueOf(rs.getString("status")));
    entity.setScenario(rs.getString("scenario"));
    entity.setVersion(rs.getLong("version"));
    entity.setCreatedAt(rs.getTimestamp("created_at"));
    entity.setUpdatedAt(rs.getTimestamp("updated_at"));
    return entity;
  }

  /**
   * Functional interface for updating transaction entities. Used in updateWithRetry method.
   */
  @FunctionalInterface
  public interface TransactionUpdateFunction {
    void apply(TransactionEntity transaction);
  }
}

