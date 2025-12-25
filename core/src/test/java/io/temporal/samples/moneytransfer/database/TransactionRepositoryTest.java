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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for TransactionRepository using H2 in-memory database for testing.
 */
public class TransactionRepositoryTest {

  private TransactionRepository repository;
  private DataSource testDataSource;
  private DataSource originalDataSource;

  @Before
  public void setUp() throws Exception {
    // Create H2 in-memory database for testing
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
    config.setDriverClassName("org.h2.Driver");
    config.setMaximumPoolSize(5);
    testDataSource = new HikariDataSource(config);

    // Initialize schema
    try (Connection conn = testDataSource.getConnection()) {
      createSchema(conn);
    }

    // Replace DatabaseConfig's dataSource with test dataSource using reflection
    Field dataSourceField = DatabaseConfig.class.getDeclaredField("dataSource");
    dataSourceField.setAccessible(true);
    originalDataSource = (DataSource) dataSourceField.get(null);
    dataSourceField.set(null, testDataSource);

    // Reset initialized flag
    Field initializedField = DatabaseConfig.class.getDeclaredField("initialized");
    initializedField.setAccessible(true);
    initializedField.setBoolean(null, true);

    repository = new TransactionRepository();
  }

  @After
  public void tearDown() throws Exception {
    // Restore original dataSource
    Field dataSourceField = DatabaseConfig.class.getDeclaredField("dataSource");
    dataSourceField.setAccessible(true);
    dataSourceField.set(null, originalDataSource);

    // Reset initialized flag
    Field initializedField = DatabaseConfig.class.getDeclaredField("initialized");
    initializedField.setAccessible(true);
    initializedField.setBoolean(null, false);

    // Close test datasource
    if (testDataSource instanceof HikariDataSource) {
      ((HikariDataSource) testDataSource).close();
    }
  }

  private void createSchema(Connection conn) throws SQLException {
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS transactions ("
            + "id VARCHAR(255) PRIMARY KEY, "
            + "workflow_id VARCHAR(255) NOT NULL, "
            + "workflow_run_id VARCHAR(255), "
            + "transaction_type VARCHAR(50) NOT NULL, "
            + "amount DECIMAL(19,2) NOT NULL, "
            + "from_account VARCHAR(255), "
            + "to_account VARCHAR(255), "
            + "idempotency_key VARCHAR(255), "
            + "charge_id VARCHAR(255), "
            + "status VARCHAR(50) NOT NULL, "
            + "scenario VARCHAR(50), "
            + "version BIGINT NOT NULL DEFAULT 0, "
            + "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            + "updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
            + ")";

    conn.createStatement().execute(createTableSQL);

    String createIndexSQL =
        "CREATE INDEX IF NOT EXISTS idx_transactions_workflow_id ON transactions(workflow_id)";
    conn.createStatement().execute(createIndexSQL);

    String createIdempotencyIndexSQL =
        "CREATE INDEX IF NOT EXISTS idx_transactions_idempotency_key ON transactions(idempotency_key)";
    conn.createStatement().execute(createIdempotencyIndexSQL);
  }

  @Test
  public void testSaveTransaction() throws SQLException {
    TransactionEntity transaction = createTestTransaction();
    transaction.setId(UUID.randomUUID().toString());

    TransactionEntity saved = repository.save(transaction);

    assertNotNull(saved);
    assertEquals(transaction.getId(), saved.getId());
    assertEquals(transaction.getWorkflowId(), saved.getWorkflowId());
    assertNotNull(saved.getCreatedAt());
    assertNotNull(saved.getUpdatedAt());
    assertEquals(0, saved.getVersion());

    // Verify it was actually saved
    Optional<TransactionEntity> found = repository.findById(transaction.getId());
    assertTrue(found.isPresent());
    assertEquals(transaction.getId(), found.get().getId());
  }

  @Test
  public void testFindById() throws SQLException {
    TransactionEntity transaction = createTestTransaction();
    transaction.setId(UUID.randomUUID().toString());
    repository.save(transaction);

    Optional<TransactionEntity> found = repository.findById(transaction.getId());
    assertTrue(found.isPresent());
    assertEquals(transaction.getId(), found.get().getId());
    assertEquals(transaction.getAmount(), found.get().getAmount());

    // Test non-existent ID
    Optional<TransactionEntity> notFound = repository.findById("non-existent-id");
    assertFalse(notFound.isPresent());
  }

  @Test
  public void testFindByWorkflowId() throws SQLException {
    String workflowId = "workflow-" + UUID.randomUUID().toString();

    // Create multiple transactions for same workflow
    TransactionEntity tx1 = createTestTransaction();
    tx1.setId(UUID.randomUUID().toString());
    tx1.setWorkflowId(workflowId);
    repository.save(tx1);

    TransactionEntity tx2 = createTestTransaction();
    tx2.setId(UUID.randomUUID().toString());
    tx2.setWorkflowId(workflowId);
    tx2.setTransactionType(TransactionEntity.TransactionType.DEPOSIT);
    repository.save(tx2);

    // Create transaction for different workflow
    TransactionEntity tx3 = createTestTransaction();
    tx3.setId(UUID.randomUUID().toString());
    tx3.setWorkflowId("different-workflow");
    repository.save(tx3);

    List<TransactionEntity> transactions = repository.findByWorkflowId(workflowId);
    assertEquals(2, transactions.size());
    assertTrue(transactions.stream().allMatch(t -> workflowId.equals(t.getWorkflowId())));
  }

  @Test
  public void testFindByIdempotencyKey() throws SQLException {
    String idempotencyKey = "idempotency-key-" + UUID.randomUUID().toString();

    TransactionEntity transaction = createTestTransaction();
    transaction.setId(UUID.randomUUID().toString());
    transaction.setIdempotencyKey(idempotencyKey);
    repository.save(transaction);

    Optional<TransactionEntity> found = repository.findByIdempotencyKey(idempotencyKey);
    assertTrue(found.isPresent());
    assertEquals(idempotencyKey, found.get().getIdempotencyKey());

    // Test non-existent key
    Optional<TransactionEntity> notFound = repository.findByIdempotencyKey("non-existent-key");
    assertFalse(notFound.isPresent());
  }

  @Test
  public void testUpdateWithOptimisticLocking() throws SQLException {
    TransactionEntity transaction = createTestTransaction();
    transaction.setId(UUID.randomUUID().toString());
    repository.save(transaction);

    // Update transaction
    transaction.setStatus(TransactionEntity.TransactionStatus.COMPLETED);
    boolean updated = repository.updateWithOptimisticLocking(transaction);
    assertTrue(updated);
    assertEquals(1, transaction.getVersion());

    // Verify update
    Optional<TransactionEntity> found = repository.findById(transaction.getId());
    assertTrue(found.isPresent());
    assertEquals(TransactionEntity.TransactionStatus.COMPLETED, found.get().getStatus());
    assertEquals(1, found.get().getVersion());

    // Try to update with old version (should fail)
    transaction.setVersion(0);
    transaction.setStatus(TransactionEntity.TransactionStatus.FAILED);
    boolean updateFailed = repository.updateWithOptimisticLocking(transaction);
    assertFalse(updateFailed);

    // Verify status didn't change
    found = repository.findById(transaction.getId());
    assertEquals(TransactionEntity.TransactionStatus.COMPLETED, found.get().getStatus());
  }

  @Test
  public void testUpdateWithRetry() throws SQLException {
    TransactionEntity transaction = createTestTransaction();
    transaction.setId(UUID.randomUUID().toString());
    repository.save(transaction);

    // Update with retry
    boolean success =
        repository.updateWithRetry(
            transaction.getId(),
            t -> {
              t.setStatus(TransactionEntity.TransactionStatus.COMPLETED);
              t.setChargeId("charge-123");
            },
            3);

    assertTrue(success);

    // Verify update
    Optional<TransactionEntity> found = repository.findById(transaction.getId());
    assertTrue(found.isPresent());
    assertEquals(TransactionEntity.TransactionStatus.COMPLETED, found.get().getStatus());
    assertEquals("charge-123", found.get().getChargeId());
  }

  private TransactionEntity createTestTransaction() {
    TransactionEntity transaction = new TransactionEntity();
    transaction.setWorkflowId("workflow-" + UUID.randomUUID().toString());
    transaction.setTransactionType(TransactionEntity.TransactionType.WITHDRAW);
    transaction.setAmount(BigDecimal.valueOf(100.50));
    transaction.setStatus(TransactionEntity.TransactionStatus.PENDING);
    return transaction;
  }
}
