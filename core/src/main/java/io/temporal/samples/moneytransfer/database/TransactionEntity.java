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

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.UUID;

/**
 * Transaction entity following Temporal Entity pattern with optimistic locking using version
 * field. This allows concurrent updates to be handled safely.
 */
public class TransactionEntity {
  private String id;
  private String workflowId;
  private String workflowRunId;
  private TransactionType transactionType;
  private BigDecimal amount;
  private String fromAccount;
  private String toAccount;
  private String idempotencyKey;
  private String chargeId;
  private TransactionStatus status;
  private String scenario;
  private long version; // Used for optimistic locking (Temporal Entity pattern)
  private Timestamp createdAt;
  private Timestamp updatedAt;

  public enum TransactionType {
    WITHDRAW,
    DEPOSIT,
    UNDO_WITHDRAW
  }

  public enum TransactionStatus {
    PENDING,
    COMPLETED,
    FAILED,
    ROLLED_BACK
  }

  public TransactionEntity() {
    this.id = UUID.randomUUID().toString();
    this.version = 0;
  }

  public TransactionEntity(
      String workflowId,
      String workflowRunId,
      TransactionType transactionType,
      BigDecimal amount,
      TransactionStatus status) {
    this();
    this.workflowId = workflowId;
    this.workflowRunId = workflowRunId;
    this.transactionType = transactionType;
    this.amount = amount;
    this.status = status;
  }

  // Getters and Setters
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getWorkflowId() {
    return workflowId;
  }

  public void setWorkflowId(String workflowId) {
    this.workflowId = workflowId;
  }

  public String getWorkflowRunId() {
    return workflowRunId;
  }

  public void setWorkflowRunId(String workflowRunId) {
    this.workflowRunId = workflowRunId;
  }

  public TransactionType getTransactionType() {
    return transactionType;
  }

  public void setTransactionType(TransactionType transactionType) {
    this.transactionType = transactionType;
  }

  public BigDecimal getAmount() {
    return amount;
  }

  public void setAmount(BigDecimal amount) {
    this.amount = amount;
  }

  public String getFromAccount() {
    return fromAccount;
  }

  public void setFromAccount(String fromAccount) {
    this.fromAccount = fromAccount;
  }

  public String getToAccount() {
    return toAccount;
  }

  public void setToAccount(String toAccount) {
    this.toAccount = toAccount;
  }

  public String getIdempotencyKey() {
    return idempotencyKey;
  }

  public void setIdempotencyKey(String idempotencyKey) {
    this.idempotencyKey = idempotencyKey;
  }

  public String getChargeId() {
    return chargeId;
  }

  public void setChargeId(String chargeId) {
    this.chargeId = chargeId;
  }

  public TransactionStatus getStatus() {
    return status;
  }

  public void setStatus(TransactionStatus status) {
    this.status = status;
  }

  public String getScenario() {
    return scenario;
  }

  public void setScenario(String scenario) {
    this.scenario = scenario;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public Timestamp getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Timestamp createdAt) {
    this.createdAt = createdAt;
  }

  public Timestamp getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(Timestamp updatedAt) {
    this.updatedAt = updatedAt;
  }

  @Override
  public String toString() {
    return "TransactionEntity{"
        + "id='"
        + id
        + '\''
        + ", workflowId='"
        + workflowId
        + '\''
        + ", transactionType="
        + transactionType
        + ", amount="
        + amount
        + ", status="
        + status
        + ", version="
        + version
        + '}';
  }
}

