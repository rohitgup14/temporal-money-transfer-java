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

import io.temporal.samples.moneytransfer.dataclasses.ChargeResponseObj;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface EntityWorkflow {
  /**
   * Handles deposit transaction with idempotency checking and locking logic.
   * Checks for existing transaction with same idempotency key and returns it if completed.
   * Otherwise, creates or updates transaction with distributed locking.
   *
   * @param idempotencyKey the idempotency key for the transaction
   * @param workflowId the workflow ID
   * @param amountDollars the deposit amount
   * @param chargeId the charge ID
   * @param scenario the execution scenario
   * @return ChargeResponseObj containing the charge ID
   */
  @WorkflowMethod(name = "entityDepositTransaction")
  ChargeResponseObj handleDepositTransaction(
      String idempotencyKey,
      String workflowId,
      float amountDollars,
      String chargeId,
      String scenario);

  /**
   * Handles withdraw transaction with locking logic.
   *
   * @param workflowId the workflow ID
   * @param amountDollars the withdraw amount
   * @param scenario the execution scenario
   * @return the transaction ID
   */
  @WorkflowMethod(name = "entityWithdrawTransaction")
  String handleWithdrawTransaction(String workflowId, float amountDollars, String scenario);

  /**
   * Handles undo withdraw transaction with locking logic.
   *
   * @param workflowId the workflow ID
   * @param amountDollars the undo withdraw amount
   * @return the transaction ID
   */
  @WorkflowMethod(name = "entityUndoWithdrawTransaction")
  String handleUndoWithdrawTransaction(String workflowId, float amountDollars);
}

