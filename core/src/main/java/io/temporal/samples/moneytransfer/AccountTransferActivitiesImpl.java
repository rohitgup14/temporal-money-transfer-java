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

import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.activity.ActivityInfo;
import io.temporal.samples.moneytransfer.database.LockedTransactionRepository;
import io.temporal.samples.moneytransfer.database.TransactionEntity;
import io.temporal.samples.moneytransfer.dataclasses.ChargeResponseObj;
import io.temporal.samples.moneytransfer.dataclasses.ExecutionScenarioObj;
import io.temporal.samples.moneytransfer.web.ServerInfo;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccountTransferActivitiesImpl implements AccountTransferActivities {
  private static final Logger log = LoggerFactory.getLogger(AccountTransferActivitiesImpl.class);
  private final LockedTransactionRepository lockedTransactionRepository;

  public AccountTransferActivitiesImpl() {
    this.lockedTransactionRepository = new LockedTransactionRepository();
  }

  @Override
  public Boolean validate(ExecutionScenarioObj scenario) {
    log.info("\n\nAPI /validate\n");

    if (scenario == ExecutionScenarioObj.HUMAN_IN_LOOP) {
      return false;
    }

    if (scenario == ExecutionScenarioObj.STRESS_TEST) {
      ActivityExecutionContext ctx = Activity.getExecutionContext();
      ActivityInfo info = ctx.getInfo();
      if (info.getAttempt() <= 1) {
        stressCpuAndMemory(50 * 1024 * 1024); // 50 MB in bytes
      }
    }
    return true;
  }

  @Override
  public String withdraw(float amountDollars, ExecutionScenarioObj scenario) {
    log.info("\n\nAPI /withdraw amount = " + amountDollars + " \n");

    ActivityExecutionContext ctx = Activity.getExecutionContext();
    ActivityInfo info = ctx.getInfo();

    // Persist withdraw transaction to database with distributed locking
    try {
      TransactionEntity transaction = new TransactionEntity();
      transaction.setWorkflowId(info.getWorkflowId());
      // workflowRunId is optional - ActivityInfo doesn't provide it directly
      transaction.setWorkflowRunId(null);
      transaction.setTransactionType(TransactionEntity.TransactionType.WITHDRAW);
      transaction.setAmount(BigDecimal.valueOf(amountDollars));
      transaction.setStatus(TransactionEntity.TransactionStatus.PENDING);
      transaction.setScenario(scenario != null ? scenario.name() : null);
      
      // Derive group ID for locking (use workflow ID as fallback)
      String groupId = LockedTransactionRepository.deriveGroupId(transaction);
      String requesterId = info.getWorkflowId() + "-" + info.getActivityId();
      
      lockedTransactionRepository.save(transaction, groupId, requesterId);
      log.info("Saved withdraw transaction: {} with lock group: {}", transaction.getId(), groupId);
      
      // Update status to completed after successful withdraw (with lock protection)
      transaction.setStatus(TransactionEntity.TransactionStatus.COMPLETED);
      lockedTransactionRepository.updateWithOptimisticLocking(transaction, groupId, requesterId);
      log.info("Updated withdraw transaction to completed: {}", transaction.getId());
    } catch (SQLException e) {
      log.error("Failed to persist withdraw transaction", e);
      // Don't fail the activity if database write fails, but log the error
    } catch (Exception e) {
      log.error("Failed to acquire lock or persist withdraw transaction", e);
      // Don't fail the activity if lock acquisition fails, but log the error
    }

    if (scenario == ExecutionScenarioObj.API_DOWNTIME) {
      log.info("\n\n*** Simulating API Downtime\n");
      if (info.getAttempt() < 5) {
        log.info("\n*** Activity Attempt: #" + info.getAttempt() + "***\n");
        int delaySeconds = 7;
        log.info("\n\n/API/simulateDelay Seconds" + delaySeconds + "\n");
        simulateDelay(delaySeconds);
      }
    }

    return "SUCCESS";
  }

  @Override
  public ChargeResponseObj deposit(
      String idempotencyKey, float amountDollars, ExecutionScenarioObj scenario) {

    log.info("\n\nAPI /deposit amount = " + amountDollars + " \n");

    // Business logic validation (idempotency checking and locking are handled by EntityWorkflow)
    if (scenario == ExecutionScenarioObj.INVALID_ACCOUNT) {
      InvalidAccountException invalidAccountException =
          new InvalidAccountException("Invalid Account");
      throw Activity.wrap(invalidAccountException);
    }

    // Return charge response (transaction persistence with locking is handled by EntityWorkflow)
    ChargeResponseObj response = new ChargeResponseObj("example-charge-id");
    return response;
  }

  @Override
  public boolean undoWithdraw(float amountDollars) {
    log.info("\n\nAPI /undoWithdraw amount = " + amountDollars + " \n");

    ActivityExecutionContext ctx = Activity.getExecutionContext();
    ActivityInfo info = ctx.getInfo();

    // Persist undo withdraw transaction to database with distributed locking
    try {
      TransactionEntity transaction = new TransactionEntity();
      transaction.setWorkflowId(info.getWorkflowId());
      // workflowRunId is optional - ActivityInfo doesn't provide it directly
      transaction.setWorkflowRunId(null);
      transaction.setTransactionType(TransactionEntity.TransactionType.UNDO_WITHDRAW);
      transaction.setAmount(BigDecimal.valueOf(amountDollars));
      transaction.setStatus(TransactionEntity.TransactionStatus.COMPLETED);
      
      String groupId = LockedTransactionRepository.deriveGroupId(transaction);
      String requesterId = info.getWorkflowId() + "-" + info.getActivityId();
      lockedTransactionRepository.save(transaction, groupId, requesterId);
      log.info("Saved undo withdraw transaction: {} with lock group: {}", transaction.getId(), groupId);
    } catch (SQLException e) {
      log.error("Failed to persist undo withdraw transaction", e);
      // Don't fail the activity if database write fails, but log the error
    } catch (Exception e) {
      log.error("Failed to acquire lock or persist undo withdraw transaction", e);
      // Don't fail the activity if lock acquisition fails, but log the error
    }

    return true;
  }

  private static String simulateDelay(int seconds) {
    String url = ServerInfo.getWebServerURL() + "/simulateDelay?s=" + seconds;
    log.info("\n\n/API/simulateDelay URL: " + url + "\n");
    Request request = new Request.Builder().url(url).build();
    try (Response response = new OkHttpClient().newCall(request).execute()) {
      return response.body().string();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // InvalidAccountException
  public static class InvalidAccountException extends RuntimeException {
    public InvalidAccountException(String message) {
      super(message);
    }
  }

  public static void stressCpuAndMemory(int sizeInBytes) {
    // Allocate a byte array of the specified size
    byte[] memoryChunk = new byte[sizeInBytes];

    long startTime = System.currentTimeMillis();
    long duration = 30 * 1000; // 30 seconds in milliseconds

    // Perform CPU-intensive task for approximately 30 seconds
    while (System.currentTimeMillis() - startTime < duration) {
      // Fill the array with some data to simulate memory usage
      for (int i = 0; i < memoryChunk.length; i++) {
        memoryChunk[i] = (byte) (i % 256);
      }

      // Simple CPU work: sum up all elements in the array
      long sum = 0;
      for (byte b : memoryChunk) {
        sum += b;
      }

      // Print the sum to avoid the compiler optimizing away the loop
      System.out.println("Current sum: " + sum);
    }

    System.out.println("Finished stressing CPU and memory.");
  }
}
