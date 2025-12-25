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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseConfig {
  private static final Logger log = LoggerFactory.getLogger(DatabaseConfig.class);
  private static volatile DataSource dataSource;
  private static volatile boolean initialized = false;

  public static DataSource getDataSource() {
    if (dataSource == null) {
      synchronized (DatabaseConfig.class) {
        if (dataSource == null) {
          initializeDataSource();
        }
      }
    }
    return dataSource;
  }

  private static void initializeDataSource() {
    HikariConfig config = new HikariConfig();
    
    // Get database connection properties from environment variables with defaults
    String dbUrl = System.getenv("DB_URL");
    if (dbUrl == null || dbUrl.isEmpty()) {
      dbUrl = "jdbc:postgresql://localhost:5432/temporal_moneytransfer";
    }
    
    String dbUser = System.getenv("DB_USER");
    if (dbUser == null || dbUser.isEmpty()) {
      dbUser = "postgres";
    }
    
    String dbPassword = System.getenv("DB_PASSWORD");
    if (dbPassword == null || dbPassword.isEmpty()) {
      dbPassword = "postgres";
    }
    
    config.setJdbcUrl(dbUrl);
    config.setUsername(dbUser);
    config.setPassword(dbPassword);
    config.setDriverClassName("org.postgresql.Driver");
    
    // Connection pool settings
    config.setMaximumPoolSize(10);
    config.setMinimumIdle(2);
    config.setConnectionTimeout(30000);
    config.setIdleTimeout(600000);
    config.setMaxLifetime(1800000);
    
    // Connection test
    config.setConnectionTestQuery("SELECT 1");

    dataSource = new HikariDataSource(config);
    log.info("Database connection pool initialized: {}", dbUrl);

    // Initialize schema on first access
    if (!initialized) {
      initializeSchema();
      initialized = true;
    }
  }

  private static void initializeSchema() {
    try (Connection conn = dataSource.getConnection()) {
      // Create transactions table with Temporal Entity pattern (version field)
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
      
      // Create index on workflow_id for faster queries
      String createIndexSQL = 
          "CREATE INDEX IF NOT EXISTS idx_transactions_workflow_id ON transactions(workflow_id)";
      conn.createStatement().execute(createIndexSQL);
      
      // Create index on idempotency_key for idempotency checks
      String createIdempotencyIndexSQL = 
          "CREATE INDEX IF NOT EXISTS idx_transactions_idempotency_key ON transactions(idempotency_key)";
      conn.createStatement().execute(createIdempotencyIndexSQL);
      
      log.info("Database schema initialized successfully");
    } catch (SQLException e) {
      log.error("Failed to initialize database schema", e);
      throw new RuntimeException("Database initialization failed", e);
    }
  }

  public static Connection getConnection() throws SQLException {
    return getDataSource().getConnection();
  }

  public static void close() {
    if (dataSource instanceof HikariDataSource) {
      ((HikariDataSource) dataSource).close();
      log.info("Database connection pool closed");
    }
  }
}

