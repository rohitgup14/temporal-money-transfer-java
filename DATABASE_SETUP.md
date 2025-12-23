# Database Setup Guide

This project uses PostgreSQL to persist all money transfer transactions with support for concurrent updates using the Temporal Entity pattern (optimistic locking with version field).

## Database Configuration

The database connection is configured via environment variables:

- `DB_URL` - PostgreSQL connection URL (default: `jdbc:postgresql://localhost:5432/temporal_moneytransfer`)
- `DB_USER` - Database username (default: `postgres`)
- `DB_PASSWORD` - Database password (default: `postgres`)

## Quick Start with Docker

1. Start PostgreSQL using Docker Compose:
   ```bash
   docker-compose -f docker/postgres-compose.yaml up -d
   ```

2. The database schema will be automatically created when the application starts.

## Manual Setup

1. Install PostgreSQL (if not already installed)

2. Create a database:
   ```sql
   CREATE DATABASE temporal_moneytransfer;
   ```

3. Set environment variables:
   ```bash
   export DB_URL=jdbc:postgresql://localhost:5432/temporal_moneytransfer
   export DB_USER=postgres
   export DB_PASSWORD=your_password
   ```

4. Run the application - the schema will be automatically created on first connection.

## Temporal Entity Pattern

The transactions table uses optimistic locking with a `version` field to handle concurrent updates:

- Each transaction has a `version` field that starts at 0
- When updating a transaction, the version is checked
- If the version matches, the update succeeds and version is incremented
- If the version doesn't match (concurrent update), the update fails and can be retried

This ensures that concurrent updates to the same transaction are handled safely without explicit database locks.

## Transaction Types

The following transaction types are persisted:

- `WITHDRAW` - Withdrawal from source account
- `DEPOSIT` - Deposit to destination account
- `UNDO_WITHDRAW` - Rollback of a withdrawal

Each transaction includes:
- Workflow ID (links transaction to the workflow execution)
- Amount
- Status (PENDING, COMPLETED, FAILED, ROLLED_BACK)
- Idempotency key (for deposit operations)
- Charge ID (for completed deposits)
- Version (for optimistic locking)

