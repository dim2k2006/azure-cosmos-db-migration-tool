# Azure Cosmos DB Data Migration Tool

## Installation

1.`git clone git@github.com:ombori/azure-cosmos-db-migration-tool.git`

2.`make init`

3. Navigate to `.env` file that is created by `make init` command and fill in azure cosmos db information.

- COSMOSDB_CONNECTION - azure cosmosDb connection string
- COSMOSDB_DATABASE - azure cosmosDb database name
- COSMOSDB_CONTAINER - azure cosmosDb container name

## Usage

This migration tool supports 3 types of data migration:

- create new data
- update existing data
- delete existing data

1. Navigate to `src/migration.ts` file. This file is created by `make init` command.

2. Describe your migration logic using MigrationConfig type.

3. Run `make start` to perform migration.

