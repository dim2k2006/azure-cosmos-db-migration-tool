import dotenv from 'dotenv';
import { CosmosClient, SqlQuerySpec } from '@azure/cosmos';
import get from 'lodash/get';
import CosmosdbService from './cosmosdb-service';
import engine, { OperationType } from './engine';

dotenv.config();

const cosmosDbConnection = process.env.COSMOSDB_CONNECTION as string;
const cosmosDbDatabase = process.env.COSMOSDB_DATABASE as string;
const cosmosDbContainer = process.env.COSMOSDB_CONTAINER as string;

const getCosmosDbService = () => {
  const client = new CosmosClient(cosmosDbConnection);
  const database = client.database(cosmosDbDatabase);
  const container = database.container(cosmosDbContainer);

  return new CosmosdbService({ container });
};

const main = async () => {
  const cosmosDbService = getCosmosDbService();

  const selectFn = (): SqlQuerySpec => ({
    query: `SELECT c.id, c.tenantId FROM c WHERE c.type = 'appProductsByDay' OFFSET 0 LIMIT 10000`,
  });

  const partitionKeySelectFn = (document: unknown): string =>
    get(document, 'tenantId', '');

  await engine({
    operationType: OperationType.Delete,
    selectFn,
    partitionKeySelectFn,
    cosmosDbService,
  });
};

main();
