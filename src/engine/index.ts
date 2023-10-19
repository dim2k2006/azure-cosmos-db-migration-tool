import dotenv from 'dotenv';
import { CosmosClient } from '@azure/cosmos';
import CosmosdbService from '../services/cosmosdb-service';
import engine from './engine';
import migrationConfig from '../migration';

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

  await engine(cosmosDbService)(migrationConfig);
};

main();
