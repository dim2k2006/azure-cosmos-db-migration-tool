import dotenv from 'dotenv';
import { CosmosClient } from '@azure/cosmos';
import CosmosdbService from '../services/cosmosdb-service';
import engine from './engine';
import migrationConfig from '../migration';

dotenv.config();

type Config = {
  cosmosDbConnection: string;
  cosmosDbDatabase: string;
  cosmosDbContainer: string;

  inputCosmosDbConnection: string;
  inputCosmosDbDatabase: string;
  inputCosmosDbContainer: string;
}

const getConfig = (): Config => {
  const cosmosDbConnection = process.env.COSMOSDB_CONNECTION as string;
  const cosmosDbDatabase = process.env.COSMOSDB_DATABASE as string;
  const cosmosDbContainer = process.env.COSMOSDB_CONTAINER as string;

  const inputCosmosDbConnection = process.env.INPUT_COSMOSDB_CONNECTION as string;
  const inputCosmosDbDatabase = process.env.INPUT_COSMOSDB_DATABASE as string;
  const inputCosmosDbContainer = process.env.INPUT_COSMOSDB_CONTAINER as string;

  return {
    cosmosDbConnection,
    cosmosDbDatabase,
    cosmosDbContainer,
    inputCosmosDbConnection,
    inputCosmosDbDatabase,
    inputCosmosDbContainer,
  };
}

type GetCosmosDbServiceProps = {
  cosmosDbConnection: string;
  cosmosDbDatabase: string;
  cosmosDbContainer: string;
}

const getCosmosDbService = ({cosmosDbConnection, cosmosDbDatabase, cosmosDbContainer}: GetCosmosDbServiceProps) => {
  const client = new CosmosClient(cosmosDbConnection);
  const database = client.database(cosmosDbDatabase);
  const container = database.container(cosmosDbContainer);

  return new CosmosdbService({ container });
};

const main = async () => {
  const config = getConfig();

  const cosmosDbService = getCosmosDbService({
    cosmosDbConnection: config.cosmosDbConnection,
    cosmosDbDatabase: config.cosmosDbDatabase,
    cosmosDbContainer: config.cosmosDbContainer,
  });

  const inputCosmosDbService = getCosmosDbService({
    cosmosDbConnection: config.inputCosmosDbConnection,
    cosmosDbDatabase: config.inputCosmosDbDatabase,
    cosmosDbContainer: config.inputCosmosDbContainer,
  });

  await engine(cosmosDbService, inputCosmosDbService)(migrationConfig);
};

main();
