import { BulkOperationType, JSONObject, SqlQuerySpec } from '@azure/cosmos';
import CosmosdbService from './cosmosdb-service';
import head from 'lodash/head';
import Listr from 'listr';
import readlineSync from 'readline-sync';

export enum OperationType {
  Create = 'CREATE',
  Update = 'UPDATE',
  Delete = 'DELETE',
}

type BaseMigrationConfig = {
  operationType: OperationType;
};

type CreateDataMigrationConfig = BaseMigrationConfig & {
  operationType: OperationType.Create;
  documents: unknown[];
};

type UpdateDataMigrationConfig = BaseMigrationConfig & {
  operationType: OperationType.Update;
  selectFn: () => SqlQuerySpec;
  updateFn: (document: unknown) => unknown;
};

type DeleteDataMigrationConfig = BaseMigrationConfig & {
  operationType: OperationType.Delete;
  selectFn: () => SqlQuerySpec;
  partitionKeySelectFn: (document: unknown) => string;
};

export type MigrationConfig =
  | CreateDataMigrationConfig
  | UpdateDataMigrationConfig
  | DeleteDataMigrationConfig;

const engine =
  (cosmosDbService: CosmosdbService) =>
  async (input: MigrationConfig): Promise<void> => {
    const getEngineFn = (): (() => Promise<void>) => {
      switch (input.operationType) {
        case OperationType.Create:
          return async () => {
            const documentsCount = input.documents.length;

            const result = readlineSync.keyInYN(
              `Operation type: Create. Documents count: ${documentsCount}. Proceed?`,
            );

            if (!result) {
              return;
            }

            const operations = input.documents.map((document) => ({
              operationType: BulkOperationType.Create,
              resourceBody: document as JSONObject,
            }));

            const tasks = new Listr([
              {
                title: 'Creating documents',
                task: () => cosmosDbService.bulkCreate(operations),
              },
            ]);

            await tasks.run();
          };

        case OperationType.Update:
          return async () => {
            const documents = await cosmosDbService.find(input.selectFn());

            const result = readlineSync.keyInYN(
              `Operation type: Update. Found: ${documents.length} documents. Proceed?`,
            );

            if (!result) {
              return;
            }

            const firstDocument = head(documents);

            if (!firstDocument) {
              return;
            }

            const newDocuments = documents.map(input.updateFn);

            const operations = newDocuments.map((document) => ({
              operationType: BulkOperationType.Upsert,
              resourceBody: document as JSONObject,
            }));

            const tasks = new Listr([
              {
                title: 'Updating documents',
                task: () => cosmosDbService.bulkUpsert(operations),
              },
            ]);

            await tasks.run();
          };

        case OperationType.Delete:
          return async () => {
            const documents = await cosmosDbService.find(input.selectFn());

            const result = readlineSync.keyInYN(
              `Operation type: Delete. Found: ${documents.length} documents. Proceed?`,
            );

            if (!result) {
              return;
            }

            const operations = documents.map((document) => ({
              operationType: BulkOperationType.Delete,
              id: document.id as string,
              partitionKey: input.partitionKeySelectFn(document),
            }));

            const tasks = new Listr([
              {
                title: 'Deleting documents',
                task: () => cosmosDbService.bulkDelete(operations),
              },
            ]);

            await tasks.run();
          };
      }
    };

    const engineFn = getEngineFn();

    await engineFn();
  };

export default engine;
