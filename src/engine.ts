import { BulkOperationType, JSONObject, SqlQuerySpec } from '@azure/cosmos';
import CosmosDBContainer from './cosmosdb-container';
import head from 'lodash/head';
import Listr from 'listr';
import readlineSync from 'readline-sync';

enum OperationType {
  Create = 'CREATE',
  Update = 'UPDATE',
  Delete = 'DELETE',
}

type BaseEngineInput = {
  cosmosDbContainer: CosmosDBContainer;
  operationType: OperationType;
};

type CreateDataInput = BaseEngineInput & {
  operationType: OperationType.Create;
  documents: unknown[];
};

type UpdateDataInput = BaseEngineInput & {
  operationType: OperationType.Update;
  selectFn: () => SqlQuerySpec;
  updateFn: (input: unknown) => unknown;
};

type DeleteDataInput = BaseEngineInput & {
  operationType: OperationType.Delete;
  selectFn: () => SqlQuerySpec;
  partitionKeySelectFn: (input: unknown) => string;
};

type EngineInput = CreateDataInput | UpdateDataInput | DeleteDataInput;

// TODO Add https://www.npmjs.com/package/readline-sync

// TODO Add listr

const engine = async (input: EngineInput): Promise<void> => {
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
              task: () => input.cosmosDbContainer.bulkCreate(operations),
            },
          ]);

          await tasks.run();
        };

      case OperationType.Update:
        return async () => {
          const documents = await input.cosmosDbContainer.find(input.selectFn());

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
              task: () => input.cosmosDbContainer.bulkUpsert(operations),
            },
          ]);

          await tasks.run();
        };

      case OperationType.Delete:
        return async () => {
          const documents = await input.cosmosDbContainer.find(input.selectFn());

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
              task: () => input.cosmosDbContainer.bulkDelete(operations),
            },
          ]);

          await tasks.run();
        };
    }
  };

  const engineFn = getEngineFn();

  await engineFn;
};

export default engine;
