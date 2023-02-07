import { BulkOperationType, JSONObject, SqlQuerySpec } from '@azure/cosmos';
import CosmosDBContainer from './cosmosdb-container';
import head from 'lodash/head';
import isEqual from 'lodash/isEqual';

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
  rollbackFn: (input: unknown) => unknown;
};

type DeleteDataInput = BaseEngineInput & {
  operationType: OperationType.Delete;
  selectFn: () => SqlQuerySpec;
  partitionKeySelectFn: (input: unknown) => string;
};

type EngineInput = CreateDataInput | UpdateDataInput | DeleteDataInput;

const engine = async (input: EngineInput): Promise<void> => {
  const getEngineFn = (): (() => Promise<void>) => {
    switch (input.operationType) {
      case OperationType.Create:
        return async () => {
          const operations = input.documents.map((document) => ({
            operationType: BulkOperationType.Create,
            resourceBody: document as JSONObject,
          }));

          await input.cosmosDbContainer.bulkCreate(operations);
        };

      case OperationType.Update:
        return async () => {
          const documents = await input.cosmosDbContainer.find(input.selectFn());

          const firstDocument = head(documents);

          if (!firstDocument) {
            return;
          }

          if (!isEqual(input.rollbackFn(input.updateFn(firstDocument)), firstDocument)) {
            throw new Error('Rollback function is not valid');
          }

          const newDocuments = documents.map(input.updateFn);

          const operations = newDocuments.map((document) => ({
            operationType: BulkOperationType.Upsert,
            resourceBody: document as JSONObject,
          }));

          await input.cosmosDbContainer.bulkUpsert(operations);
        };

      case OperationType.Delete:
        return async () => {
          const documents = await input.cosmosDbContainer.find(input.selectFn());

          const operations = documents.map((document) => ({
            operationType: BulkOperationType.Delete,
            id: document.id as string,
            partitionKey: input.partitionKeySelectFn(document),
          }));

          await input.cosmosDbContainer.bulkDelete(operations);
        };
    }
  };

  const engineFn = getEngineFn();

  await engineFn;
};

export default engine;
