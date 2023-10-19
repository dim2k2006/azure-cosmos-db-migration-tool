import { BulkOperationType, JSONObject, SqlQuerySpec } from '@azure/cosmos';
import CosmosdbService from '../services/cosmosdb-service';
import head from 'lodash/head';
import identity from 'lodash/identity';
import Listr from 'listr';
import readlineSync from 'readline-sync';
import csv from 'csv-parser';
import fs from 'fs';
import path from 'path';

export enum OperationType {
  Create = 'CREATE',
  Update = 'UPDATE',
  Delete = 'DELETE',
}

type BaseMigrationConfig = {
  operationType: OperationType;
};

export enum InputType {
  Json = 'JSON',
  Csv = 'CSV',
}

type BaseCreateDataMigrationConfig = BaseMigrationConfig & {
  operationType: OperationType.Create;
  inputType: InputType;
};

type CreateDataMigrationConfigFromJson = BaseCreateDataMigrationConfig & {
  inputType: InputType.Json;
  documents: unknown[];
};

type CreateDataMigrationConfigFromCsv = BaseCreateDataMigrationConfig & {
  inputType: InputType.Csv;
  filePath: string;
  options?: csv.Options & {
    mapRow?: (row: Record<string, unknown>) => Record<string, unknown>;
  };
};

type CreateDataMigrationConfig =
  | CreateDataMigrationConfigFromJson
  | CreateDataMigrationConfigFromCsv;

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

const genFullFilePath = (filepath: string) => path.resolve(process.cwd(), filepath);

const engine =
  (cosmosDbService: CosmosdbService) =>
  async (input: MigrationConfig): Promise<void> => {
    const getEngineFn = (): (() => Promise<void>) => {
      switch (input.operationType) {
        case OperationType.Create:
          return async () => {
            const getDocuments = (): (() => Promise<unknown[]>) => {
              switch (input.inputType) {
                case InputType.Json:
                  return async () => input.documents;

                case InputType.Csv:
                  return async () => {
                    return new Promise((resolve, reject) => {
                      const results = [] as unknown[];

                      const mapRow = input.options?.mapRow ?? identity;

                      fs.createReadStream(genFullFilePath(input.filePath))
                        .pipe(csv(input.options))
                        .on('data', (data) => {
                          results.push(mapRow(data));
                        })
                        .on('end', () => {
                          resolve(results);
                        })
                        .on('error', (error) => {
                          reject(error);
                        });
                    });
                  };
              }
            };

            const documentsFn = getDocuments();

            const documents = await documentsFn();

            const documentsCount = documents.length;

            const result = readlineSync.keyInYN(
              `Operation type: Create. Documents count: ${documentsCount}. Proceed?`,
            );

            if (!result) {
              return;
            }

            const operations = documents.map((document) => ({
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
