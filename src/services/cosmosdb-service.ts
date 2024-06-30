import {
  Container,
  SqlQuerySpec,
  CreateOperationInput,
  UpsertOperationInput,
  DeleteOperationInput,
  ItemDefinition,
  Resource,
} from '@azure/cosmos';
import chunk from 'lodash/chunk';
import uniqBy from 'lodash/uniqBy';
import omit from 'lodash/omit';

type BulkProcessOperation =
  | CreateOperationInput
  | UpsertOperationInput
  | DeleteOperationInput;

export default class CosmosdbService {
  protected readonly container: Container;

  private readonly successStatusCodes = [200, 201, 204];

  constructor({ container }: { container: Container }) {
    this.container = container;
  }

  private omitCosmosProperties = (data: ItemDefinition & Resource) =>
    omit(data, ['_rid', '_self', '_etag', '_attachments', '_ts']);

  bulkProcess = async (operations: BulkProcessOperation[]): Promise<void> => {
    const chunkedOperations = chunk(operations, 100);

    const iter = async (list: BulkProcessOperation[][]): Promise<void> => {
      if (list.length === 0) {
        return;
      }

      const [firstChunk, ...rest] = list;

      if (!firstChunk) {
        await iter(rest);

        return;
      }

      const operationResponse = await this.container.items.bulk(firstChunk);

      const failedOperations = operationResponse.filter((operationResponse) => {
        return !this.successStatusCodes.includes(operationResponse.statusCode);
      });

      if (failedOperations.length > 0) {
        const uniqueFailedStatusCodes = uniqBy(
          failedOperations.map((failedResponse) => failedResponse.statusCode),
          'statusCode',
        );

        throw new Error(
          `Failed to bulk upsert items. Failed status codes: ${uniqueFailedStatusCodes.join(
            ', ',
          )}.`,
        );
      }

      await iter(rest);
    };

    await iter(chunkedOperations);
  };

  find = async (query: SqlQuerySpec): Promise<ItemDefinition[]> => {
    const { resources } = await this.container.items.query(query).fetchAll();

    return (resources || []).map(this.omitCosmosProperties);
  };

  create = async (data: ItemDefinition) => {
    const { resource } = await this.container.items.create(data);

    if (!resource) {
      throw new Error(`Failed to create document ${JSON.stringify(data, null, 2)}`);
    }

    return this.omitCosmosProperties(resource);
  };

  bulkCreate = async (operations: CreateOperationInput[]) => {
    await this.bulkProcess(operations);
  };

  upsert = async (item: ItemDefinition): Promise<ItemDefinition> => {
    const { resource } = await this.container.items.upsert(item);

    if (!resource) {
      throw new Error(`Failed to upsert document ${JSON.stringify(item, null, 2)}`);
    }

    return this.omitCosmosProperties(resource);
  };

  bulkUpsert = async (operations: UpsertOperationInput[]) => {
    await this.bulkProcess(operations);
  };

  delete = async (id: string, partitionKeyValue?: string): Promise<void> => {
    await this.container.item(id, partitionKeyValue).delete();
  };

  bulkDelete = async (operations: DeleteOperationInput[]) => {
    await this.bulkProcess(operations);
  };
}
