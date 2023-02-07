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
import head from 'lodash/head';
import omit from 'lodash/omit';

type BulkProcessOperation =
  | CreateOperationInput
  | UpsertOperationInput
  | DeleteOperationInput;

export default class CosmosDBContainer<T extends ItemDefinition> {
  protected readonly container: Container;

  constructor({ container }: { container: Container }) {
    this.container = container;
  }

  private omitCosmosProperties = (data: ItemDefinition & Resource) =>
    omit(data, ['_rid', '_self', '_etag', '_attachments', '_ts']) as T;

  bulkProcess = async (operations: BulkProcessOperation[]): Promise<void> => {
    const chunkedOperations = chunk(operations, 100);

    const iter = async (list: BulkProcessOperation[][]): Promise<void> => {
      if (list.length === 0) {
        return;
      }

      const firstChunk = head(list);

      if (!firstChunk) {
        await iter(list.slice(1));

        return;
      }

      await this.container.items.bulk(firstChunk);

      await iter(list.slice(1));
    };

    await iter(chunkedOperations);
  };

  find = async (query: SqlQuerySpec): Promise<T[]> => {
    const { resources } = await this.container.items.query(query).fetchAll();

    return (resources || []).map(this.omitCosmosProperties);
  };

  create = async (data: T) => {
    const { resource } = await this.container.items.create(data);

    if (!resource) {
      throw new Error(`Failed to create document ${JSON.stringify(data, null, 2)}`);
    }

    return this.omitCosmosProperties(resource);
  };

  bulkCreate = async (operations: CreateOperationInput[]) => {
    await this.bulkProcess(operations);
  };

  upsert = async (item: T): Promise<T> => {
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
