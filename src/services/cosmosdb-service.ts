import {
  Container,
  SqlQuerySpec,
  CreateOperationInput,
  UpsertOperationInput,
  DeleteOperationInput,
  ItemDefinition,
  Resource,
  OperationResponse,
} from '@azure/cosmos';
import chunk from 'lodash/chunk';
import get from 'lodash/get';
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

  private sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private async processChunk(operations: BulkProcessOperation[]): Promise<OperationResponse[]> {
    const operationResponse = await this.container.items.bulk(operations);

    const failedOperationsResponse = operationResponse.filter((operationResponse) => {
      return !this.successStatusCodes.includes(operationResponse.statusCode);
    });

    if (failedOperationsResponse.length === 0) {
      return operationResponse;
    }

    const initialFailedOperations: BulkProcessOperation[] = [];

    const failedOperations = operationResponse.reduce((accumulator, operationResponse, index) => {
      const { statusCode } = operationResponse;

      if (this.successStatusCodes.includes(statusCode)) {
        return accumulator;
      }

      const operation = operations[index];

      const newAccumulator = [...accumulator, operation];

      return newAccumulator;
    }, initialFailedOperations);

    const maxRetryAfterMilliseconds = failedOperationsResponse.reduce((maxRetryAfter, failedOperation) => {
      const retryAfterMilliseconds = get(failedOperation, 'retryAfterMilliseconds', 0) as number;

      return Math.max(maxRetryAfter, retryAfterMilliseconds ?? 0);
    }, 0);

    await this.sleep(maxRetryAfterMilliseconds);

    return this.processChunk(failedOperations)
  }

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

      await this.processChunk(firstChunk)

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
