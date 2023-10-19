import contentful from 'contentful-management';

interface ContentfulServiceConstructorProps {
  accessToken: string;
  spaceId: string;
  environmentId: string;
}

class ContentfulService {
  private client: contentful.PlainClientAPI;

  private spaceId: string;

  private environmentId: string;

  constructor({
    accessToken,
    spaceId,
    environmentId,
  }: ContentfulServiceConstructorProps) {
    const plainClient = contentful.createClient({ accessToken }, { type: 'plain' });

    this.client = plainClient;

    this.spaceId = spaceId;

    this.environmentId = environmentId;
  }

  getAllPosters = async () => {
    const { total } = await this.client.entry.getMany({
      spaceId: this.spaceId,
      environmentId: this.environmentId,
      query: {
        content_type: 'posterItem',
        limit: 1,
      },
    });

    const params = {
      spaceId: this.spaceId,
      environmentId: this.environmentId,
      query: {
        content_type: 'posterItem',
        limit: 500,
        order: '-sys.createdAt',
      },
    };

    const requestsCount = Math.ceil(total / params.query.limit);

    const iter = async (
      index: number,
      accumulator: contentful.EntryProps<contentful.KeyValueMap>[],
    ): Promise<contentful.EntryProps<contentful.KeyValueMap>[]> => {
      if (index >= requestsCount) {
        return accumulator;
      }

      const newParams = {
        ...params,
        query: {
          ...params.query,
          skip: index * params.query.limit,
        },
      };

      const response = await this.client.entry.getMany(newParams);

      const newAccumulator = [...accumulator, ...response.items];

      return iter(index + 1, newAccumulator);
    };

    const entries = await iter(0, []);

    return entries;
  };

  getAllAssets = async () => {
    const { total } = await this.client.asset.getMany({
      spaceId: this.spaceId,
      environmentId: this.environmentId,
      query: {
        limit: 1,
      },
    });

    const params = {
      spaceId: this.spaceId,
      environmentId: this.environmentId,
      query: {
        limit: 500,
        order: '-sys.createdAt',
      },
    };

    const requestsCount = Math.ceil(total / params.query.limit);

    const iter = async (
      index: number,
      accumulator: contentful.AssetProps[],
    ): Promise<contentful.AssetProps[]> => {
      if (index >= requestsCount) {
        return accumulator;
      }

      const newParams = {
        ...params,
        query: {
          ...params.query,
          skip: index * params.query.limit,
        },
      };

      const response = await this.client.asset.getMany(newParams);

      const newAccumulator = [...accumulator, ...response.items];

      return iter(index + 1, newAccumulator);
    };

    const entries = await iter(0, []);

    return entries;
  };

  updatePoster = async (newEntry: contentful.EntryProps<contentful.KeyValueMap>) => {
    try {
      const query = {
        spaceId: this.spaceId,
        environmentId: this.environmentId,
        entryId: newEntry.sys.id,
      };

      const updatedEntry = await this.client.entry.update(query, newEntry);

      const publishedEntry = await this.client.entry.publish(query, updatedEntry);

      return publishedEntry;
    } catch (error) {
      console.log(JSON.stringify(newEntry, null, 2));

      throw error;
    }
  };

  removeAsset = async (assetId: string) => {
    const query = {
      spaceId: this.spaceId,
      environmentId: this.environmentId,
      assetId,
    };

    await this.client.asset.unpublish(query);

    await this.client.asset.delete(query);
  };
}

export default ContentfulService;
