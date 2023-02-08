const fs = require('fs');

const buildEnvFile = () => {
  const content = `COSMOSDB_CONNECTION=
COSMOSDB_DATABASE=
COSMOSDB_CONTAINER=
`;

  fs.writeFileSync('.env', content, 'utf-8');
};

const buildMigrationFile = () => {
  const content = `import { SqlQuerySpec } from '@azure/cosmos';
import { MigrationConfig, OperationType } from './engine/engine';

const migrationConfig: MigrationConfig = {};

export default migrationConfig;
`;

  fs.writeFileSync('./src/migration.ts', content, 'utf-8');
};

const init = () => {
  buildEnvFile();

  buildMigrationFile();
};

init();
