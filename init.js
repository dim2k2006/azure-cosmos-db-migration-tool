const fs = require('fs');

const content = `
import { SqlQuerySpec } from '@azure/cosmos';
import { MigrationConfig, OperationType } from './engine/engine';

const migrationConfig: MigrationConfig = {};

export default migrationConfig;
`;

const init = () => {
  fs.writeFileSync('./src/migration.ts', content, 'utf-8');
};

init();
