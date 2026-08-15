import { DataSource } from 'typeorm';
import { Inventory } from '../inventories/entities/inventory.entity';
import { Product } from '../products/entities/product.entity';

try {
  process.loadEnvFile();
} catch (error) {
  if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
    throw error;
  }
}

const requiredEnv = (name: string): string => {
  const value = process.env[name];

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
};

export default new DataSource({
  type: 'mysql',
  host: requiredEnv('DB_HOST'),
  port: Number(requiredEnv('DB_PORT')),
  username: requiredEnv('DB_USERNAME'),
  password: requiredEnv('DB_PASSWORD'),
  database: requiredEnv('DB_DATABASE'),
  entities: [Product, Inventory],
  migrations: [__dirname + '/../migrations/*{.ts,.js}'],
  synchronize: false,
});
