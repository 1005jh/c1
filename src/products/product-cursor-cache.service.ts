import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { RedisService } from '../redis/redis.service';
import { GetProductsCursorQueryDto } from './dto/get-products-cursor-query.dto';
import { Product } from './entities/product.entity';

export const PRODUCT_CURSOR_CACHE_KEY = 'products:cursor:first:limit:20:v1';

export interface ProductCursorResponse {
  items: Product[];
  limit: number;
  hasNext: boolean;
  nextCursor: number | null;
}

type CachedProduct = Omit<Product, 'createdAt' | 'updatedAt'> & {
  createdAt: string;
  updatedAt: string;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const isDateString = (value: unknown): value is string =>
  typeof value === 'string' && Number.isFinite(Date.parse(value));

const isCachedProduct = (value: unknown): value is CachedProduct =>
  isRecord(value) &&
  typeof value.id === 'number' &&
  Number.isSafeInteger(value.id) &&
  value.id > 0 &&
  typeof value.name === 'string' &&
  typeof value.price === 'number' &&
  Number.isSafeInteger(value.price) &&
  value.price >= 0 &&
  (value.description === null || typeof value.description === 'string') &&
  isDateString(value.createdAt) &&
  isDateString(value.updatedAt);

@Injectable()
export class ProductCursorCacheService {
  private readonly logger = new Logger(ProductCursorCacheService.name);
  private readonly enabled: boolean;
  private readonly ttlSeconds: number;

  constructor(
    configService: ConfigService,
    private readonly redisService: RedisService,
  ) {
    this.enabled =
      configService.get<string>('PRODUCT_CURSOR_CACHE_ENABLED', 'false') ===
      'true';
    this.ttlSeconds = configService.get<number>(
      'PRODUCT_CURSOR_CACHE_TTL_SECONDS',
      60,
    );
  }

  isCacheable(query: GetProductsCursorQueryDto): boolean {
    return this.enabled && query.cursorId === undefined && query.limit === 20;
  }

  async get(): Promise<ProductCursorResponse | null> {
    if (!this.enabled) {
      return null;
    }

    const json = await this.redisService.get(PRODUCT_CURSOR_CACHE_KEY);
    if (json === null) {
      return null;
    }

    try {
      const value: unknown = JSON.parse(json);
      if (
        !isRecord(value) ||
        value.limit !== 20 ||
        typeof value.hasNext !== 'boolean' ||
        !Array.isArray(value.items) ||
        value.items.length > 20 ||
        !value.items.every(isCachedProduct)
      ) {
        throw new Error('Invalid cursor response shape');
      }

      const items = value.items;
      const expectedCursor = value.hasNext ? items.at(-1)?.id : null;
      if (
        (value.hasNext && items.length !== 20) ||
        value.nextCursor !== expectedCursor ||
        !items.every(
          (item, index) => index === 0 || items[index - 1].id > item.id,
        )
      ) {
        throw new Error('Invalid cursor response ordering or pagination');
      }

      return {
        items: items.map((item) => ({
          ...item,
          createdAt: new Date(item.createdAt),
          updatedAt: new Date(item.updatedAt),
        })),
        limit: 20,
        hasNext: value.hasNext,
        nextCursor: expectedCursor ?? null,
      };
    } catch {
      this.logger.warn('Invalid product cursor cache payload; reading from DB');
      await this.invalidate();
      return null;
    }
  }

  async set(response: ProductCursorResponse): Promise<void> {
    if (!this.enabled) {
      return;
    }

    try {
      await this.redisService.set(
        PRODUCT_CURSOR_CACHE_KEY,
        JSON.stringify(response),
        this.ttlSeconds,
      );
    } catch {
      this.logger.warn('Product cursor cache serialization failed');
    }
  }

  async invalidate(): Promise<void> {
    if (this.enabled) {
      await this.redisService.del(PRODUCT_CURSOR_CACHE_KEY);
    }
  }
}
