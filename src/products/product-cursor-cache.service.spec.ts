import { Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { RedisService } from '../redis/redis.service';
import {
  PRODUCT_CURSOR_CACHE_KEY,
  ProductCursorCacheService,
  ProductCursorResponse,
} from './product-cursor-cache.service';

describe('ProductCursorCacheService', () => {
  let redis: jest.Mocked<Pick<RedisService, 'get' | 'set' | 'del'>>;
  let service: ProductCursorCacheService;
  let warnSpy: jest.SpyInstance;
  const response: ProductCursorResponse = {
    items: [
      {
        id: 1,
        name: 'Product',
        price: 100,
        description: null,
        createdAt: new Date('2026-01-01T00:00:00.000Z'),
        updatedAt: new Date('2026-01-01T00:00:00.000Z'),
      },
    ],
    limit: 20,
    hasNext: false,
    nextCursor: null,
  };

  const createService = (config: Record<string, unknown>) =>
    new ProductCursorCacheService(
      new ConfigService(config),
      redis as unknown as RedisService,
    );

  beforeEach(() => {
    redis = {
      get: jest.fn().mockResolvedValue(null),
      set: jest.fn().mockResolvedValue(undefined),
      del: jest.fn().mockResolvedValue(undefined),
    };
    service = createService({ PRODUCT_CURSOR_CACHE_ENABLED: 'true' });
    warnSpy = jest.spyOn(Logger.prototype, 'warn').mockImplementation();
  });

  afterEach(() => jest.restoreAllMocks());

  it.each([undefined, 'false'])(
    'does nothing when disabled (%s)',
    async (enabled) => {
      service = createService({ PRODUCT_CURSOR_CACHE_ENABLED: enabled });
      expect(service.isCacheable({ limit: 20 })).toBe(false);
      await expect(service.get()).resolves.toBeNull();
      await service.set(response);
      await service.invalidate();
      expect(redis.get).not.toHaveBeenCalled();
      expect(redis.set).not.toHaveBeenCalled();
      expect(redis.del).not.toHaveBeenCalled();
    },
  );

  it('accepts only the hot first page', () => {
    expect(service.isCacheable({ limit: 20 })).toBe(true);
    expect(service.isCacheable({ limit: 20, cursorId: 2 })).toBe(false);
    expect(service.isCacheable({ limit: 10 })).toBe(false);
    expect(service.isCacheable({ limit: 50 })).toBe(false);
  });

  it('round trips valid JSON while preserving Product dates', async () => {
    redis.get.mockResolvedValue(JSON.stringify(response));
    await expect(service.get()).resolves.toEqual(response);
    expect(redis.del).not.toHaveBeenCalled();
  });

  it.each([
    '{invalid',
    'null',
    '{}',
    JSON.stringify({ ...response, items: 'invalid' }),
    JSON.stringify({ ...response, limit: 10 }),
    JSON.stringify({ ...response, hasNext: 'false' }),
    JSON.stringify({ ...response, nextCursor: '1' }),
    JSON.stringify({ ...response, items: [{ ...response.items[0], id: '1' }] }),
    JSON.stringify({
      ...response,
      items: [{ ...response.items[0], name: null }],
    }),
    JSON.stringify({
      ...response,
      items: [{ ...response.items[0], createdAt: 'invalid' }],
    }),
    JSON.stringify({ ...response, hasNext: true, nextCursor: 1 }),
    JSON.stringify({
      ...response,
      items: [response.items[0], response.items[0]],
    }),
  ])('treats corrupt or unexpected payload as a miss: %s', async (json) => {
    redis.get.mockResolvedValue(json);
    await expect(service.get()).resolves.toBeNull();
    expect(redis.del).toHaveBeenCalledWith(PRODUCT_CURSOR_CACHE_KEY);
    expect(warnSpy).toHaveBeenCalled();
  });

  it.each([60, 15])('sets JSON with TTL %i seconds', async (ttl) => {
    service = createService({
      PRODUCT_CURSOR_CACHE_ENABLED: 'true',
      PRODUCT_CURSOR_CACHE_TTL_SECONDS: ttl,
    });
    await service.set(response);
    expect(redis.set).toHaveBeenCalledWith(
      PRODUCT_CURSOR_CACHE_KEY,
      JSON.stringify(response),
      ttl,
    );
  });
});
