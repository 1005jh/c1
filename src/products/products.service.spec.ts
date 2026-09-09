import { Logger, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';
import { LessThan, Repository } from 'typeorm';
import { createClient } from 'redis';
import { RedisService } from '../redis/redis.service';
import { Product } from './entities/product.entity';
import {
  PRODUCT_CURSOR_CACHE_KEY,
  ProductCursorCacheService,
} from './product-cursor-cache.service';
import { ProductsService } from './products.service';

jest.mock('redis', () => ({ createClient: jest.fn() }));

type MockRepository<T = unknown> = Partial<
  Record<keyof Repository<T>, jest.Mock>
>;

const createMockRepository = (): MockRepository<Product> => ({
  create: jest.fn(),
  save: jest.fn(),
  findOne: jest.fn(),
  find: jest.fn(),
  findAndCount: jest.fn(),
});

describe('ProductsService', () => {
  let service: ProductsService;
  let repository: MockRepository<Product>;
  let cache: ProductCursorCacheService;
  let warnSpy: jest.SpyInstance;
  let client: {
    isReady: boolean;
    connect: jest.Mock;
    on: jest.Mock;
    get: jest.Mock;
    set: jest.Mock;
    del: jest.Mock;
  };

  beforeEach(async () => {
    client = {
      isReady: true,
      connect: jest.fn().mockResolvedValue(undefined),
      on: jest.fn(),
      get: jest.fn().mockResolvedValue(null),
      set: jest.fn().mockResolvedValue('OK'),
      del: jest.fn().mockResolvedValue(1),
    };
    jest
      .mocked(createClient)
      .mockReturnValue(client as unknown as ReturnType<typeof createClient>);
    warnSpy = jest.spyOn(Logger.prototype, 'warn').mockImplementation();
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ProductsService,
        ProductCursorCacheService,
        RedisService,
        {
          provide: ConfigService,
          useValue: new ConfigService({ PRODUCT_CURSOR_CACHE_ENABLED: 'true' }),
        },
        {
          provide: getRepositoryToken(Product),
          useValue: createMockRepository(),
        },
      ],
    }).compile();

    await module.init();

    service = module.get<ProductsService>(ProductsService);
    cache = module.get(ProductCursorCacheService);
    repository = module.get<MockRepository<Product>>(
      getRepositoryToken(Product),
    );
  });

  afterEach(() => jest.restoreAllMocks());

  describe('create', () => {
    it('saves and returns a product', async () => {
      const createProductDto = {
        name: 'MacBook Pro',
        price: 2990000,
        description: 'Test product',
      };
      const product = { id: 1, ...createProductDto } as Product;

      repository.create?.mockReturnValue(product);
      repository.save?.mockResolvedValue(product);

      await expect(service.create(createProductDto)).resolves.toBe(product);
      expect(repository.create).toHaveBeenCalledWith(createProductDto);
      expect(repository.save).toHaveBeenCalledWith(product);
      expect(client.del).toHaveBeenCalledWith(PRODUCT_CURSOR_CACHE_KEY);
    });

    it('invalidates only after the DB save succeeds', async () => {
      const product = { id: 1, name: 'New product', price: 100 } as Product;
      repository.create?.mockReturnValue(product);
      repository.save?.mockImplementation(() => {
        expect(client.del).not.toHaveBeenCalled();
        return Promise.resolve(product);
      });

      await expect(service.create(product)).resolves.toBe(product);
      expect(client.del).toHaveBeenCalledTimes(1);
    });

    it('does not invalidate when the DB save fails', async () => {
      repository.save?.mockRejectedValue(new Error('DB save failed'));

      await expect(service.create({ name: 'New', price: 100 })).rejects.toThrow(
        'DB save failed',
      );
      expect(client.del).not.toHaveBeenCalled();
    });

    it('returns the saved product even when Redis DEL fails', async () => {
      const product = { id: 1, name: 'New product', price: 100 } as Product;
      repository.save?.mockResolvedValue(product);
      client.del.mockRejectedValue(new Error('Redis unavailable'));

      await expect(service.create(product)).resolves.toBe(product);
      expect(warnSpy).toHaveBeenCalled();
    });
  });

  describe('findOne', () => {
    it('returns a product when it exists', async () => {
      const product = { id: 1, name: 'MacBook Pro', price: 2990000 } as Product;

      repository.findOne?.mockResolvedValue(product);

      await expect(service.findOne(1)).resolves.toBe(product);
      expect(repository.findOne).toHaveBeenCalledWith({ where: { id: 1 } });
      expect(client.get).not.toHaveBeenCalled();
    });

    it('throws NotFoundException when product does not exist', async () => {
      repository.findOne?.mockResolvedValue(null);

      await expect(service.findOne(1)).rejects.toBeInstanceOf(
        NotFoundException,
      );
    });
  });

  describe('findAll', () => {
    it('passes pagination options to the repository', async () => {
      const items = [
        { id: 1, name: 'MacBook Pro', price: 2990000 },
      ] as Product[];

      repository.findAndCount?.mockResolvedValue([items, 1]);

      await expect(service.findAll({ page: 2, limit: 10 })).resolves.toEqual({
        items,
        page: 2,
        limit: 10,
        total: 1,
      });
      expect(repository.findAndCount).toHaveBeenCalledWith({
        order: { id: 'DESC' },
        skip: 10,
        take: 10,
      });
      expect(client.get).not.toHaveBeenCalled();
    });
  });

  describe('findAllByCursor', () => {
    it('returns a hot first-page cache hit without querying the DB', async () => {
      const response = {
        items: [
          {
            id: 1,
            name: 'Cached product',
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
      client.get.mockResolvedValue(JSON.stringify(response));

      await expect(service.findAllByCursor({ limit: 20 })).resolves.toEqual(
        response,
      );
      expect(repository.find).not.toHaveBeenCalled();
      expect(client.set).not.toHaveBeenCalled();
    });

    it('populates the cache with the DB response on a miss', async () => {
      repository.find?.mockResolvedValue([]);
      const response = await service.findAllByCursor({ limit: 20 });

      expect(client.get).toHaveBeenCalledWith(PRODUCT_CURSOR_CACHE_KEY);
      expect(client.set).toHaveBeenCalledWith(
        PRODUCT_CURSOR_CACHE_KEY,
        JSON.stringify(response),
        { EX: 60 },
      );
    });

    it('falls back to the DB when Redis GET rejects', async () => {
      client.get.mockRejectedValue(new Error('Redis GET failed'));
      repository.find?.mockResolvedValue([]);

      await expect(service.findAllByCursor({ limit: 20 })).resolves.toEqual({
        items: [],
        limit: 20,
        hasNext: false,
        nextCursor: null,
      });
      expect(repository.find).toHaveBeenCalledTimes(1);
      expect(warnSpy).toHaveBeenCalled();
    });

    it('falls back immediately when Redis is not ready', async () => {
      client.isReady = false;
      repository.find?.mockResolvedValue([]);

      await expect(
        service.findAllByCursor({ limit: 20 }),
      ).resolves.toMatchObject({
        items: [],
        limit: 20,
      });
      expect(repository.find).toHaveBeenCalledTimes(1);
      expect(client.get).not.toHaveBeenCalled();
      expect(client.set).not.toHaveBeenCalled();
    });

    it('returns the DB response when Redis SET rejects', async () => {
      client.set.mockRejectedValue(new Error('Redis SET failed'));
      repository.find?.mockResolvedValue([]);

      await expect(
        service.findAllByCursor({ limit: 20 }),
      ).resolves.toMatchObject({
        items: [],
        limit: 20,
      });
      expect(client.set).toHaveBeenCalledTimes(1);
      expect(warnSpy).toHaveBeenCalled();
    });

    it.each([10, 50])('bypasses the cache for limit %i', async (limit) => {
      const getSpy = jest.spyOn(cache, 'get');
      const setSpy = jest.spyOn(cache, 'set');
      repository.find?.mockResolvedValue([]);

      await service.findAllByCursor({ limit });

      expect(repository.find).toHaveBeenCalledWith({
        order: { id: 'DESC' },
        take: limit + 1,
      });
      expect(getSpy).not.toHaveBeenCalled();
      expect(setSpy).not.toHaveBeenCalled();
    });

    it('reads the first cursor page by id desc with limit plus one', async () => {
      const rows = [
        { id: 30, name: 'Product 30', price: 30000 },
        { id: 29, name: 'Product 29', price: 29000 },
      ] as Product[];

      repository.find?.mockResolvedValue(rows);

      await expect(service.findAllByCursor({ limit: 20 })).resolves.toEqual({
        items: rows,
        limit: 20,
        nextCursor: null,
        hasNext: false,
      });
      expect(repository.find).toHaveBeenCalledWith({
        order: { id: 'DESC' },
        take: 21,
      });
    });

    it('uses id less than cursorId for the next cursor page', async () => {
      repository.find?.mockResolvedValue([]);

      await service.findAllByCursor({ cursorId: 50, limit: 20 });

      expect(repository.find).toHaveBeenCalledWith({
        where: { id: LessThan(50) },
        order: { id: 'DESC' },
        take: 21,
      });
      expect(client.get).not.toHaveBeenCalled();
      expect(client.set).not.toHaveBeenCalled();
    });

    it('returns limit items with hasNext true and nextCursor from the last returned item', async () => {
      const rows = Array.from({ length: 21 }, (_, index) => ({
        id: 100 - index,
        name: `Product ${100 - index}`,
        price: 1000 + index,
      })) as Product[];

      repository.find?.mockResolvedValue(rows);

      await expect(service.findAllByCursor({ limit: 20 })).resolves.toEqual({
        items: rows.slice(0, 20),
        limit: 20,
        nextCursor: 81,
        hasNext: true,
      });
    });

    it('returns no next cursor when rows do not exceed the requested limit', async () => {
      const rows = Array.from({ length: 20 }, (_, index) => ({
        id: 20 - index,
        name: `Product ${20 - index}`,
        price: 1000 + index,
      })) as Product[];

      repository.find?.mockResolvedValue(rows);

      await expect(
        service.findAllByCursor({ cursorId: 21, limit: 20 }),
      ).resolves.toEqual({
        items: rows,
        limit: 20,
        nextCursor: null,
        hasNext: false,
      });
    });
  });
});
