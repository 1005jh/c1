import { Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createClient } from 'redis';
import { RedisService } from './redis.service';

jest.mock('redis', () => ({ createClient: jest.fn() }));

describe('RedisService', () => {
  let service: RedisService;
  let warnSpy: jest.SpyInstance;
  let client: {
    isReady: boolean;
    isOpen: boolean;
    on: jest.Mock;
    connect: jest.Mock;
    destroy: jest.Mock;
    get: jest.Mock;
    set: jest.Mock;
    del: jest.Mock;
  };

  beforeEach(() => {
    jest.mocked(createClient).mockReset();
    client = {
      isReady: true,
      isOpen: true,
      on: jest.fn(),
      connect: jest.fn().mockResolvedValue(undefined),
      destroy: jest.fn(),
      get: jest.fn().mockResolvedValue('value'),
      set: jest.fn().mockResolvedValue('OK'),
      del: jest.fn().mockResolvedValue(1),
    };
    jest
      .mocked(createClient)
      .mockReturnValue(client as unknown as ReturnType<typeof createClient>);
    warnSpy = jest.spyOn(Logger.prototype, 'warn').mockImplementation();
    service = new RedisService(
      new ConfigService({ PRODUCT_CURSOR_CACHE_ENABLED: 'true' }),
    );
  });

  afterEach(() => jest.restoreAllMocks());

  it('does not connect when cache is disabled', async () => {
    service = new RedisService(new ConfigService());
    service.onModuleInit();
    expect(createClient).not.toHaveBeenCalled();
    await expect(service.get('key')).resolves.toBeNull();
    await expect(service.set('key', 'value', 60)).resolves.toBeUndefined();
    await expect(service.del('key')).resolves.toBeUndefined();
  });

  it('does not wait for a pending initial connection', () => {
    client.connect.mockReturnValue(new Promise(() => {}));
    expect(service.onModuleInit()).toBeUndefined();
    expect(createClient).toHaveBeenCalledWith({
      url: 'redis://localhost:6379',
      socket: { connectTimeout: 1000 },
      disableOfflineQueue: true,
      commandOptions: { timeout: 500 },
    });
    expect(client.on).toHaveBeenCalledWith('error', expect.any(Function));
  });

  it.each([
    new Error('Connection refused'),
    new AggregateError([new Error('Connection refused')]),
  ])('logs connection rejection without failing startup: %s', async (error) => {
    client.connect.mockRejectedValue(error);
    expect(service.onModuleInit()).toBeUndefined();
    await Promise.resolve();
    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining('Connection refused'),
    );
  });

  it('tolerates an invalid Redis URL at initialization', async () => {
    jest.mocked(createClient).mockImplementation(() => {
      throw new Error('Invalid URL');
    });
    expect(() => service.onModuleInit()).not.toThrow();
    await expect(service.get('key')).resolves.toBeNull();
    expect(warnSpy).toHaveBeenCalled();
  });

  it('returns a cache value and sets with EX TTL', async () => {
    service.onModuleInit();
    await expect(service.get('key')).resolves.toBe('value');
    await service.set('key', 'value', 17);
    expect(client.set).toHaveBeenCalledWith('key', 'value', { EX: 17 });
    await service.del('key');
    expect(client.del).toHaveBeenCalledWith('key');
  });

  it('immediately skips commands while not ready and resumes when ready', async () => {
    service.onModuleInit();
    client.isReady = false;
    await expect(service.get('key')).resolves.toBeNull();
    await service.set('key', 'value', 60);
    await service.del('key');
    expect(client.get).not.toHaveBeenCalled();
    expect(client.set).not.toHaveBeenCalled();
    expect(client.del).not.toHaveBeenCalled();
    client.isReady = true;
    await expect(service.get('key')).resolves.toBe('value');
  });

  it('fails open and logs rejected GET, SET, and DEL commands', async () => {
    service.onModuleInit();
    client.get.mockRejectedValue(new Error('GET unavailable'));
    client.set.mockRejectedValue(new Error('SET unavailable'));
    client.del.mockRejectedValue(new Error('DEL unavailable'));
    await expect(service.get('key')).resolves.toBeNull();
    await expect(service.set('key', 'value', 60)).resolves.toBeUndefined();
    await expect(service.del('key')).resolves.toBeUndefined();
    expect(warnSpy).toHaveBeenCalledTimes(3);
  });

  it('destroys an open client, including a reconnecting client, at shutdown', () => {
    service.onModuleInit();
    client.isReady = false;
    service.onModuleDestroy();
    expect(client.destroy).toHaveBeenCalledTimes(1);
  });
});
