import { Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { EventEmitter } from 'node:events';
import { ChannelModel, Options, connect } from 'amqplib';
import { RabbitMqService } from './rabbitmq.service';

jest.mock('amqplib', () => ({ connect: jest.fn() }));

const createChannel = () =>
  Object.assign(new EventEmitter(), {
    assertExchange: jest.fn().mockResolvedValue(undefined),
    assertQueue: jest.fn().mockResolvedValue(undefined),
    bindQueue: jest.fn().mockResolvedValue(undefined),
    consume: jest.fn().mockResolvedValue(undefined),
    publish: jest.fn(),
    close: jest.fn().mockResolvedValue(undefined),
  });

describe('RabbitMqService publisher confirms', () => {
  let service: RabbitMqService;
  let publisher: ReturnType<typeof createChannel>;
  let consumer: ReturnType<typeof createChannel>;
  let connection: {
    createConfirmChannel: jest.Mock;
    createChannel: jest.Mock;
    close: jest.Mock;
  };
  let confirmations: Array<(error?: unknown) => void>;
  let warnSpy: jest.SpyInstance;
  let errorSpy: jest.SpyInstance;
  const event = {
    eventId: 'payment.completed:1',
    eventType: 'payment.completed',
  };

  beforeEach(async () => {
    jest.useFakeTimers();
    confirmations = [];
    publisher = createChannel();
    consumer = createChannel();
    publisher.publish.mockImplementation(
      (
        _exchange: string,
        _routingKey: string,
        _content: Buffer,
        _options: Options.Publish,
        callback: (error?: unknown) => void,
      ) => {
        confirmations.push(callback);
        return true;
      },
    );
    connection = {
      createConfirmChannel: jest.fn().mockResolvedValue(publisher),
      createChannel: jest.fn().mockResolvedValue(consumer),
      close: jest.fn().mockResolvedValue(undefined),
    };
    jest
      .mocked(connect)
      .mockResolvedValue(connection as unknown as ChannelModel);
    warnSpy = jest.spyOn(Logger.prototype, 'warn').mockImplementation();
    jest.spyOn(Logger.prototype, 'log').mockImplementation();
    errorSpy = jest.spyOn(Logger.prototype, 'error').mockImplementation();
    service = new RabbitMqService(
      new ConfigService({
        RABBITMQ_URL: 'amqp://localhost',
        RABBITMQ_PUBLISH_CONFIRM_TIMEOUT_MS: 100,
      }),
    );
    await service.onModuleInit();
  });

  afterEach(async () => {
    await service.onModuleDestroy();
    jest.useRealTimers();
    jest.restoreAllMocks();
  });

  it('creates a ConfirmChannel for publishing and a regular Channel for consuming', async () => {
    expect(connection.createConfirmChannel).toHaveBeenCalledTimes(1);
    expect(connection.createChannel).toHaveBeenCalledTimes(1);
    await service.consume('queue', jest.fn());
    expect(consumer.consume).toHaveBeenCalledWith(
      'queue',
      expect.any(Function),
      { noAck: false },
    );
    expect(publisher.consume).not.toHaveBeenCalled();
  });

  it('waits for ACK before resolving publishJson and preserves event metadata', async () => {
    const resolved = jest.fn();
    const pending = service
      .publishJson('exchange', 'route', event)
      .then(resolved);
    await jest.advanceTimersByTimeAsync(0);
    expect(resolved).not.toHaveBeenCalled();
    expect(publisher.publish).toHaveBeenCalledWith(
      'exchange',
      'route',
      Buffer.from(JSON.stringify(event)),
      {
        persistent: true,
        contentType: 'application/json',
        messageId: event.eventId,
        type: event.eventType,
      },
      expect.any(Function),
    );
    confirmations[0](null);
    await pending;
    expect(resolved).toHaveBeenCalledTimes(1);
    expect(jest.getTimerCount()).toBe(0);
  });

  it.each(['message nacked', 'channel closed'])(
    'rejects a confirm error: %s',
    async (message) => {
      const pending = service.publishJson('exchange', 'route', event);
      const assertion = expect(pending).rejects.toThrow(message);
      await jest.advanceTimersByTimeAsync(0);
      confirmations[0](new Error(message));
      await assertion;
      expect(jest.getTimerCount()).toBe(0);
    },
  );

  it('rejects synchronous publish errors and clears the timer', async () => {
    publisher.publish.mockImplementationOnce(() => {
      throw new Error('closed channel');
    });
    await expect(
      service.publishJson('exchange', 'route', event),
    ).rejects.toThrow('closed channel');
    expect(jest.getTimerCount()).toBe(0);
  });

  it('rejects after the configured timeout and ignores a late ACK', async () => {
    const resolved = jest.fn();
    const pending = service
      .publishJson('exchange', 'route', event)
      .then(resolved);
    const assertion = expect(pending).rejects.toThrow(
      'confirm not observed within 100ms',
    );
    await jest.advanceTimersByTimeAsync(99);
    expect(resolved).not.toHaveBeenCalled();
    await jest.advanceTimersByTimeAsync(1);
    await assertion;
    confirmations[0](null);
    expect(resolved).not.toHaveBeenCalled();
    expect(jest.getTimerCount()).toBe(0);
  });

  it('defaults the confirm timeout to 3000ms', async () => {
    service = new RabbitMqService(
      new ConfigService({ RABBITMQ_URL: 'amqp://localhost' }),
    );
    const pending = service.publishJson('exchange', 'route', event);
    const assertion = expect(pending).rejects.toThrow(
      'confirm not observed within 3000ms',
    );
    await jest.advanceTimersByTimeAsync(3000);
    await assertion;
  });

  it('warns on buffer pressure but resolves after a later ACK', async () => {
    publisher.publish.mockImplementationOnce(
      (
        _exchange: string,
        _route: string,
        _content: Buffer,
        _options: Options.Publish,
        callback: (error?: unknown) => void,
      ) => {
        confirmations.push(callback);
        return false;
      },
    );
    const resolved = jest.fn();
    const pending = service
      .publishJson('exchange', 'route', event)
      .then(resolved);
    await jest.advanceTimersByTimeAsync(0);
    expect(warnSpy).toHaveBeenCalledWith('RabbitMQ publish buffer is full');
    expect(resolved).not.toHaveBeenCalled();
    confirmations[0](null);
    await pending;
    expect(resolved).toHaveBeenCalledTimes(1);
  });

  it('waits for publishMessage confirmation and preserves headers and options', async () => {
    const content = Buffer.from('retry body');
    const options = {
      headers: { 'x-retry-count': 2 },
      contentType: 'application/json',
      messageId: event.eventId,
      type: event.eventType,
      correlationId: 'correlation',
    };
    const resolved = jest.fn();
    const pending = service
      .publishMessage('retry', 'route', content, options)
      .then(resolved);
    await jest.advanceTimersByTimeAsync(0);
    expect(resolved).not.toHaveBeenCalled();
    expect(publisher.publish).toHaveBeenCalledWith(
      'retry',
      'route',
      content,
      { ...options, persistent: true },
      expect.any(Function),
    );
    confirmations[0](null);
    await pending;
    expect(resolved).toHaveBeenCalledTimes(1);
  });

  it('rejects publishMessage on NACK', async () => {
    const pending = service.publishMessage(
      'retry',
      'route',
      Buffer.from('body'),
    );
    const assertion = expect(pending).rejects.toThrow('message nacked');
    await jest.advanceTimersByTimeAsync(0);
    confirmations[0](new Error('message nacked'));
    await assertion;
  });

  it('tracks confirmations per message when publications overlap', async () => {
    const firstResolved = jest.fn();
    const secondResolved = jest.fn();
    const first = service
      .publishJson('exchange', 'route', event)
      .then(firstResolved);
    const second = service
      .publishJson('exchange', 'route', event)
      .then(secondResolved);
    await jest.advanceTimersByTimeAsync(0);
    confirmations[1](null);
    await second;
    expect(firstResolved).not.toHaveBeenCalled();
    expect(secondResolved).toHaveBeenCalledTimes(1);
    confirmations[0](null);
    await first;
    expect(jest.getTimerCount()).toBe(0);
  });

  it('logs publisher channel error events without an unhandled EventEmitter error', () => {
    expect(() =>
      publisher.emit('error', new Error('channel failure')),
    ).not.toThrow();
    expect(errorSpy).toHaveBeenCalledWith(
      'RabbitMQ publisher channel error: channel failure',
    );
  });
});
