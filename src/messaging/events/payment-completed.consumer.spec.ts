import { Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Channel, ConsumeMessage, MessageProperties } from 'amqplib';
import {
  PAYMENT_COMPLETED_DEAD_ROUTING_KEY,
  PAYMENT_COMPLETED_DLX,
  PAYMENT_COMPLETED_FAILURE_TYPE_HEADER,
  PAYMENT_COMPLETED_QUEUE,
  PAYMENT_COMPLETED_RETRY_COUNT_HEADER,
  PAYMENT_COMPLETED_RETRY_EXCHANGE,
  PAYMENT_COMPLETED_RETRY_ROUTING_KEY,
} from '../rabbitmq/rabbitmq.constants';
import { RabbitMqService } from '../rabbitmq/rabbitmq.service';
import {
  PAYMENT_COMPLETED_EVENT_TYPE,
  PAYMENT_COMPLETED_EVENT_VERSION,
  PaymentCompletedEvent,
} from './payment-completed.event';
import {
  PAYMENT_COMPLETED_CONSUMER_FAILURE_MESSAGE,
  PaymentCompletedConsumerFaultInjector,
} from './payment-completed.consumer-fault-injector';
import {
  PAYMENT_COMPLETED_CONSUMER_NAME,
  PaymentCompletedConsumer,
} from './payment-completed.consumer';
import {
  ProcessedMessagePersistenceError,
  ProcessedMessageService,
} from '../processed-message.service';

const MAX_RETRIES = 3;

const createMessage = (
  content: string,
  headers: Record<string, unknown> = {},
): ConsumeMessage =>
  ({
    content: Buffer.from(content),
    properties: {
      contentType: 'application/json',
      contentEncoding: undefined,
      headers,
      deliveryMode: 2,
      priority: undefined,
      correlationId: undefined,
      replyTo: undefined,
      expiration: undefined,
      messageId: 'message-id',
      timestamp: undefined,
      type: PAYMENT_COMPLETED_EVENT_TYPE,
      userId: undefined,
      appId: undefined,
      clusterId: undefined,
    } satisfies MessageProperties,
  }) as ConsumeMessage;

const createEvent = (paymentId = 1): PaymentCompletedEvent => ({
  eventId: `payment.completed:${paymentId}`,
  eventType: PAYMENT_COMPLETED_EVENT_TYPE,
  eventVersion: PAYMENT_COMPLETED_EVENT_VERSION,
  occurredAt: new Date().toISOString(),
  paymentId,
  orderId: paymentId + 1,
  amount: 30000,
  providerTransactionId: `tx_${paymentId}`,
});

const createConfigService = ({
  failCount = 0,
  maxRetries = MAX_RETRIES,
}: {
  failCount?: number;
  maxRetries?: number;
} = {}): ConfigService =>
  ({
    get: jest.fn((key: string) => {
      if (key === 'PAYMENT_COMPLETED_CONSUMER_FAIL_COUNT') {
        return failCount;
      }

      if (key === 'PAYMENT_COMPLETED_MAX_RETRIES') {
        return maxRetries;
      }

      return undefined;
    }),
  }) as unknown as ConfigService;

const createFaultInjector = (
  failCount: number,
): PaymentCompletedConsumerFaultInjector =>
  new PaymentCompletedConsumerFaultInjector(createConfigService({ failCount }));

describe('PaymentCompletedConsumer', () => {
  let rabbitMqService: jest.Mocked<
    Pick<RabbitMqService, 'consume' | 'publishMessage'>
  >;
  let processedMessageService: jest.Mocked<
    Pick<ProcessedMessageService, 'processOnce'>
  >;
  let channel: jest.Mocked<Pick<Channel, 'ack' | 'nack'>>;
  let consumer: PaymentCompletedConsumer;
  let logSpy: jest.SpyInstance;
  let warnSpy: jest.SpyInstance;
  let errorSpy: jest.SpyInstance;

  const createConsumer = ({
    failCount = 0,
    maxRetries = MAX_RETRIES,
  }: {
    failCount?: number;
    maxRetries?: number;
  } = {}) =>
    new PaymentCompletedConsumer(
      rabbitMqService as unknown as RabbitMqService,
      createFaultInjector(failCount),
      processedMessageService as unknown as ProcessedMessageService,
      createConfigService({ maxRetries }),
    );

  beforeEach(() => {
    rabbitMqService = {
      consume: jest.fn().mockResolvedValue(undefined),
      publishMessage: jest.fn().mockResolvedValue(undefined),
    };
    processedMessageService = {
      processOnce: jest.fn(
        async (_consumerName, _eventId, _eventType, process) => {
          await process();

          return 'processed';
        },
      ),
    };
    channel = {
      ack: jest.fn(),
      nack: jest.fn(),
    };
    consumer = createConsumer();
    logSpy = jest.spyOn(Logger.prototype, 'log').mockImplementation();
    warnSpy = jest.spyOn(Logger.prototype, 'warn').mockImplementation();
    errorSpy = jest.spyOn(Logger.prototype, 'error').mockImplementation();
  });

  afterEach(() => {
    logSpy.mockRestore();
    warnSpy.mockRestore();
    errorSpy.mockRestore();
  });

  it('registers consumer on the payment completed queue', async () => {
    await consumer.onModuleInit();

    expect(rabbitMqService.consume).toHaveBeenCalledWith(
      PAYMENT_COMPLETED_QUEUE,
      expect.any(Function),
    );
  });

  it.each([0, MAX_RETRIES])(
    'waits for replacement confirmation before ACK at retryCount=%i',
    async (retryCount) => {
      consumer = createConsumer({ failCount: 1 });
      const message = createMessage(JSON.stringify(createEvent(1)), {
        [PAYMENT_COMPLETED_RETRY_COUNT_HEADER]: retryCount,
      });
      let confirm!: () => void;
      rabbitMqService.publishMessage.mockReturnValueOnce(
        new Promise<void>((resolve) => {
          confirm = resolve;
        }),
      );
      const pending = consumer.handleMessage(
        message,
        channel as unknown as Channel,
      );
      await Promise.resolve();
      expect(rabbitMqService.publishMessage).toHaveBeenCalledTimes(1);
      expect(channel.ack).not.toHaveBeenCalled();
      confirm();
      await pending;
      expect(channel.ack).toHaveBeenCalledWith(message);
    },
  );

  it.each([
    [0, 'message nacked'],
    [0, 'confirm timeout'],
    [MAX_RETRIES, 'message nacked'],
    [MAX_RETRIES, 'confirm timeout'],
  ])(
    'does not ACK a failed handoff at retryCount=%i: %s',
    async (retryCount, reason) => {
      consumer = createConsumer({ failCount: 1 });
      const message = createMessage(JSON.stringify(createEvent(1)), {
        [PAYMENT_COMPLETED_RETRY_COUNT_HEADER]: retryCount,
      });
      rabbitMqService.publishMessage.mockRejectedValueOnce(
        new Error(String(reason)),
      );
      await expect(
        consumer.handleMessage(message, channel as unknown as Channel),
      ).rejects.toThrow(String(reason));
      expect(channel.ack).not.toHaveBeenCalled();
      expect(channel.nack).not.toHaveBeenCalled();
    },
  );

  it('does not ACK invalid JSON when the direct DLQ handoff fails', async () => {
    const message = createMessage('{');
    rabbitMqService.publishMessage.mockRejectedValueOnce(
      new Error('confirm timeout'),
    );
    await expect(
      consumer.handleMessage(message, channel as unknown as Channel),
    ).rejects.toThrow('confirm timeout');
    expect(channel.ack).not.toHaveBeenCalled();
  });

  it('acks a valid payment.completed event after processing without retry or DLQ publish', async () => {
    const event = createEvent(1);
    const message = createMessage(JSON.stringify(event));

    await consumer.handleMessage(message, channel as unknown as Channel);

    expect(channel.ack).toHaveBeenCalledWith(message);
    expect(channel.nack).not.toHaveBeenCalled();
    expect(rabbitMqService.publishMessage).not.toHaveBeenCalled();
    expect(processedMessageService.processOnce).toHaveBeenCalledWith(
      PAYMENT_COMPLETED_CONSUMER_NAME,
      event.eventId,
      event.eventType,
      expect.any(Function),
    );
  });

  it('skips duplicate valid events after the first processing and still acks them', async () => {
    const event = createEvent(1);
    const firstMessage = createMessage(JSON.stringify(event));
    const duplicateMessage = createMessage(JSON.stringify(event));
    processedMessageService.processOnce
      .mockImplementationOnce(
        async (_consumerName, _eventId, _eventType, process) => {
          await process();

          return 'processed';
        },
      )
      .mockResolvedValueOnce('duplicate');

    await consumer.handleMessage(firstMessage, channel as unknown as Channel);
    await consumer.handleMessage(
      duplicateMessage,
      channel as unknown as Channel,
    );

    expect(logSpy).toHaveBeenCalledTimes(1);
    expect(logSpy).toHaveBeenCalledWith(
      `Payment completed event consumed: eventId=${event.eventId} paymentId=${event.paymentId} orderId=${event.orderId} amount=${event.amount} providerTransactionId=${event.providerTransactionId}`,
    );
    expect(warnSpy).toHaveBeenCalledWith(
      `Payment completed duplicate event skipped: eventId=${event.eventId}`,
    );
    expect(channel.ack).toHaveBeenCalledTimes(2);
    expect(channel.ack).toHaveBeenNthCalledWith(1, firstMessage);
    expect(channel.ack).toHaveBeenNthCalledWith(2, duplicateMessage);
    expect(channel.nack).not.toHaveBeenCalled();
    expect(rabbitMqService.publishMessage).not.toHaveBeenCalled();
    expect(processedMessageService.processOnce).toHaveBeenCalledTimes(2);
  });

  it('does not ack when processed message persistence fails with a general DB error', async () => {
    const message = createMessage(JSON.stringify(createEvent(1)));
    processedMessageService.processOnce.mockRejectedValueOnce(
      new ProcessedMessagePersistenceError(new Error('database unavailable')),
    );

    await expect(
      consumer.handleMessage(message, channel as unknown as Channel),
    ).rejects.toThrow('Failed to record processed message');

    expect(channel.ack).not.toHaveBeenCalled();
    expect(channel.nack).not.toHaveBeenCalled();
    expect(rabbitMqService.publishMessage).not.toHaveBeenCalled();
  });

  it('publishes the first failed valid event to retry with retry count one before acking', async () => {
    consumer = createConsumer({ failCount: 1 });
    const event = createEvent(1);
    const message = createMessage(JSON.stringify(event));

    await consumer.handleMessage(message, channel as unknown as Channel);

    expect(rabbitMqService.publishMessage).toHaveBeenCalledWith(
      PAYMENT_COMPLETED_RETRY_EXCHANGE,
      PAYMENT_COMPLETED_RETRY_ROUTING_KEY,
      message.content,
      expect.objectContaining({
        contentType: 'application/json',
        messageId: 'message-id',
        type: PAYMENT_COMPLETED_EVENT_TYPE,
        headers: expect.objectContaining({
          [PAYMENT_COMPLETED_RETRY_COUNT_HEADER]: 1,
        }),
      }),
    );
    expect(channel.ack).toHaveBeenCalledWith(message);
    expect(channel.nack).not.toHaveBeenCalled();
    expect(warnSpy).toHaveBeenCalledWith(
      `Payment completed event retry scheduled: eventId=${event.eventId} retry=1/${MAX_RETRIES}`,
    );
    expect(processedMessageService.processOnce).not.toHaveBeenCalled();
  });

  it('increments retry count one to two after a failed retried event', async () => {
    consumer = createConsumer({ failCount: 1 });
    const message = createMessage(JSON.stringify(createEvent(1)), {
      [PAYMENT_COMPLETED_RETRY_COUNT_HEADER]: 1,
    });

    await consumer.handleMessage(message, channel as unknown as Channel);

    expect(rabbitMqService.publishMessage).toHaveBeenCalledWith(
      PAYMENT_COMPLETED_RETRY_EXCHANGE,
      PAYMENT_COMPLETED_RETRY_ROUTING_KEY,
      message.content,
      expect.objectContaining({
        headers: expect.objectContaining({
          [PAYMENT_COMPLETED_RETRY_COUNT_HEADER]: 2,
        }),
      }),
    );
    expect(channel.ack).toHaveBeenCalledWith(message);
    expect(processedMessageService.processOnce).not.toHaveBeenCalled();
  });

  it('increments retry count two to three after a failed retried event', async () => {
    consumer = createConsumer({ failCount: 1 });
    const message = createMessage(JSON.stringify(createEvent(1)), {
      [PAYMENT_COMPLETED_RETRY_COUNT_HEADER]: 2,
    });

    await consumer.handleMessage(message, channel as unknown as Channel);

    expect(rabbitMqService.publishMessage).toHaveBeenCalledWith(
      PAYMENT_COMPLETED_RETRY_EXCHANGE,
      PAYMENT_COMPLETED_RETRY_ROUTING_KEY,
      message.content,
      expect.objectContaining({
        headers: expect.objectContaining({
          [PAYMENT_COMPLETED_RETRY_COUNT_HEADER]: 3,
        }),
      }),
    );
    expect(channel.ack).toHaveBeenCalledWith(message);
    expect(processedMessageService.processOnce).not.toHaveBeenCalled();
  });

  it('moves a failed event to DLQ when retry count already reached max retries', async () => {
    consumer = createConsumer({ failCount: 1 });
    const event = createEvent(1);
    const message = createMessage(JSON.stringify(event), {
      [PAYMENT_COMPLETED_RETRY_COUNT_HEADER]: MAX_RETRIES,
    });

    await consumer.handleMessage(message, channel as unknown as Channel);

    expect(rabbitMqService.publishMessage).toHaveBeenCalledWith(
      PAYMENT_COMPLETED_DLX,
      PAYMENT_COMPLETED_DEAD_ROUTING_KEY,
      message.content,
      expect.objectContaining({
        headers: expect.objectContaining({
          [PAYMENT_COMPLETED_RETRY_COUNT_HEADER]: MAX_RETRIES,
          [PAYMENT_COMPLETED_FAILURE_TYPE_HEADER]: 'processing-failed',
        }),
      }),
    );
    expect(channel.ack).toHaveBeenCalledWith(message);
    expect(warnSpy).toHaveBeenCalledWith(
      `Payment completed event moved to DLQ: eventId=${event.eventId} retryCount=${MAX_RETRIES}`,
    );
    expect(processedMessageService.processOnce).not.toHaveBeenCalled();
  });

  it('moves invalid JSON directly to DLQ without retrying', async () => {
    const message = createMessage('{');

    await consumer.handleMessage(message, channel as unknown as Channel);

    expect(rabbitMqService.publishMessage).toHaveBeenCalledWith(
      PAYMENT_COMPLETED_DLX,
      PAYMENT_COMPLETED_DEAD_ROUTING_KEY,
      message.content,
      expect.objectContaining({
        headers: expect.objectContaining({
          [PAYMENT_COMPLETED_RETRY_COUNT_HEADER]: 0,
          [PAYMENT_COMPLETED_FAILURE_TYPE_HEADER]: 'invalid-json',
        }),
      }),
    );
    expect(rabbitMqService.publishMessage).not.toHaveBeenCalledWith(
      PAYMENT_COMPLETED_RETRY_EXCHANGE,
      expect.any(String),
      expect.any(Buffer),
      expect.any(Object),
    );
    expect(channel.ack).toHaveBeenCalledWith(message);
    expect(channel.nack).not.toHaveBeenCalled();
    expect(processedMessageService.processOnce).not.toHaveBeenCalled();
  });

  it('moves invalid payload directly to DLQ without retrying', async () => {
    const message = createMessage(
      JSON.stringify({
        eventType: PAYMENT_COMPLETED_EVENT_TYPE,
        eventId: 'payment.completed:1',
      }),
    );

    await consumer.handleMessage(message, channel as unknown as Channel);

    expect(rabbitMqService.publishMessage).toHaveBeenCalledWith(
      PAYMENT_COMPLETED_DLX,
      PAYMENT_COMPLETED_DEAD_ROUTING_KEY,
      message.content,
      expect.objectContaining({
        headers: expect.objectContaining({
          [PAYMENT_COMPLETED_RETRY_COUNT_HEADER]: 0,
          [PAYMENT_COMPLETED_FAILURE_TYPE_HEADER]: 'invalid-payload',
        }),
      }),
    );
    expect(rabbitMqService.publishMessage).not.toHaveBeenCalledWith(
      PAYMENT_COMPLETED_RETRY_EXCHANGE,
      expect.any(String),
      expect.any(Buffer),
      expect.any(Object),
    );
    expect(channel.ack).toHaveBeenCalledWith(message);
    expect(channel.nack).not.toHaveBeenCalled();
    expect(processedMessageService.processOnce).not.toHaveBeenCalled();
  });

  it('does not ack the original message when retry publish throws', async () => {
    consumer = createConsumer({ failCount: 1 });
    rabbitMqService.publishMessage.mockRejectedValueOnce(
      new Error('retry publish failed'),
    );
    const message = createMessage(JSON.stringify(createEvent(1)));

    await expect(
      consumer.handleMessage(message, channel as unknown as Channel),
    ).rejects.toThrow('retry publish failed');

    expect(channel.ack).not.toHaveBeenCalled();
    expect(channel.nack).not.toHaveBeenCalled();
    expect(processedMessageService.processOnce).not.toHaveBeenCalled();
  });

  it('does not ack the original message when DLQ publish throws', async () => {
    consumer = createConsumer({ failCount: 1 });
    rabbitMqService.publishMessage.mockRejectedValueOnce(
      new Error('DLQ publish failed'),
    );
    const message = createMessage(JSON.stringify(createEvent(1)), {
      [PAYMENT_COMPLETED_RETRY_COUNT_HEADER]: MAX_RETRIES,
    });

    await expect(
      consumer.handleMessage(message, channel as unknown as Channel),
    ).rejects.toThrow('DLQ publish failed');

    expect(channel.ack).not.toHaveBeenCalled();
    expect(channel.nack).not.toHaveBeenCalled();
    expect(processedMessageService.processOnce).not.toHaveBeenCalled();
  });

  it('acks a retried event that later succeeds without scheduling another retry', async () => {
    consumer = createConsumer({ failCount: 1 });
    const failedMessage = createMessage(JSON.stringify(createEvent(1)));
    const successfulMessage = createMessage(JSON.stringify(createEvent(1)), {
      [PAYMENT_COMPLETED_RETRY_COUNT_HEADER]: 1,
    });

    await consumer.handleMessage(failedMessage, channel as unknown as Channel);
    await consumer.handleMessage(
      successfulMessage,
      channel as unknown as Channel,
    );

    expect(rabbitMqService.publishMessage).toHaveBeenCalledTimes(1);
    expect(rabbitMqService.publishMessage).toHaveBeenCalledWith(
      PAYMENT_COMPLETED_RETRY_EXCHANGE,
      PAYMENT_COMPLETED_RETRY_ROUTING_KEY,
      failedMessage.content,
      expect.any(Object),
    );
    expect(channel.ack).toHaveBeenCalledWith(failedMessage);
    expect(channel.ack).toHaveBeenCalledWith(successfulMessage);
    expect(processedMessageService.processOnce).toHaveBeenCalledTimes(1);
  });

  it('applies fault injection after JSON parsing and event validation', async () => {
    consumer = createConsumer({ failCount: 1 });
    const invalidPayloadMessage = createMessage(
      JSON.stringify({
        eventType: PAYMENT_COMPLETED_EVENT_TYPE,
        eventId: 'payment.completed:1',
      }),
    );
    const failedValidMessage = createMessage(JSON.stringify(createEvent(1)));
    const successfulValidMessage = createMessage(
      JSON.stringify(createEvent(2)),
    );

    await consumer.handleMessage(
      invalidPayloadMessage,
      channel as unknown as Channel,
    );
    await consumer.handleMessage(
      failedValidMessage,
      channel as unknown as Channel,
    );
    await consumer.handleMessage(
      successfulValidMessage,
      channel as unknown as Channel,
    );

    expect(rabbitMqService.publishMessage).toHaveBeenNthCalledWith(
      1,
      PAYMENT_COMPLETED_DLX,
      PAYMENT_COMPLETED_DEAD_ROUTING_KEY,
      invalidPayloadMessage.content,
      expect.any(Object),
    );
    expect(rabbitMqService.publishMessage).toHaveBeenNthCalledWith(
      2,
      PAYMENT_COMPLETED_RETRY_EXCHANGE,
      PAYMENT_COMPLETED_RETRY_ROUTING_KEY,
      failedValidMessage.content,
      expect.any(Object),
    );
    expect(channel.ack).toHaveBeenCalledWith(invalidPayloadMessage);
    expect(channel.ack).toHaveBeenCalledWith(failedValidMessage);
    expect(channel.ack).toHaveBeenCalledWith(successfulValidMessage);
    expect(errorSpy).toHaveBeenCalledWith(
      `Payment completed event handling failed: ${PAYMENT_COMPLETED_CONSUMER_FAILURE_MESSAGE}`,
    );
    expect(processedMessageService.processOnce).toHaveBeenCalledTimes(1);
  });
});
