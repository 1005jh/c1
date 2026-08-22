import { Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Channel, ConsumeMessage } from 'amqplib';
import { PAYMENT_COMPLETED_QUEUE } from '../rabbitmq/rabbitmq.constants';
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
import { PaymentCompletedConsumer } from './payment-completed.consumer';

const createMessage = (content: string): ConsumeMessage =>
  ({
    content: Buffer.from(content),
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

const createFaultInjector = (
  failCount: number,
): PaymentCompletedConsumerFaultInjector =>
  new PaymentCompletedConsumerFaultInjector({
    get: jest.fn((key: string) =>
      key === 'PAYMENT_COMPLETED_CONSUMER_FAIL_COUNT' ? failCount : undefined,
    ),
  } as unknown as ConfigService);

describe('PaymentCompletedConsumer', () => {
  let rabbitMqService: jest.Mocked<Pick<RabbitMqService, 'consume'>>;
  let channel: jest.Mocked<Pick<Channel, 'ack' | 'nack'>>;
  let consumer: PaymentCompletedConsumer;
  let logSpy: jest.SpyInstance;
  let errorSpy: jest.SpyInstance;

  beforeEach(() => {
    rabbitMqService = {
      consume: jest.fn().mockResolvedValue(undefined),
    };
    channel = {
      ack: jest.fn(),
      nack: jest.fn(),
    };
    consumer = new PaymentCompletedConsumer(
      rabbitMqService as unknown as RabbitMqService,
      createFaultInjector(0),
    );
    logSpy = jest.spyOn(Logger.prototype, 'log').mockImplementation();
    errorSpy = jest.spyOn(Logger.prototype, 'error').mockImplementation();
  });

  afterEach(() => {
    logSpy.mockRestore();
    errorSpy.mockRestore();
  });

  it('registers consumer on the payment completed queue', async () => {
    await consumer.onModuleInit();

    expect(rabbitMqService.consume).toHaveBeenCalledWith(
      PAYMENT_COMPLETED_QUEUE,
      expect.any(Function),
    );
  });

  it('acks a valid payment.completed event after processing', () => {
    const event = createEvent(1);
    const message = createMessage(JSON.stringify(event));

    consumer.handleMessage(message, channel as unknown as Channel);

    expect(channel.ack).toHaveBeenCalledWith(message);
    expect(channel.nack).not.toHaveBeenCalled();
  });

  it('nacks the first valid event without requeueing when fail count is one', () => {
    consumer = new PaymentCompletedConsumer(
      rabbitMqService as unknown as RabbitMqService,
      createFaultInjector(1),
    );
    const message = createMessage(JSON.stringify(createEvent(1)));

    consumer.handleMessage(message, channel as unknown as Channel);

    expect(channel.ack).not.toHaveBeenCalled();
    expect(channel.nack).toHaveBeenCalledWith(message, false, false);
    expect(errorSpy).toHaveBeenCalledWith(
      `Payment completed event handling failed: ${PAYMENT_COMPLETED_CONSUMER_FAILURE_MESSAGE}`,
    );
  });

  it('processes the second valid event after one injected failure is consumed', () => {
    consumer = new PaymentCompletedConsumer(
      rabbitMqService as unknown as RabbitMqService,
      createFaultInjector(1),
    );
    const failedMessage = createMessage(JSON.stringify(createEvent(1)));
    const successfulMessage = createMessage(JSON.stringify(createEvent(2)));

    consumer.handleMessage(failedMessage, channel as unknown as Channel);
    consumer.handleMessage(successfulMessage, channel as unknown as Channel);

    expect(channel.nack).toHaveBeenCalledWith(failedMessage, false, false);
    expect(channel.ack).toHaveBeenCalledWith(successfulMessage);
  });

  it('nacks invalid JSON without requeueing', () => {
    const message = createMessage('{');

    consumer.handleMessage(message, channel as unknown as Channel);

    expect(channel.ack).not.toHaveBeenCalled();
    expect(channel.nack).toHaveBeenCalledWith(message, false, false);
  });

  it('nacks invalid payload without requeueing', () => {
    const message = createMessage(
      JSON.stringify({
        eventType: PAYMENT_COMPLETED_EVENT_TYPE,
        eventId: 'payment.completed:1',
      }),
    );

    consumer.handleMessage(message, channel as unknown as Channel);

    expect(channel.ack).not.toHaveBeenCalled();
    expect(channel.nack).toHaveBeenCalledWith(message, false, false);
  });

  it('applies fault injection after JSON parsing and event validation', () => {
    consumer = new PaymentCompletedConsumer(
      rabbitMqService as unknown as RabbitMqService,
      createFaultInjector(1),
    );
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

    consumer.handleMessage(
      invalidPayloadMessage,
      channel as unknown as Channel,
    );
    consumer.handleMessage(failedValidMessage, channel as unknown as Channel);
    consumer.handleMessage(
      successfulValidMessage,
      channel as unknown as Channel,
    );

    expect(channel.nack).toHaveBeenCalledWith(
      invalidPayloadMessage,
      false,
      false,
    );
    expect(channel.nack).toHaveBeenCalledWith(failedValidMessage, false, false);
    expect(channel.ack).toHaveBeenCalledWith(successfulValidMessage);
  });
});
