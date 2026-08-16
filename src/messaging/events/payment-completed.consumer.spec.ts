import { Logger } from '@nestjs/common';
import { Channel, ConsumeMessage } from 'amqplib';
import { PAYMENT_COMPLETED_QUEUE } from '../rabbitmq/rabbitmq.constants';
import { RabbitMqService } from '../rabbitmq/rabbitmq.service';
import {
  PAYMENT_COMPLETED_EVENT_TYPE,
  PAYMENT_COMPLETED_EVENT_VERSION,
  PaymentCompletedEvent,
} from './payment-completed.event';
import { PaymentCompletedConsumer } from './payment-completed.consumer';

const createMessage = (content: string): ConsumeMessage =>
  ({
    content: Buffer.from(content),
  }) as ConsumeMessage;

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
    const event: PaymentCompletedEvent = {
      eventId: 'payment.completed:1',
      eventType: PAYMENT_COMPLETED_EVENT_TYPE,
      eventVersion: PAYMENT_COMPLETED_EVENT_VERSION,
      occurredAt: new Date().toISOString(),
      paymentId: 1,
      orderId: 2,
      amount: 30000,
      providerTransactionId: 'tx_1',
    };
    const message = createMessage(JSON.stringify(event));

    consumer.handleMessage(message, channel as unknown as Channel);

    expect(channel.ack).toHaveBeenCalledWith(message);
    expect(channel.nack).not.toHaveBeenCalled();
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
});
