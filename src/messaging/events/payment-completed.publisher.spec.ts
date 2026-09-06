import { ConfigService } from '@nestjs/config';
import {
  COMMERCE_EVENTS_EXCHANGE,
  PAYMENT_COMPLETED_ROUTING_KEY,
} from '../rabbitmq/rabbitmq.constants';
import { RabbitMqService } from '../rabbitmq/rabbitmq.service';
import {
  PAYMENT_COMPLETED_EVENT_TYPE,
  PAYMENT_COMPLETED_EVENT_VERSION,
  PaymentCompletedEvent,
} from './payment-completed.event';
import { PaymentCompletedPublisher } from './payment-completed.publisher';
import {
  PAYMENT_COMPLETED_PUBLISH_FAILURE_MESSAGE,
  PaymentCompletedPublisherFaultInjector,
} from './payment-completed.publisher-fault-injector';

describe('PaymentCompletedPublisher', () => {
  let rabbitMqService: jest.Mocked<Pick<RabbitMqService, 'publishJson'>>;
  let publisher: PaymentCompletedPublisher;
  let faultInjector: PaymentCompletedPublisherFaultInjector;

  const createConfigService = (publishFailCount: number) =>
    ({
      get: jest.fn((key: string) =>
        key === 'PAYMENT_COMPLETED_PUBLISH_FAIL_COUNT'
          ? publishFailCount
          : undefined,
      ),
    }) as unknown as ConfigService;

  const createEvent = (id = 1): PaymentCompletedEvent =>
    ({
      eventId: `payment.completed:${id}`,
      eventType: PAYMENT_COMPLETED_EVENT_TYPE,
      eventVersion: PAYMENT_COMPLETED_EVENT_VERSION,
      occurredAt: '2026-09-06T00:00:00.000Z',
      paymentId: id,
      orderId: 2,
      amount: 30000,
      providerTransactionId: `tx_${id}`,
    }) as PaymentCompletedEvent;

  const createPublisher = (publishFailCount: number) => {
    rabbitMqService = {
      publishJson: jest.fn().mockResolvedValue(undefined),
    };
    faultInjector = new PaymentCompletedPublisherFaultInjector(
      createConfigService(publishFailCount),
    );
    publisher = new PaymentCompletedPublisher(
      rabbitMqService as unknown as RabbitMqService,
      faultInjector,
    );
  };

  beforeEach(() => {
    createPublisher(0);
  });

  it('publishes payment.completed event with expected exchange, routing key, and payload', async () => {
    const event = createEvent();

    await expect(publisher.publish(event)).resolves.toMatchObject({
      eventId: 'payment.completed:1',
      eventType: PAYMENT_COMPLETED_EVENT_TYPE,
      eventVersion: PAYMENT_COMPLETED_EVENT_VERSION,
      paymentId: 1,
      orderId: 2,
      amount: 30000,
      providerTransactionId: 'tx_1',
    });

    expect(rabbitMqService.publishJson).toHaveBeenCalledWith(
      COMMERCE_EVENTS_EXCHANGE,
      PAYMENT_COMPLETED_ROUTING_KEY,
      {
        eventId: 'payment.completed:1',
        eventType: PAYMENT_COMPLETED_EVENT_TYPE,
        eventVersion: PAYMENT_COMPLETED_EVENT_VERSION,
        occurredAt: '2026-09-06T00:00:00.000Z',
        paymentId: 1,
        orderId: 2,
        amount: 30000,
        providerTransactionId: 'tx_1',
      },
    );
  });

  it('keeps the payment.completed event contract free of publish fault fields', async () => {
    const event = createEvent();

    await expect(publisher.publish(event)).resolves.not.toEqual(
      expect.objectContaining({
        shouldFail: expect.anything(),
        testMode: expect.anything(),
        publishFailure: expect.anything(),
      }),
    );

    expect(rabbitMqService.publishJson).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(String),
      expect.not.objectContaining({
        shouldFail: expect.anything(),
        testMode: expect.anything(),
        publishFailure: expect.anything(),
      }),
    );
  });

  it('publishes normally when publish fail count is zero', async () => {
    createPublisher(0);
    const event = createEvent();

    await expect(publisher.publish(event)).resolves.toMatchObject({
      eventId: 'payment.completed:1',
    });

    expect(rabbitMqService.publishJson).toHaveBeenCalledTimes(1);
  });

  it('fails before RabbitMQ publishJson while publish fail count remains', async () => {
    createPublisher(1);
    const event = createEvent();

    await expect(publisher.publish(event)).rejects.toThrow(
      PAYMENT_COMPLETED_PUBLISH_FAILURE_MESSAGE,
    );

    expect(rabbitMqService.publishJson).not.toHaveBeenCalled();
  });

  it('publishes the next event after the configured publish failure is consumed', async () => {
    createPublisher(1);
    const failedEvent = createEvent(1);
    const secondEvent = createEvent(2);

    await expect(publisher.publish(failedEvent)).rejects.toThrow(
      PAYMENT_COMPLETED_PUBLISH_FAILURE_MESSAGE,
    );
    await expect(publisher.publish(secondEvent)).resolves.toMatchObject({
      eventId: 'payment.completed:2',
    });

    expect(rabbitMqService.publishJson).toHaveBeenCalledTimes(1);
    expect(rabbitMqService.publishJson).toHaveBeenCalledWith(
      COMMERCE_EVENTS_EXCHANGE,
      PAYMENT_COMPLETED_ROUTING_KEY,
      expect.objectContaining({
        eventId: 'payment.completed:2',
      }),
    );
  });
});
