import { ConfigService } from '@nestjs/config';
import { PaymentStatus } from '../../payments/entities/payment-status.enum';
import { Payment } from '../../payments/entities/payment.entity';
import {
  COMMERCE_EVENTS_EXCHANGE,
  PAYMENT_COMPLETED_ROUTING_KEY,
} from '../rabbitmq/rabbitmq.constants';
import { RabbitMqService } from '../rabbitmq/rabbitmq.service';
import {
  PAYMENT_COMPLETED_EVENT_TYPE,
  PAYMENT_COMPLETED_EVENT_VERSION,
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

  const createSuccessPayment = (id = 1) =>
    ({
      id,
      orderId: 2,
      amount: 30000,
      status: PaymentStatus.SUCCESS,
      providerTransactionId: `tx_${id}`,
    }) as Payment;

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
    const payment = createSuccessPayment();

    await expect(publisher.publish(payment)).resolves.toMatchObject({
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
        occurredAt: expect.any(String),
        paymentId: 1,
        orderId: 2,
        amount: 30000,
        providerTransactionId: 'tx_1',
      },
    );
  });

  it('keeps the payment.completed event contract free of publish fault fields', async () => {
    const payment = createSuccessPayment();

    await expect(publisher.publish(payment)).resolves.not.toEqual(
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
    const payment = createSuccessPayment();

    await expect(publisher.publish(payment)).resolves.toMatchObject({
      eventId: 'payment.completed:1',
    });

    expect(rabbitMqService.publishJson).toHaveBeenCalledTimes(1);
  });

  it('fails before RabbitMQ publishJson while publish fail count remains', async () => {
    createPublisher(1);
    const payment = createSuccessPayment();

    await expect(publisher.publish(payment)).rejects.toThrow(
      PAYMENT_COMPLETED_PUBLISH_FAILURE_MESSAGE,
    );

    expect(rabbitMqService.publishJson).not.toHaveBeenCalled();
  });

  it('publishes the next event after the configured publish failure is consumed', async () => {
    createPublisher(1);
    const failedPayment = createSuccessPayment(1);
    const secondPayment = createSuccessPayment(2);

    await expect(publisher.publish(failedPayment)).rejects.toThrow(
      PAYMENT_COMPLETED_PUBLISH_FAILURE_MESSAGE,
    );
    await expect(publisher.publish(secondPayment)).resolves.toMatchObject({
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

  it('rejects completed event creation when provider transaction id is missing', async () => {
    const payment = {
      id: 1,
      orderId: 2,
      amount: 30000,
      status: PaymentStatus.UNKNOWN,
      providerTransactionId: null,
    } as Payment;

    await expect(publisher.publish(payment)).rejects.toThrow(
      'Payment completed event requires providerTransactionId',
    );
    expect(rabbitMqService.publishJson).not.toHaveBeenCalled();
  });
});
