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

describe('PaymentCompletedPublisher', () => {
  let rabbitMqService: jest.Mocked<Pick<RabbitMqService, 'publishJson'>>;
  let publisher: PaymentCompletedPublisher;

  beforeEach(() => {
    rabbitMqService = {
      publishJson: jest.fn().mockResolvedValue(undefined),
    };
    publisher = new PaymentCompletedPublisher(
      rabbitMqService as unknown as RabbitMqService,
    );
  });

  it('publishes payment.completed event with expected exchange, routing key, and payload', async () => {
    const payment = {
      id: 1,
      orderId: 2,
      amount: 30000,
      status: PaymentStatus.SUCCESS,
      providerTransactionId: 'tx_1',
    } as Payment;

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
