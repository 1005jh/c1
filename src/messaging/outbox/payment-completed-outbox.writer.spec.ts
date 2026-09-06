import { EntityManager, Repository } from 'typeorm';
import { PaymentStatus } from '../../payments/entities/payment-status.enum';
import { Payment } from '../../payments/entities/payment.entity';
import {
  PAYMENT_COMPLETED_EVENT_TYPE,
  PAYMENT_COMPLETED_EVENT_VERSION,
} from '../events/payment-completed.event';
import { OutboxEvent } from './entities/outbox-event.entity';
import { OutboxEventStatus } from './entities/outbox-event-status.enum';
import { PaymentCompletedOutboxWriter } from './payment-completed-outbox.writer';

type MockRepository<T = unknown> = Partial<
  Record<keyof Repository<T>, jest.Mock>
>;

describe('PaymentCompletedOutboxWriter', () => {
  let writer: PaymentCompletedOutboxWriter;
  let outboxRepository: MockRepository<OutboxEvent>;
  let manager: jest.Mocked<Pick<EntityManager, 'getRepository'>>;

  beforeEach(() => {
    outboxRepository = {
      create: jest.fn((value) => value),
      save: jest.fn(async (value) => ({ id: 1, ...value })),
    };
    manager = {
      getRepository: jest.fn((entity) => {
        if (entity === OutboxEvent) {
          return outboxRepository as Repository<OutboxEvent>;
        }

        throw new Error('Unexpected repository');
      }),
    };
    writer = new PaymentCompletedOutboxWriter();
  });

  it('stores a pending payment.completed event snapshot for a successful payment', async () => {
    const payment = {
      id: 10,
      orderId: 20,
      amount: 30000,
      status: PaymentStatus.SUCCESS,
      providerTransactionId: 'tx_10',
    } as Payment;

    await expect(
      writer.enqueue(manager as unknown as EntityManager, payment),
    ).resolves.toMatchObject({
      id: 1,
      eventId: 'payment.completed:10',
      eventType: PAYMENT_COMPLETED_EVENT_TYPE,
      eventVersion: PAYMENT_COMPLETED_EVENT_VERSION,
      status: OutboxEventStatus.PENDING,
      attempts: 0,
      lastError: null,
      publishedAt: null,
    });

    expect(manager.getRepository).toHaveBeenCalledWith(OutboxEvent);
    expect(outboxRepository.create).toHaveBeenCalledWith({
      eventId: 'payment.completed:10',
      eventType: PAYMENT_COMPLETED_EVENT_TYPE,
      eventVersion: PAYMENT_COMPLETED_EVENT_VERSION,
      payload: {
        eventId: 'payment.completed:10',
        eventType: PAYMENT_COMPLETED_EVENT_TYPE,
        eventVersion: PAYMENT_COMPLETED_EVENT_VERSION,
        occurredAt: expect.any(String),
        paymentId: 10,
        orderId: 20,
        amount: 30000,
        providerTransactionId: 'tx_10',
      },
      status: OutboxEventStatus.PENDING,
      attempts: 0,
      lastError: null,
      publishedAt: null,
    });
    expect(outboxRepository.save).toHaveBeenCalledTimes(1);
  });

  it('keeps event metadata aligned with the stored payload', async () => {
    const payment = {
      id: 11,
      orderId: 21,
      amount: 40000,
      status: PaymentStatus.SUCCESS,
      providerTransactionId: 'tx_11',
    } as Payment;

    await writer.enqueue(manager as unknown as EntityManager, payment);

    const created = outboxRepository.create?.mock.calls[0][0] as OutboxEvent;
    expect(created.eventId).toBe(created.payload.eventId);
    expect(created.eventType).toBe(created.payload.eventType);
    expect(created.eventVersion).toBe(created.payload.eventVersion);
  });

  it('rejects event creation when provider transaction id is missing', async () => {
    const payment = {
      id: 12,
      orderId: 22,
      amount: 50000,
      status: PaymentStatus.UNKNOWN,
      providerTransactionId: null,
    } as Payment;

    await expect(
      writer.enqueue(manager as unknown as EntityManager, payment),
    ).rejects.toThrow('Payment completed event requires providerTransactionId');

    expect(manager.getRepository).not.toHaveBeenCalled();
    expect(outboxRepository.save).not.toHaveBeenCalled();
  });
});
