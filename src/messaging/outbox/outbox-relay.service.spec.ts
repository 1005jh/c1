import { Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { DataSource, Repository } from 'typeorm';
import {
  PAYMENT_COMPLETED_EVENT_TYPE,
  PAYMENT_COMPLETED_EVENT_VERSION,
  PaymentCompletedEvent,
} from '../events/payment-completed.event';
import { PaymentCompletedPublisher } from '../events/payment-completed.publisher';
import { OutboxEvent } from './entities/outbox-event.entity';
import { OutboxEventStatus } from './entities/outbox-event-status.enum';
import { OutboxRelayService } from './outbox-relay.service';
import {
  OUTBOX_MARK_PUBLISHED_FAILURE_MESSAGE,
  OutboxMarkPublishedFaultInjector,
} from './outbox-mark-published-fault-injector';

type MockRepository<T = unknown> = Partial<
  Record<keyof Repository<T>, jest.Mock>
>;

const createPaymentCompletedEvent = (id = 1): PaymentCompletedEvent => ({
  eventId: `payment.completed:${id}`,
  eventType: PAYMENT_COMPLETED_EVENT_TYPE,
  eventVersion: PAYMENT_COMPLETED_EVENT_VERSION,
  occurredAt: '2026-09-06T00:00:00.000Z',
  paymentId: id,
  orderId: id + 100,
  amount: 30000,
  providerTransactionId: `tx_${id}`,
});

const createOutboxEvent = (
  id = 1,
  overrides: Partial<OutboxEvent> = {},
): OutboxEvent =>
  ({
    id,
    eventId: `payment.completed:${id}`,
    eventType: PAYMENT_COMPLETED_EVENT_TYPE,
    eventVersion: PAYMENT_COMPLETED_EVENT_VERSION,
    payload: createPaymentCompletedEvent(id),
    status: OutboxEventStatus.PENDING,
    attempts: 0,
    lastError: null,
    publishedAt: null,
    createdAt: new Date('2026-09-06T00:00:00.000Z'),
    updatedAt: new Date('2026-09-06T00:00:00.000Z'),
    ...overrides,
  }) as OutboxEvent;

const createConfigService = ({
  enabled = 'true',
  intervalMs = 1000,
  batchSize = 20,
  markFailCount = 0,
}: {
  enabled?: string;
  intervalMs?: number;
  batchSize?: number;
  markFailCount?: number;
} = {}): ConfigService =>
  ({
    get: jest.fn((key: string) => {
      if (key === 'OUTBOX_MARK_PUBLISHED_FAIL_COUNT') {
        return markFailCount;
      }
      if (key === 'OUTBOX_RELAY_ENABLED') {
        return enabled;
      }

      if (key === 'OUTBOX_RELAY_INTERVAL_MS') {
        return intervalMs;
      }

      if (key === 'OUTBOX_RELAY_BATCH_SIZE') {
        return batchSize;
      }

      return undefined;
    }),
  }) as unknown as ConfigService;

describe('OutboxRelayService', () => {
  let repository: MockRepository<OutboxEvent>;
  let dataSource: jest.Mocked<Pick<DataSource, 'getRepository'>>;
  let publisher: jest.Mocked<Pick<PaymentCompletedPublisher, 'publish'>>;
  let service: OutboxRelayService;
  let warnSpy: jest.SpyInstance;

  const createService = (
    configService: ConfigService = createConfigService(),
  ) => {
    service = new OutboxRelayService(
      dataSource as unknown as DataSource,
      publisher as unknown as PaymentCompletedPublisher,
      configService,
      new OutboxMarkPublishedFaultInjector(configService),
    );
  };

  beforeEach(() => {
    repository = {
      find: jest.fn().mockResolvedValue([]),
      update: jest.fn().mockResolvedValue({ affected: 1 }),
    };
    dataSource = {
      getRepository: jest.fn((entity) => {
        if (entity === OutboxEvent) {
          return repository as Repository<OutboxEvent>;
        }

        throw new Error('Unexpected repository');
      }),
    };
    publisher = {
      publish: jest.fn().mockResolvedValue(undefined),
    };
    warnSpy = jest.spyOn(Logger.prototype, 'warn').mockImplementation();
    createService();
  });

  afterEach(() => {
    warnSpy.mockRestore();
    jest.useRealTimers();
    jest.restoreAllMocks();
  });

  it('publishes pending events and marks them published', async () => {
    const event = createOutboxEvent(1);
    repository.find?.mockResolvedValueOnce([event]);

    await service.runOnce();

    expect(repository.find).toHaveBeenCalledWith({
      where: { status: OutboxEventStatus.PENDING },
      order: { id: 'ASC' },
      take: 20,
    });
    expect(publisher.publish).toHaveBeenCalledWith(event.payload);
    expect(repository.update).toHaveBeenCalledWith(event.id, {
      status: OutboxEventStatus.PUBLISHED,
      attempts: 1,
      lastError: null,
      publishedAt: expect.any(Date),
    });
  });

  it('does not update the Outbox before the publisher resolves', async () => {
    const event = createOutboxEvent(1);
    let confirm!: () => void;
    publisher.publish.mockReturnValueOnce(
      new Promise((resolve) => {
        confirm = () => resolve(event.payload);
      }),
    );
    repository.find?.mockResolvedValueOnce([event]);
    const pending = service.runOnce();
    await Promise.resolve();
    expect(repository.update).not.toHaveBeenCalled();
    confirm();
    await pending;
    expect(repository.update).toHaveBeenCalledWith(
      event.id,
      expect.objectContaining({ status: OutboxEventStatus.PUBLISHED }),
    );
  });

  it('keeps a confirmed publication pending on injected mark failure and republishes on the next run', async () => {
    createService(createConfigService({ markFailCount: 1 }));
    const first = createOutboxEvent(1);
    const second = createOutboxEvent(1, {
      attempts: 1,
      lastError: OUTBOX_MARK_PUBLISHED_FAILURE_MESSAGE,
    });
    repository.find
      ?.mockResolvedValueOnce([first])
      .mockResolvedValueOnce([second]);
    await service.runOnce();
    expect(publisher.publish).toHaveBeenCalledWith(first.payload);
    expect(repository.update).toHaveBeenNthCalledWith(1, first.id, {
      status: OutboxEventStatus.PENDING,
      attempts: 1,
      lastError: OUTBOX_MARK_PUBLISHED_FAILURE_MESSAGE,
    });
    await service.runOnce();
    expect(publisher.publish).toHaveBeenNthCalledWith(2, first.payload);
    expect(repository.update).toHaveBeenNthCalledWith(2, first.id, {
      status: OutboxEventStatus.PUBLISHED,
      attempts: 2,
      lastError: null,
      publishedAt: expect.any(Date) as Date,
    });
  });

  it('does not consume a post-confirm fault on a publisher rejection', async () => {
    createService(createConfigService({ markFailCount: 1 }));
    repository.find?.mockResolvedValueOnce([
      createOutboxEvent(1),
      createOutboxEvent(2),
      createOutboxEvent(3),
    ]);
    publisher.publish.mockRejectedValueOnce(new Error('NACK'));
    await service.runOnce();
    expect(repository.update).toHaveBeenNthCalledWith(
      1,
      1,
      expect.objectContaining({
        status: OutboxEventStatus.PENDING,
        lastError: 'NACK',
      }),
    );
    expect(repository.update).toHaveBeenNthCalledWith(
      2,
      2,
      expect.objectContaining({
        status: OutboxEventStatus.PENDING,
        lastError: OUTBOX_MARK_PUBLISHED_FAILURE_MESSAGE,
      }),
    );
    expect(repository.update).toHaveBeenNthCalledWith(
      3,
      3,
      expect.objectContaining({ status: OutboxEventStatus.PUBLISHED }),
    );
  });

  it('keeps failed publish events pending and records the error', async () => {
    const event = createOutboxEvent(1);
    repository.find?.mockResolvedValueOnce([event]);
    publisher.publish.mockRejectedValueOnce(new Error('publish failed'));

    await service.runOnce();

    expect(repository.update).toHaveBeenCalledWith(event.id, {
      status: OutboxEventStatus.PENDING,
      attempts: 1,
      lastError: 'publish failed',
    });
    expect(warnSpy).toHaveBeenCalledWith(
      `Outbox event publish failed: id=${event.id} eventId=${event.eventId} error=publish failed`,
    );
  });

  it('marks a previously failed pending event as published on the next run', async () => {
    const firstAttempt = createOutboxEvent(1, { attempts: 0 });
    const secondAttempt = createOutboxEvent(1, {
      attempts: 1,
      lastError: 'publish failed',
    });
    repository.find
      ?.mockResolvedValueOnce([firstAttempt])
      .mockResolvedValueOnce([secondAttempt]);
    publisher.publish
      .mockRejectedValueOnce(new Error('publish failed'))
      .mockResolvedValueOnce(undefined);

    await service.runOnce();
    await service.runOnce();

    expect(repository.update).toHaveBeenNthCalledWith(1, firstAttempt.id, {
      status: OutboxEventStatus.PENDING,
      attempts: 1,
      lastError: 'publish failed',
    });
    expect(repository.update).toHaveBeenNthCalledWith(2, secondAttempt.id, {
      status: OutboxEventStatus.PUBLISHED,
      attempts: 2,
      lastError: null,
      publishedAt: expect.any(Date),
    });
  });

  it('continues processing later pending rows when one row fails', async () => {
    const failed = createOutboxEvent(1);
    const succeeded = createOutboxEvent(2);
    repository.find?.mockResolvedValueOnce([failed, succeeded]);
    publisher.publish
      .mockRejectedValueOnce(new Error('publish failed'))
      .mockResolvedValueOnce(undefined);

    await service.runOnce();

    expect(publisher.publish).toHaveBeenCalledTimes(2);
    expect(repository.update).toHaveBeenNthCalledWith(1, failed.id, {
      status: OutboxEventStatus.PENDING,
      attempts: 1,
      lastError: 'publish failed',
    });
    expect(repository.update).toHaveBeenNthCalledWith(2, succeeded.id, {
      status: OutboxEventStatus.PUBLISHED,
      attempts: 1,
      lastError: null,
      publishedAt: expect.any(Date),
    });
  });

  it('keeps invalid payload pending without publishing', async () => {
    const event = createOutboxEvent(1, {
      payload: {
        eventType: PAYMENT_COMPLETED_EVENT_TYPE,
        eventId: 'payment.completed:1',
      } as PaymentCompletedEvent,
    });
    repository.find?.mockResolvedValueOnce([event]);

    await service.runOnce();

    expect(publisher.publish).not.toHaveBeenCalled();
    expect(repository.update).toHaveBeenCalledWith(event.id, {
      status: OutboxEventStatus.PENDING,
      attempts: 1,
      lastError: 'Invalid payment.completed outbox payload',
    });
  });

  it('keeps unsupported event types pending without publishing', async () => {
    const event = createOutboxEvent(1, {
      eventType: 'product.created',
    });
    repository.find?.mockResolvedValueOnce([event]);

    await service.runOnce();

    expect(publisher.publish).not.toHaveBeenCalled();
    expect(repository.update).toHaveBeenCalledWith(event.id, {
      status: OutboxEventStatus.PENDING,
      attempts: 1,
      lastError: 'Unsupported outbox event type: product.created',
    });
  });

  it('queries only pending rows', async () => {
    await service.runOnce();

    expect(repository.find).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { status: OutboxEventStatus.PENDING },
      }),
    );
    expect(publisher.publish).not.toHaveBeenCalled();
    expect(repository.update).not.toHaveBeenCalled();
  });

  it('does not start the relay timer when disabled', () => {
    const setIntervalSpy = jest.spyOn(global, 'setInterval');
    createService(createConfigService({ enabled: 'false' }));

    service.onModuleInit();

    expect(setIntervalSpy).not.toHaveBeenCalled();
  });

  it('prevents overlapping runOnce executions', async () => {
    const event = createOutboxEvent(1);
    let releasePublish: () => void;
    const publishGate = new Promise<void>((resolve) => {
      releasePublish = resolve;
    });
    repository.find?.mockResolvedValue([event]);
    publisher.publish.mockReturnValue(publishGate);

    const firstRun = service.runOnce();
    const secondRun = service.runOnce();
    releasePublish!();
    await Promise.all([firstRun, secondRun]);

    expect(repository.find).toHaveBeenCalledTimes(1);
    expect(publisher.publish).toHaveBeenCalledTimes(1);
  });
});
