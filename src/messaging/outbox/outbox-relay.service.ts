import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { DataSource } from 'typeorm';
import {
  PAYMENT_COMPLETED_EVENT_TYPE,
  isPaymentCompletedEvent,
} from '../events/payment-completed.event';
import { PaymentCompletedPublisher } from '../events/payment-completed.publisher';
import { OutboxEvent } from './entities/outbox-event.entity';
import { OutboxEventStatus } from './entities/outbox-event-status.enum';
import { OutboxMarkPublishedFaultInjector } from './outbox-mark-published-fault-injector';

@Injectable()
export class OutboxRelayService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxRelayService.name);
  private readonly enabled: boolean;
  private readonly intervalMs: number;
  private readonly batchSize: number;
  private running = false;
  private timer?: NodeJS.Timeout;

  constructor(
    private readonly dataSource: DataSource,
    private readonly paymentCompletedPublisher: PaymentCompletedPublisher,
    configService: ConfigService,
    private readonly markPublishedFaultInjector: OutboxMarkPublishedFaultInjector,
  ) {
    this.enabled =
      configService.get<string>('OUTBOX_RELAY_ENABLED') !== 'false';
    this.intervalMs = Number(
      configService.get<number>('OUTBOX_RELAY_INTERVAL_MS') ?? 1000,
    );
    this.batchSize = Number(
      configService.get<number>('OUTBOX_RELAY_BATCH_SIZE') ?? 20,
    );
  }

  onModuleInit(): void {
    if (!this.enabled) {
      return;
    }

    this.timer = setInterval(() => {
      void this.runOnce();
    }, this.intervalMs);
  }

  onModuleDestroy(): void {
    if (this.timer) {
      clearInterval(this.timer);
    }
  }

  async runOnce(): Promise<void> {
    if (this.running) {
      return;
    }

    this.running = true;

    try {
      await this.runBatch();
    } finally {
      this.running = false;
    }
  }

  private async runBatch(): Promise<void> {
    const repository = this.dataSource.getRepository(OutboxEvent);
    const events = await repository.find({
      where: { status: OutboxEventStatus.PENDING },
      order: { id: 'ASC' },
      take: this.batchSize,
    });

    for (const event of events) {
      await this.publishEvent(event);
    }
  }

  private async publishEvent(event: OutboxEvent): Promise<void> {
    try {
      if (event.eventType !== PAYMENT_COMPLETED_EVENT_TYPE) {
        throw new Error(`Unsupported outbox event type: ${event.eventType}`);
      }

      if (!isPaymentCompletedEvent(event.payload)) {
        throw new Error('Invalid payment.completed outbox payload');
      }

      await this.paymentCompletedPublisher.publish(event.payload);
      this.markPublishedFaultInjector.throwIfEnabled();
      await this.markPublished(event);
    } catch (error) {
      await this.markPendingWithFailure(event, error);
    }
  }

  private async markPublished(event: OutboxEvent): Promise<void> {
    await this.dataSource.getRepository(OutboxEvent).update(event.id, {
      status: OutboxEventStatus.PUBLISHED,
      attempts: event.attempts + 1,
      lastError: null,
      publishedAt: new Date(),
    });
  }

  private async markPendingWithFailure(
    event: OutboxEvent,
    error: unknown,
  ): Promise<void> {
    const message = this.messageFrom(error);

    await this.dataSource.getRepository(OutboxEvent).update(event.id, {
      status: OutboxEventStatus.PENDING,
      attempts: event.attempts + 1,
      lastError: message,
    });

    this.logger.warn(
      `Outbox event publish failed: id=${event.id} eventId=${event.eventId} error=${message}`,
    );
  }

  private messageFrom(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
  }
}
