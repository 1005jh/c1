import { Injectable } from '@nestjs/common';
import { EntityManager } from 'typeorm';
import { Payment } from '../../payments/entities/payment.entity';
import {
  PAYMENT_COMPLETED_EVENT_TYPE,
  createPaymentCompletedEvent,
} from '../events/payment-completed.event';
import { OutboxEvent } from './entities/outbox-event.entity';
import { OutboxEventStatus } from './entities/outbox-event-status.enum';

@Injectable()
export class PaymentCompletedOutboxWriter {
  async enqueue(
    manager: EntityManager,
    payment: Payment,
  ): Promise<OutboxEvent> {
    const event = createPaymentCompletedEvent(payment);
    const repository = manager.getRepository(OutboxEvent);

    return repository.save(
      repository.create({
        eventId: event.eventId,
        eventType: PAYMENT_COMPLETED_EVENT_TYPE,
        eventVersion: event.eventVersion,
        payload: event,
        status: OutboxEventStatus.PENDING,
        attempts: 0,
        lastError: null,
        publishedAt: null,
      }),
    );
  }
}
