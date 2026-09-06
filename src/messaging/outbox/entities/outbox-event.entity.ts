import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
  Unique,
  UpdateDateColumn,
} from 'typeorm';
import type { PaymentCompletedEvent } from '../../events/payment-completed.event';
import { OutboxEventStatus } from './outbox-event-status.enum';

export const OUTBOX_EVENTS_EVENT_ID_UNIQUE = 'UQ_outbox_events_eventId';
export const OUTBOX_EVENTS_STATUS_ID_INDEX = 'IDX_outbox_events_status_id';

@Entity({ name: 'outbox_events' })
@Unique(OUTBOX_EVENTS_EVENT_ID_UNIQUE, ['eventId'])
export class OutboxEvent {
  @PrimaryGeneratedColumn({ type: 'int', unsigned: true })
  id!: number;

  @Column({ type: 'varchar', length: 150 })
  eventId!: string;

  @Column({ type: 'varchar', length: 100 })
  eventType!: string;

  @Column({ type: 'int', unsigned: true })
  eventVersion!: number;

  @Column({ type: 'json' })
  payload!: PaymentCompletedEvent;

  @Column({ type: 'varchar', length: 20 })
  status!: OutboxEventStatus;

  @Column({ type: 'int', unsigned: true, default: 0 })
  attempts!: number;

  @Column({ type: 'text', nullable: true })
  lastError!: string | null;

  @Column({ type: 'datetime', precision: 6, nullable: true })
  publishedAt!: Date | null;

  @CreateDateColumn({ type: 'datetime', precision: 6 })
  createdAt!: Date;

  @UpdateDateColumn({ type: 'datetime', precision: 6 })
  updatedAt!: Date;
}
