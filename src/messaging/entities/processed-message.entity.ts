import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
  Unique,
} from 'typeorm';

export const PROCESSED_MESSAGES_CONSUMER_EVENT_UNIQUE =
  'UQ_processed_messages_consumerName_eventId';

@Entity({ name: 'processed_messages' })
@Unique(PROCESSED_MESSAGES_CONSUMER_EVENT_UNIQUE, ['consumerName', 'eventId'])
export class ProcessedMessage {
  @PrimaryGeneratedColumn({ type: 'int', unsigned: true })
  id!: number;

  @Column({ type: 'varchar', length: 100 })
  consumerName!: string;

  @Column({ type: 'varchar', length: 150 })
  eventId!: string;

  @Column({ type: 'varchar', length: 100 })
  eventType!: string;

  @CreateDateColumn({ type: 'datetime', precision: 6 })
  processedAt!: Date;
}
