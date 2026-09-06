import { MigrationInterface, QueryRunner, Table, TableIndex } from 'typeorm';
import {
  OUTBOX_EVENTS_EVENT_ID_UNIQUE,
  OUTBOX_EVENTS_STATUS_ID_INDEX,
} from '../messaging/outbox/entities/outbox-event.entity';

export class CreateOutboxEventsTable1788624000000 implements MigrationInterface {
  name = 'CreateOutboxEventsTable1788624000000';

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: 'outbox_events',
        columns: [
          {
            name: 'id',
            type: 'int',
            unsigned: true,
            isPrimary: true,
            isGenerated: true,
            generationStrategy: 'increment',
          },
          {
            name: 'eventId',
            type: 'varchar',
            length: '150',
            isNullable: false,
          },
          {
            name: 'eventType',
            type: 'varchar',
            length: '100',
            isNullable: false,
          },
          {
            name: 'eventVersion',
            type: 'int',
            unsigned: true,
            isNullable: false,
          },
          {
            name: 'payload',
            type: 'json',
            isNullable: false,
          },
          {
            name: 'status',
            type: 'varchar',
            length: '20',
            isNullable: false,
          },
          {
            name: 'attempts',
            type: 'int',
            unsigned: true,
            default: 0,
            isNullable: false,
          },
          {
            name: 'lastError',
            type: 'text',
            isNullable: true,
          },
          {
            name: 'publishedAt',
            type: 'datetime',
            precision: 6,
            isNullable: true,
          },
          {
            name: 'createdAt',
            type: 'datetime',
            precision: 6,
            default: 'CURRENT_TIMESTAMP(6)',
            isNullable: false,
          },
          {
            name: 'updatedAt',
            type: 'datetime',
            precision: 6,
            default: 'CURRENT_TIMESTAMP(6)',
            onUpdate: 'CURRENT_TIMESTAMP(6)',
            isNullable: false,
          },
        ],
      }),
    );

    await queryRunner.createIndex(
      'outbox_events',
      new TableIndex({
        name: OUTBOX_EVENTS_EVENT_ID_UNIQUE,
        columnNames: ['eventId'],
        isUnique: true,
      }),
    );

    await queryRunner.createIndex(
      'outbox_events',
      new TableIndex({
        name: OUTBOX_EVENTS_STATUS_ID_INDEX,
        columnNames: ['status', 'id'],
      }),
    );
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropIndex('outbox_events', OUTBOX_EVENTS_STATUS_ID_INDEX);
    await queryRunner.dropIndex('outbox_events', OUTBOX_EVENTS_EVENT_ID_UNIQUE);
    await queryRunner.dropTable('outbox_events');
  }
}
