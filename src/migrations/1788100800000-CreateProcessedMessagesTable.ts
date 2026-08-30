import { MigrationInterface, QueryRunner, Table, TableIndex } from 'typeorm';
import { PROCESSED_MESSAGES_CONSUMER_EVENT_UNIQUE } from '../messaging/entities/processed-message.entity';

export class CreateProcessedMessagesTable1788100800000 implements MigrationInterface {
  name = 'CreateProcessedMessagesTable1788100800000';

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: 'processed_messages',
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
            name: 'consumerName',
            type: 'varchar',
            length: '100',
            isNullable: false,
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
            name: 'processedAt',
            type: 'datetime',
            precision: 6,
            default: 'CURRENT_TIMESTAMP(6)',
            isNullable: false,
          },
        ],
      }),
    );

    await queryRunner.createIndex(
      'processed_messages',
      new TableIndex({
        name: PROCESSED_MESSAGES_CONSUMER_EVENT_UNIQUE,
        columnNames: ['consumerName', 'eventId'],
        isUnique: true,
      }),
    );
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropIndex(
      'processed_messages',
      PROCESSED_MESSAGES_CONSUMER_EVENT_UNIQUE,
    );
    await queryRunner.dropTable('processed_messages');
  }
}
