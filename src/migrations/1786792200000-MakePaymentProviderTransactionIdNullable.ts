import { MigrationInterface, QueryRunner, TableColumn } from 'typeorm';

export class MakePaymentProviderTransactionIdNullable1786792200000
  implements MigrationInterface
{
  name = 'MakePaymentProviderTransactionIdNullable1786792200000';

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.changeColumn(
      'payments',
      'providerTransactionId',
      new TableColumn({
        name: 'providerTransactionId',
        type: 'varchar',
        length: '100',
        isNullable: true,
      }),
    );
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.changeColumn(
      'payments',
      'providerTransactionId',
      new TableColumn({
        name: 'providerTransactionId',
        type: 'varchar',
        length: '100',
        isNullable: false,
      }),
    );
  }
}
