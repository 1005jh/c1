import {
  MigrationInterface,
  QueryRunner,
  Table,
  TableForeignKey,
  TableIndex,
} from 'typeorm';

export class CreateInventoriesTable1786779000000 implements MigrationInterface {
  name = 'CreateInventoriesTable1786779000000';

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: 'inventories',
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
            name: 'productId',
            type: 'int',
            unsigned: true,
            isNullable: false,
          },
          {
            name: 'quantity',
            type: 'int',
            unsigned: true,
            isNullable: false,
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
      'inventories',
      new TableIndex({
        name: 'IDX_inventories_productId_unique',
        columnNames: ['productId'],
        isUnique: true,
      }),
    );

    await queryRunner.createForeignKey(
      'inventories',
      new TableForeignKey({
        name: 'FK_inventories_productId_products_id',
        columnNames: ['productId'],
        referencedTableName: 'products',
        referencedColumnNames: ['id'],
        onDelete: 'RESTRICT',
        onUpdate: 'RESTRICT',
      }),
    );
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropForeignKey(
      'inventories',
      'FK_inventories_productId_products_id',
    );
    await queryRunner.dropIndex(
      'inventories',
      'IDX_inventories_productId_unique',
    );
    await queryRunner.dropTable('inventories');
  }
}
