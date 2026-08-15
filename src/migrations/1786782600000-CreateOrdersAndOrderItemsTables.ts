import {
  MigrationInterface,
  QueryRunner,
  Table,
  TableForeignKey,
} from 'typeorm';

export class CreateOrdersAndOrderItemsTables1786782600000 implements MigrationInterface {
  name = 'CreateOrdersAndOrderItemsTables1786782600000';

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: 'orders',
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
            name: 'status',
            type: 'varchar',
            length: '30',
            default: "'PENDING_PAYMENT'",
            isNullable: false,
          },
          {
            name: 'totalAmount',
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

    await queryRunner.createTable(
      new Table({
        name: 'order_items',
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
            name: 'orderId',
            type: 'int',
            unsigned: true,
            isNullable: false,
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
            name: 'unitPrice',
            type: 'int',
            unsigned: true,
            isNullable: false,
          },
          {
            name: 'subtotal',
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
        ],
      }),
    );

    await queryRunner.createForeignKey(
      'order_items',
      new TableForeignKey({
        name: 'FK_order_items_orderId_orders_id',
        columnNames: ['orderId'],
        referencedTableName: 'orders',
        referencedColumnNames: ['id'],
        onDelete: 'RESTRICT',
        onUpdate: 'RESTRICT',
      }),
    );

    await queryRunner.createForeignKey(
      'order_items',
      new TableForeignKey({
        name: 'FK_order_items_productId_products_id',
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
      'order_items',
      'FK_order_items_productId_products_id',
    );
    await queryRunner.dropForeignKey(
      'order_items',
      'FK_order_items_orderId_orders_id',
    );
    await queryRunner.dropTable('order_items');
    await queryRunner.dropTable('orders');
  }
}
