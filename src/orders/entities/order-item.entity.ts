import {
  Column,
  CreateDateColumn,
  Entity,
  JoinColumn,
  ManyToOne,
  PrimaryGeneratedColumn,
} from 'typeorm';
import { Product } from '../../products/entities/product.entity';
import { Order } from './order.entity';

@Entity({ name: 'order_items' })
export class OrderItem {
  @PrimaryGeneratedColumn({ type: 'int', unsigned: true })
  id!: number;

  @Column({ type: 'int', unsigned: true })
  orderId!: number;

  @Column({ type: 'int', unsigned: true })
  productId!: number;

  @Column({ type: 'int', unsigned: true })
  quantity!: number;

  @Column({ type: 'int', unsigned: true })
  unitPrice!: number;

  @Column({ type: 'int', unsigned: true })
  subtotal!: number;

  @ManyToOne(() => Order, (order) => order.items, {
    nullable: false,
    onDelete: 'RESTRICT',
  })
  @JoinColumn({ name: 'orderId' })
  order!: Order;

  @ManyToOne(() => Product, { nullable: false, onDelete: 'RESTRICT' })
  @JoinColumn({ name: 'productId' })
  product!: Product;

  @CreateDateColumn({ type: 'datetime', precision: 6 })
  createdAt!: Date;
}
