import {
  Column,
  CreateDateColumn,
  Entity,
  JoinColumn,
  OneToOne,
  PrimaryGeneratedColumn,
  Unique,
  UpdateDateColumn,
} from 'typeorm';
import { Order } from '../../orders/entities/order.entity';
import { PaymentStatus } from './payment-status.enum';

@Entity({ name: 'payments' })
@Unique('UQ_payments_orderId', ['orderId'])
export class Payment {
  @PrimaryGeneratedColumn({ type: 'int', unsigned: true })
  id!: number;

  @Column({ type: 'int', unsigned: true })
  orderId!: number;

  @Column({ type: 'int', unsigned: true })
  amount!: number;

  @Column({ type: 'varchar', length: 30 })
  status!: PaymentStatus;

  @Column({ type: 'varchar', length: 100 })
  providerTransactionId!: string;

  @OneToOne(() => Order, { nullable: false, onDelete: 'RESTRICT' })
  @JoinColumn({ name: 'orderId' })
  order!: Order;

  @CreateDateColumn({ type: 'datetime', precision: 6 })
  createdAt!: Date;

  @UpdateDateColumn({ type: 'datetime', precision: 6 })
  updatedAt!: Date;
}
