import {
  ConflictException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { DataSource } from 'typeorm';
import { OrderStatus } from '../orders/entities/order-status.enum';
import { Order } from '../orders/entities/order.entity';
import { FakePaymentClient } from './clients/fake-payment.client';
import { PaymentStatus } from './entities/payment-status.enum';
import { Payment } from './entities/payment.entity';

@Injectable()
export class PaymentsService {
  constructor(
    private readonly dataSource: DataSource,
    private readonly fakePaymentClient: FakePaymentClient,
  ) {}

  async payOrder(orderId: number): Promise<Payment> {
    const orderRepository = this.dataSource.getRepository(Order);
    const paymentRepository = this.dataSource.getRepository(Payment);

    const order = await orderRepository.findOne({ where: { id: orderId } });

    if (!order) {
      throw new NotFoundException(`Order with id ${orderId} not found`);
    }

    const existingPayment = await paymentRepository.findOne({
      where: { orderId, status: PaymentStatus.SUCCESS },
    });

    if (existingPayment) {
      return existingPayment;
    }

    if (order.status !== OrderStatus.PENDING_PAYMENT) {
      throw new ConflictException(`Order with id ${orderId} is not payable`);
    }

    const providerPayment = await this.fakePaymentClient.charge({
      orderId,
      amount: order.totalAmount,
      idempotencyKey: this.createProviderIdempotencyKey(order.id),
    });

    try {
      return await this.dataSource.transaction(async (manager) => {
        const transactionalPaymentRepository = manager.getRepository(Payment);
        const transactionalOrderRepository = manager.getRepository(Order);

        const existingTransactionalPayment =
          await transactionalPaymentRepository.findOne({
            where: { orderId, status: PaymentStatus.SUCCESS },
          });

        if (existingTransactionalPayment) {
          return existingTransactionalPayment;
        }

        const payment = await transactionalPaymentRepository.save(
          transactionalPaymentRepository.create({
            orderId,
            amount: order.totalAmount,
            status: PaymentStatus.SUCCESS,
            providerTransactionId: providerPayment.transactionId,
          }),
        );

        order.status = OrderStatus.PAID;
        await transactionalOrderRepository.save(order);

        return payment;
      });
    } catch (error) {
      if (!this.isRecoverablePaymentInsertRaceError(error)) {
        throw error;
      }

      const recoveredPayment = await paymentRepository.findOne({
        where: { orderId, status: PaymentStatus.SUCCESS },
      });

      if (!recoveredPayment) {
        throw error;
      }

      return recoveredPayment;
    }
  }

  private createProviderIdempotencyKey(orderId: number): string {
    return `payment:order:${orderId}`;
  }

  private isRecoverablePaymentInsertRaceError(error: unknown): boolean {
    const queryError = error as {
      query?: string;
      driverError?: { code?: string; errno?: number };
    };
    const isPaymentInsert =
      typeof queryError.query === 'string' &&
      queryError.query.includes('INSERT INTO `payments`');
    const isDuplicateKey =
      queryError.driverError?.code === 'ER_DUP_ENTRY' ||
      queryError.driverError?.errno === 1062;
    const isDeadlock =
      queryError.driverError?.code === 'ER_LOCK_DEADLOCK' ||
      queryError.driverError?.errno === 1213;

    return isPaymentInsert && (isDuplicateKey || isDeadlock);
  }
}
