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
import { PaymentProviderUnknownOutcomeException } from './exceptions/payment-provider-unknown-outcome.exception';

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
      where: { orderId },
    });

    if (existingPayment) {
      if (existingPayment.status === PaymentStatus.SUCCESS) {
        return existingPayment;
      }

      throw new ConflictException(
        `Payment outcome for order id ${orderId} is unknown; reconciliation required`,
      );
    }

    if (order.status !== OrderStatus.PENDING_PAYMENT) {
      throw new ConflictException(`Order with id ${orderId} is not payable`);
    }

    const idempotencyKey = this.createProviderIdempotencyKey(order.id);
    let providerPayment: { transactionId: string; status: PaymentStatus };

    try {
      providerPayment = await this.fakePaymentClient.charge({
        orderId,
        amount: order.totalAmount,
        idempotencyKey,
      });
    } catch (error) {
      if (!(error instanceof PaymentProviderUnknownOutcomeException)) {
        throw error;
      }

      await this.saveUnknownPayment(order);
      throw error;
    }

    try {
      return await this.dataSource.transaction(async (manager) => {
        const transactionalPaymentRepository = manager.getRepository(Payment);
        const transactionalOrderRepository = manager.getRepository(Order);

        const existingTransactionalPayment =
          await transactionalPaymentRepository.findOne({
            where: { orderId },
          });

        if (existingTransactionalPayment) {
          if (existingTransactionalPayment.status === PaymentStatus.SUCCESS) {
            return existingTransactionalPayment;
          }

          throw new ConflictException(
            `Payment outcome for order id ${orderId} is unknown; reconciliation required`,
          );
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
        where: { orderId },
      });

      if (recoveredPayment?.status === PaymentStatus.SUCCESS) {
        return recoveredPayment;
      }

      throw error;
    }
  }

  async reconcileOrder(orderId: number): Promise<Payment> {
    const orderRepository = this.dataSource.getRepository(Order);
    const paymentRepository = this.dataSource.getRepository(Payment);

    const order = await orderRepository.findOne({ where: { id: orderId } });

    if (!order) {
      throw new NotFoundException(`Order with id ${orderId} not found`);
    }

    const payment = await paymentRepository.findOne({ where: { orderId } });

    if (!payment) {
      throw new NotFoundException(`Payment for order id ${orderId} not found`);
    }

    if (payment.status === PaymentStatus.SUCCESS) {
      return payment;
    }

    const providerPayment =
      await this.fakePaymentClient.getChargeByIdempotencyKey(
        this.createProviderIdempotencyKey(orderId),
      );

    if (!providerPayment.found) {
      throw new ConflictException(
        `Payment outcome for order id ${orderId} is not available from provider`,
      );
    }

    if (
      providerPayment.status !== PaymentStatus.SUCCESS ||
      providerPayment.orderId !== orderId ||
      providerPayment.amount !== payment.amount
    ) {
      throw new ConflictException(
        `Payment provider result for order id ${orderId} does not match local payment`,
      );
    }

    return this.dataSource.transaction(async (manager) => {
      const transactionalPaymentRepository = manager.getRepository(Payment);
      const transactionalOrderRepository = manager.getRepository(Order);

      const transactionalPayment = await transactionalPaymentRepository.findOne(
        {
          where: { orderId },
        },
      );

      if (!transactionalPayment) {
        throw new NotFoundException(
          `Payment for order id ${orderId} not found`,
        );
      }

      if (transactionalPayment.status === PaymentStatus.SUCCESS) {
        return transactionalPayment;
      }

      transactionalPayment.status = PaymentStatus.SUCCESS;
      transactionalPayment.providerTransactionId =
        providerPayment.transactionId;

      const savedPayment =
        await transactionalPaymentRepository.save(transactionalPayment);

      order.status = OrderStatus.PAID;
      await transactionalOrderRepository.save(order);

      return savedPayment;
    });
  }

  private async saveUnknownPayment(order: Order): Promise<Payment> {
    try {
      return await this.dataSource.transaction(async (manager) => {
        const transactionalPaymentRepository = manager.getRepository(Payment);

        const existingPayment = await transactionalPaymentRepository.findOne({
          where: { orderId: order.id },
        });

        if (existingPayment) {
          return existingPayment;
        }

        return transactionalPaymentRepository.save(
          transactionalPaymentRepository.create({
            orderId: order.id,
            amount: order.totalAmount,
            status: PaymentStatus.UNKNOWN,
            providerTransactionId: null,
          }),
        );
      });
    } catch (error) {
      if (!this.isRecoverablePaymentInsertRaceError(error)) {
        throw error;
      }

      const recoveredPayment = await this.dataSource
        .getRepository(Payment)
        .findOne({
          where: { orderId: order.id },
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
