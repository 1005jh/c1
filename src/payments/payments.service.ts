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

    if (order.status !== OrderStatus.PENDING_PAYMENT) {
      throw new ConflictException(`Order with id ${orderId} is not payable`);
    }

    const existingPayment = await paymentRepository.findOne({
      where: { orderId },
    });

    if (existingPayment) {
      throw new ConflictException(
        `Payment for order id ${orderId} already exists`,
      );
    }

    const providerPayment = await this.fakePaymentClient.charge({
      orderId,
      amount: order.totalAmount,
    });

    return this.dataSource.transaction(async (manager) => {
      const transactionalPaymentRepository = manager.getRepository(Payment);
      const transactionalOrderRepository = manager.getRepository(Order);

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
  }
}
