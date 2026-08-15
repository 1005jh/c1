import {
  BadGatewayException,
  ConflictException,
  NotFoundException,
} from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { DataSource, Repository } from 'typeorm';
import { OrderStatus } from '../orders/entities/order-status.enum';
import { Order } from '../orders/entities/order.entity';
import { FakePaymentClient } from './clients/fake-payment.client';
import { PaymentStatus } from './entities/payment-status.enum';
import { Payment } from './entities/payment.entity';
import { PaymentsService } from './payments.service';

type MockRepository<T = unknown> = Partial<
  Record<keyof Repository<T>, jest.Mock>
>;

const createMockRepository = <T = unknown>(): MockRepository<T> => ({
  create: jest.fn(),
  save: jest.fn(),
  findOne: jest.fn(),
});

describe('PaymentsService', () => {
  let service: PaymentsService;
  let dataSource: {
    transaction: jest.Mock;
    getRepository: jest.Mock;
  };
  let orderRepository: MockRepository<Order>;
  let paymentRepository: MockRepository<Payment>;
  let transactionalOrderRepository: MockRepository<Order>;
  let transactionalPaymentRepository: MockRepository<Payment>;
  let fakePaymentClient: jest.Mocked<Pick<FakePaymentClient, 'charge'>>;

  beforeEach(async () => {
    orderRepository = createMockRepository<Order>();
    paymentRepository = createMockRepository<Payment>();
    transactionalOrderRepository = createMockRepository<Order>();
    transactionalPaymentRepository = createMockRepository<Payment>();
    fakePaymentClient = {
      charge: jest.fn(),
    };

    const manager = {
      getRepository: jest.fn((entity) => {
        if (entity === Order) {
          return transactionalOrderRepository;
        }

        if (entity === Payment) {
          return transactionalPaymentRepository;
        }

        throw new Error('Unexpected repository');
      }),
    };

    dataSource = {
      transaction: jest.fn((callback) => callback(manager)),
      getRepository: jest.fn((entity) => {
        if (entity === Order) {
          return orderRepository;
        }

        if (entity === Payment) {
          return paymentRepository;
        }

        throw new Error('Unexpected repository');
      }),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        PaymentsService,
        {
          provide: DataSource,
          useValue: dataSource,
        },
        {
          provide: FakePaymentClient,
          useValue: fakePaymentClient,
        },
      ],
    }).compile();

    service = module.get<PaymentsService>(PaymentsService);
  });

  describe('payOrder', () => {
    it('creates a successful payment and marks order as paid', async () => {
      const order = {
        id: 1,
        status: OrderStatus.PENDING_PAYMENT,
        totalAmount: 30000,
      } as Order;
      const payment = {
        id: 1,
        orderId: 1,
        amount: 30000,
        status: PaymentStatus.SUCCESS,
        providerTransactionId: 'tx_1',
      } as Payment;

      orderRepository.findOne?.mockResolvedValue(order);
      paymentRepository.findOne?.mockResolvedValue(null);
      fakePaymentClient.charge.mockResolvedValue({
        transactionId: 'tx_1',
        status: PaymentStatus.SUCCESS,
      });
      transactionalPaymentRepository.findOne?.mockResolvedValue(null);
      transactionalPaymentRepository.create?.mockImplementation(
        (value) => value,
      );
      transactionalPaymentRepository.save?.mockResolvedValue(payment);
      transactionalOrderRepository.save?.mockResolvedValue({
        ...order,
        status: OrderStatus.PAID,
      });

      await expect(service.payOrder(1)).resolves.toBe(payment);

      expect(fakePaymentClient.charge).toHaveBeenCalledWith({
        orderId: 1,
        amount: 30000,
        idempotencyKey: 'payment:order:1',
      });
      expect(fakePaymentClient.charge.mock.invocationCallOrder[0]).toBeLessThan(
        dataSource.transaction.mock.invocationCallOrder[0],
      );
      expect(transactionalPaymentRepository.create).toHaveBeenCalledWith({
        orderId: 1,
        amount: 30000,
        status: PaymentStatus.SUCCESS,
        providerTransactionId: 'tx_1',
      });
      expect(transactionalPaymentRepository.save).toHaveBeenCalled();
      expect(order.status).toBe(OrderStatus.PAID);
      expect(transactionalOrderRepository.save).toHaveBeenCalledWith(order);
    });

    it('throws NotFoundException when order does not exist', async () => {
      orderRepository.findOne?.mockResolvedValue(null);

      await expect(service.payOrder(1)).rejects.toBeInstanceOf(
        NotFoundException,
      );
      expect(fakePaymentClient.charge).not.toHaveBeenCalled();
      expect(dataSource.transaction).not.toHaveBeenCalled();
    });

    it('throws ConflictException when order is already paid', async () => {
      orderRepository.findOne?.mockResolvedValue({
        id: 1,
        status: OrderStatus.PAID,
        totalAmount: 30000,
      });
      paymentRepository.findOne?.mockResolvedValue(null);

      await expect(service.payOrder(1)).rejects.toBeInstanceOf(
        ConflictException,
      );
      expect(fakePaymentClient.charge).not.toHaveBeenCalled();
      expect(dataSource.transaction).not.toHaveBeenCalled();
    });

    it('returns existing successful payment without charging provider', async () => {
      const existingPayment = {
        id: 1,
        orderId: 1,
        amount: 30000,
        status: PaymentStatus.SUCCESS,
        providerTransactionId: 'tx_1',
      } as Payment;

      orderRepository.findOne?.mockResolvedValue({
        id: 1,
        status: OrderStatus.PENDING_PAYMENT,
        totalAmount: 30000,
      });
      paymentRepository.findOne?.mockResolvedValue(existingPayment);

      await expect(service.payOrder(1)).resolves.toBe(existingPayment);
      expect(fakePaymentClient.charge).not.toHaveBeenCalled();
      expect(dataSource.transaction).not.toHaveBeenCalled();
    });

    it('returns existing payment when order is already paid', async () => {
      const existingPayment = {
        id: 1,
        orderId: 1,
        amount: 30000,
        status: PaymentStatus.SUCCESS,
        providerTransactionId: 'tx_1',
      } as Payment;

      orderRepository.findOne?.mockResolvedValue({
        id: 1,
        status: OrderStatus.PAID,
        totalAmount: 30000,
      });
      paymentRepository.findOne?.mockResolvedValue(existingPayment);

      await expect(service.payOrder(1)).resolves.toBe(existingPayment);
      expect(fakePaymentClient.charge).not.toHaveBeenCalled();
      expect(dataSource.transaction).not.toHaveBeenCalled();
    });

    it('recovers existing payment after duplicate insert race', async () => {
      const order = {
        id: 1,
        status: OrderStatus.PENDING_PAYMENT,
        totalAmount: 30000,
      } as Order;
      const existingPayment = {
        id: 1,
        orderId: 1,
        amount: 30000,
        status: PaymentStatus.SUCCESS,
        providerTransactionId: 'tx_1',
      } as Payment;
      const duplicateError = {
        query:
          'INSERT INTO `payments`(`id`, `orderId`, `amount`, `status`, `providerTransactionId`, `createdAt`, `updatedAt`) VALUES (DEFAULT, ?, ?, ?, ?, DEFAULT, DEFAULT)',
        driverError: {
          code: 'ER_DUP_ENTRY',
        },
      };

      orderRepository.findOne?.mockResolvedValue(order);
      paymentRepository.findOne
        ?.mockResolvedValueOnce(null)
        .mockResolvedValueOnce(existingPayment);
      fakePaymentClient.charge.mockResolvedValue({
        transactionId: 'tx_1',
        status: PaymentStatus.SUCCESS,
      });
      transactionalPaymentRepository.findOne?.mockResolvedValue(null);
      transactionalPaymentRepository.create?.mockImplementation(
        (value) => value,
      );
      transactionalPaymentRepository.save?.mockRejectedValue(duplicateError);

      await expect(service.payOrder(1)).resolves.toBe(existingPayment);
      expect(fakePaymentClient.charge).toHaveBeenCalledWith({
        orderId: 1,
        amount: 30000,
        idempotencyKey: 'payment:order:1',
      });
      expect(paymentRepository.findOne).toHaveBeenLastCalledWith({
        where: { orderId: 1, status: PaymentStatus.SUCCESS },
      });
      expect(transactionalOrderRepository.save).not.toHaveBeenCalled();
    });

    it('recovers existing payment after payment insert deadlock race', async () => {
      const order = {
        id: 1,
        status: OrderStatus.PENDING_PAYMENT,
        totalAmount: 30000,
      } as Order;
      const existingPayment = {
        id: 1,
        orderId: 1,
        amount: 30000,
        status: PaymentStatus.SUCCESS,
        providerTransactionId: 'tx_1',
      } as Payment;
      const deadlockError = {
        query:
          'INSERT INTO `payments`(`id`, `orderId`, `amount`, `status`, `providerTransactionId`, `createdAt`, `updatedAt`) VALUES (DEFAULT, ?, ?, ?, ?, DEFAULT, DEFAULT)',
        driverError: {
          code: 'ER_LOCK_DEADLOCK',
        },
      };

      orderRepository.findOne?.mockResolvedValue(order);
      paymentRepository.findOne
        ?.mockResolvedValueOnce(null)
        .mockResolvedValueOnce(existingPayment);
      fakePaymentClient.charge.mockResolvedValue({
        transactionId: 'tx_1',
        status: PaymentStatus.SUCCESS,
      });
      transactionalPaymentRepository.findOne?.mockResolvedValue(null);
      transactionalPaymentRepository.create?.mockImplementation(
        (value) => value,
      );
      transactionalPaymentRepository.save?.mockRejectedValue(deadlockError);

      await expect(service.payOrder(1)).resolves.toBe(existingPayment);
      expect(transactionalOrderRepository.save).not.toHaveBeenCalled();
    });

    it('does not hide general database errors', async () => {
      const order = {
        id: 1,
        status: OrderStatus.PENDING_PAYMENT,
        totalAmount: 30000,
      } as Order;
      const databaseError = new Error('database failed');

      orderRepository.findOne?.mockResolvedValue(order);
      paymentRepository.findOne?.mockResolvedValue(null);
      fakePaymentClient.charge.mockResolvedValue({
        transactionId: 'tx_1',
        status: PaymentStatus.SUCCESS,
      });
      transactionalPaymentRepository.findOne?.mockResolvedValue(null);
      transactionalPaymentRepository.create?.mockImplementation(
        (value) => value,
      );
      transactionalPaymentRepository.save?.mockRejectedValue(databaseError);

      await expect(service.payOrder(1)).rejects.toBe(databaseError);
      expect(fakePaymentClient.charge).toHaveBeenCalledWith({
        orderId: 1,
        amount: 30000,
        idempotencyKey: 'payment:order:1',
      });
      expect(paymentRepository.findOne).toHaveBeenCalledTimes(1);
    });

    it('does not create payment or mark order paid when provider fails', async () => {
      const order = {
        id: 1,
        status: OrderStatus.PENDING_PAYMENT,
        totalAmount: 30000,
      } as Order;

      orderRepository.findOne?.mockResolvedValue(order);
      paymentRepository.findOne?.mockResolvedValue(null);
      fakePaymentClient.charge.mockRejectedValue(new BadGatewayException());

      await expect(service.payOrder(1)).rejects.toBeInstanceOf(
        BadGatewayException,
      );
      expect(dataSource.transaction).not.toHaveBeenCalled();
      expect(transactionalPaymentRepository.save).not.toHaveBeenCalled();
      expect(transactionalOrderRepository.save).not.toHaveBeenCalled();
      expect(order.status).toBe(OrderStatus.PENDING_PAYMENT);
    });

    it('uses the same transaction manager for payment and order changes', async () => {
      const order = {
        id: 1,
        status: OrderStatus.PENDING_PAYMENT,
        totalAmount: 30000,
      } as Order;

      orderRepository.findOne?.mockResolvedValue(order);
      paymentRepository.findOne?.mockResolvedValue(null);
      fakePaymentClient.charge.mockResolvedValue({
        transactionId: 'tx_1',
        status: PaymentStatus.SUCCESS,
      });
      transactionalPaymentRepository.findOne?.mockResolvedValue(null);
      transactionalPaymentRepository.create?.mockImplementation(
        (value) => value,
      );
      transactionalPaymentRepository.save?.mockResolvedValue({
        id: 1,
        orderId: 1,
      });

      await service.payOrder(1);

      expect(dataSource.transaction).toHaveBeenCalledTimes(1);
      expect(transactionalPaymentRepository.save).toHaveBeenCalledTimes(1);
      expect(transactionalOrderRepository.save).toHaveBeenCalledTimes(1);
    });
  });
});
