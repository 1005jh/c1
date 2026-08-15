import {
  BadRequestException,
  ConflictException,
  NotFoundException,
} from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { DataSource, Repository } from 'typeorm';
import { Inventory } from '../inventories/entities/inventory.entity';
import { Product } from '../products/entities/product.entity';
import { OrderItem } from './entities/order-item.entity';
import { OrderStatus } from './entities/order-status.enum';
import { Order } from './entities/order.entity';
import { OrdersService } from './orders.service';

type MockRepository<T = unknown> = Partial<
  Record<keyof Repository<T>, jest.Mock>
>;

const createMockRepository = <T = unknown>(): MockRepository<T> => ({
  create: jest.fn(),
  save: jest.fn(),
  findOne: jest.fn(),
});

describe('OrdersService', () => {
  let service: OrdersService;
  let dataSource: {
    transaction: jest.Mock;
    getRepository: jest.Mock;
  };
  let productRepository: MockRepository<Product>;
  let inventoryRepository: MockRepository<Inventory>;
  let orderRepository: MockRepository<Order>;
  let orderItemRepository: MockRepository<OrderItem>;

  beforeEach(async () => {
    productRepository = createMockRepository<Product>();
    inventoryRepository = createMockRepository<Inventory>();
    orderRepository = createMockRepository<Order>();
    orderItemRepository = createMockRepository<OrderItem>();

    const manager = {
      getRepository: jest.fn((entity) => {
        if (entity === Product) {
          return productRepository;
        }

        if (entity === Inventory) {
          return inventoryRepository;
        }

        if (entity === Order) {
          return orderRepository;
        }

        if (entity === OrderItem) {
          return orderItemRepository;
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

        throw new Error('Unexpected repository');
      }),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OrdersService,
        {
          provide: DataSource,
          useValue: dataSource,
        },
      ],
    }).compile();

    service = module.get<OrdersService>(OrdersService);
  });

  describe('create', () => {
    it('decreases inventory, creates order and order items, and calculates total amount', async () => {
      const product = { id: 1, price: 10000 } as Product;
      const inventory = { id: 1, productId: 1, quantity: 100 } as Inventory;
      const order = {
        id: 1,
        status: OrderStatus.PENDING_PAYMENT,
        totalAmount: 30000,
      } as Order;
      const orderItems = [
        {
          id: 1,
          orderId: 1,
          productId: 1,
          quantity: 3,
          unitPrice: 10000,
          subtotal: 30000,
        },
      ] as OrderItem[];

      productRepository.findOne?.mockResolvedValue(product);
      inventoryRepository.findOne?.mockResolvedValue(inventory);
      inventoryRepository.save?.mockResolvedValue(inventory);
      orderRepository.create?.mockImplementation((value) => value);
      orderRepository.save?.mockResolvedValue(order);
      orderItemRepository.create?.mockImplementation((value) => value);
      orderItemRepository.save?.mockResolvedValue(orderItems);

      await expect(
        service.create({ items: [{ productId: 1, quantity: 3 }] }),
      ).resolves.toEqual({ ...order, items: orderItems });

      expect(dataSource.transaction).toHaveBeenCalledTimes(1);
      expect(inventory.quantity).toBe(97);
      expect(inventoryRepository.save).toHaveBeenCalledWith(inventory);
      expect(orderRepository.create).toHaveBeenCalledWith({
        status: OrderStatus.PENDING_PAYMENT,
        totalAmount: 30000,
      });
      expect(orderItemRepository.create).toHaveBeenCalledWith({
        orderId: 1,
        productId: 1,
        quantity: 3,
        unitPrice: 10000,
        subtotal: 30000,
      });
    });

    it('throws NotFoundException when product does not exist', async () => {
      productRepository.findOne?.mockResolvedValue(null);

      await expect(
        service.create({ items: [{ productId: 999, quantity: 1 }] }),
      ).rejects.toBeInstanceOf(NotFoundException);
      expect(inventoryRepository.findOne).not.toHaveBeenCalled();
      expect(orderRepository.save).not.toHaveBeenCalled();
    });

    it('throws ConflictException when inventory does not exist', async () => {
      productRepository.findOne?.mockResolvedValue({ id: 1, price: 10000 });
      inventoryRepository.findOne?.mockResolvedValue(null);

      await expect(
        service.create({ items: [{ productId: 1, quantity: 1 }] }),
      ).rejects.toBeInstanceOf(ConflictException);
      expect(inventoryRepository.save).not.toHaveBeenCalled();
      expect(orderRepository.save).not.toHaveBeenCalled();
    });

    it('throws ConflictException when inventory is insufficient', async () => {
      productRepository.findOne?.mockResolvedValue({ id: 1, price: 10000 });
      inventoryRepository.findOne?.mockResolvedValue({
        id: 1,
        productId: 1,
        quantity: 1,
      });

      await expect(
        service.create({ items: [{ productId: 1, quantity: 2 }] }),
      ).rejects.toBeInstanceOf(ConflictException);
      expect(inventoryRepository.save).not.toHaveBeenCalled();
      expect(orderRepository.save).not.toHaveBeenCalled();
    });

    it('throws BadRequestException when request contains duplicate product ids', async () => {
      await expect(
        service.create({
          items: [
            { productId: 1, quantity: 1 },
            { productId: 1, quantity: 2 },
          ],
        }),
      ).rejects.toBeInstanceOf(BadRequestException);
      expect(dataSource.transaction).not.toHaveBeenCalled();
    });
  });

  describe('findOne', () => {
    it('returns an order with items when it exists', async () => {
      const order = {
        id: 1,
        status: OrderStatus.PENDING_PAYMENT,
        totalAmount: 30000,
        items: [{ id: 1, orderId: 1, productId: 1 }],
      } as Order;

      orderRepository.findOne?.mockResolvedValue(order);

      await expect(service.findOne(1)).resolves.toBe(order);
      expect(orderRepository.findOne).toHaveBeenCalledWith({
        where: { id: 1 },
        relations: { items: true },
      });
    });

    it('throws NotFoundException when order does not exist', async () => {
      orderRepository.findOne?.mockResolvedValue(null);

      await expect(service.findOne(1)).rejects.toBeInstanceOf(
        NotFoundException,
      );
    });
  });
});
