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

const createMockUpdateQueryBuilder = () => {
  const queryBuilder = {
    update: jest.fn(),
    set: jest.fn(),
    where: jest.fn(),
    andWhere: jest.fn(),
    setParameters: jest.fn(),
    execute: jest.fn(),
  };

  queryBuilder.update.mockReturnValue(queryBuilder);
  queryBuilder.set.mockReturnValue(queryBuilder);
  queryBuilder.where.mockReturnValue(queryBuilder);
  queryBuilder.andWhere.mockReturnValue(queryBuilder);
  queryBuilder.setParameters.mockReturnValue(queryBuilder);

  return queryBuilder;
};

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
  let updateQueryBuilder: ReturnType<typeof createMockUpdateQueryBuilder>;

  beforeEach(async () => {
    productRepository = createMockRepository<Product>();
    inventoryRepository = createMockRepository<Inventory>();
    orderRepository = createMockRepository<Order>();
    orderItemRepository = createMockRepository<OrderItem>();
    updateQueryBuilder = createMockUpdateQueryBuilder();

    const manager = {
      createQueryBuilder: jest.fn(() => updateQueryBuilder),
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
      updateQueryBuilder.execute.mockResolvedValue({ affected: 1 });
      orderRepository.create?.mockImplementation((value) => value);
      orderRepository.save?.mockResolvedValue(order);
      orderItemRepository.create?.mockImplementation((value) => value);
      orderItemRepository.save?.mockResolvedValue(orderItems);

      await expect(
        service.create({ items: [{ productId: 1, quantity: 3 }] }),
      ).resolves.toEqual({ ...order, items: orderItems });

      expect(dataSource.transaction).toHaveBeenCalledTimes(1);
      expect(updateQueryBuilder.update).toHaveBeenCalledWith(Inventory);
      expect(updateQueryBuilder.set).toHaveBeenCalledWith({
        quantity: expect.any(Function),
      });
      expect(updateQueryBuilder.where).toHaveBeenCalledWith(
        'productId = :productId',
        { productId: 1 },
      );
      expect(updateQueryBuilder.andWhere).toHaveBeenCalledWith(
        'quantity >= :quantity',
        { quantity: 3 },
      );
      expect(updateQueryBuilder.setParameters).toHaveBeenCalledWith({
        quantity: 3,
      });
      expect(inventoryRepository.findOne).not.toHaveBeenCalled();
      expect(inventoryRepository.save).not.toHaveBeenCalled();
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
      expect(updateQueryBuilder.execute).not.toHaveBeenCalled();
      expect(orderRepository.save).not.toHaveBeenCalled();
    });

    it('throws ConflictException when inventory does not exist', async () => {
      productRepository.findOne?.mockResolvedValue({ id: 1, price: 10000 });
      updateQueryBuilder.execute.mockResolvedValue({ affected: 0 });
      inventoryRepository.findOne?.mockResolvedValue(null);

      await expect(
        service.create({ items: [{ productId: 1, quantity: 1 }] }),
      ).rejects.toBeInstanceOf(ConflictException);
      expect(inventoryRepository.save).not.toHaveBeenCalled();
      expect(inventoryRepository.findOne).toHaveBeenCalledWith({
        where: { productId: 1 },
      });
      expect(orderRepository.save).not.toHaveBeenCalled();
    });

    it('throws ConflictException when inventory is insufficient', async () => {
      productRepository.findOne?.mockResolvedValue({ id: 1, price: 10000 });
      updateQueryBuilder.execute.mockResolvedValue({ affected: 0 });
      inventoryRepository.findOne?.mockResolvedValue({
        id: 1,
        productId: 1,
        quantity: 1,
      });

      await expect(
        service.create({ items: [{ productId: 1, quantity: 2 }] }),
      ).rejects.toBeInstanceOf(ConflictException);
      expect(inventoryRepository.save).not.toHaveBeenCalled();
      expect(inventoryRepository.findOne).toHaveBeenCalledWith({
        where: { productId: 1 },
      });
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
