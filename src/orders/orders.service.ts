import {
  BadRequestException,
  ConflictException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { DataSource } from 'typeorm';
import { Inventory } from '../inventories/entities/inventory.entity';
import { Product } from '../products/entities/product.entity';
import { CreateOrderDto, CreateOrderItemDto } from './dto/create-order.dto';
import { OrderItem } from './entities/order-item.entity';
import { OrderStatus } from './entities/order-status.enum';
import { Order } from './entities/order.entity';

type PreparedOrderItem = CreateOrderItemDto & {
  unitPrice: number;
  subtotal: number;
};

@Injectable()
export class OrdersService {
  constructor(private readonly dataSource: DataSource) {}

  async create(createOrderDto: CreateOrderDto): Promise<Order> {
    this.validateDuplicateProductIds(createOrderDto.items);

    return this.dataSource.transaction(async (manager) => {
      const productRepository = manager.getRepository(Product);
      const inventoryRepository = manager.getRepository(Inventory);
      const orderRepository = manager.getRepository(Order);
      const orderItemRepository = manager.getRepository(OrderItem);

      const preparedItems: PreparedOrderItem[] = [];

      for (const item of createOrderDto.items) {
        const product = await productRepository.findOne({
          where: { id: item.productId },
        });

        if (!product) {
          throw new NotFoundException(
            `Product with id ${item.productId} not found`,
          );
        }

        const inventory = await inventoryRepository.findOne({
          where: { productId: item.productId },
        });

        if (!inventory) {
          throw new ConflictException(
            `Inventory for product id ${item.productId} does not exist`,
          );
        }

        if (inventory.quantity < item.quantity) {
          throw new ConflictException(
            `Insufficient inventory for product id ${item.productId}`,
          );
        }

        inventory.quantity -= item.quantity;
        await inventoryRepository.save(inventory);

        const unitPrice = product.price;
        const subtotal = unitPrice * item.quantity;

        preparedItems.push({
          productId: item.productId,
          quantity: item.quantity,
          unitPrice,
          subtotal,
        });
      }

      const totalAmount = preparedItems.reduce(
        (sum, item) => sum + item.subtotal,
        0,
      );

      const order = await orderRepository.save(
        orderRepository.create({
          status: OrderStatus.PENDING_PAYMENT,
          totalAmount,
        }),
      );

      const orderItems = await orderItemRepository.save(
        preparedItems.map((item) =>
          orderItemRepository.create({
            orderId: order.id,
            productId: item.productId,
            quantity: item.quantity,
            unitPrice: item.unitPrice,
            subtotal: item.subtotal,
          }),
        ),
      );

      order.items = orderItems;

      return order;
    });
  }

  async findOne(id: number): Promise<Order> {
    const orderRepository = this.dataSource.getRepository(Order);
    const order = await orderRepository.findOne({
      where: { id },
      relations: { items: true },
    });

    if (!order) {
      throw new NotFoundException(`Order with id ${id} not found`);
    }

    return order;
  }

  private validateDuplicateProductIds(items: CreateOrderItemDto[]): void {
    const productIds = new Set<number>();

    for (const item of items) {
      if (productIds.has(item.productId)) {
        throw new BadRequestException(
          `Duplicate product id ${item.productId} is not allowed`,
        );
      }

      productIds.add(item.productId);
    }
  }
}
