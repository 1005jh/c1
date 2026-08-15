import {
  ConflictException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { ProductsService } from '../products/products.service';
import { CreateInventoryDto } from './dto/create-inventory.dto';
import { Inventory } from './entities/inventory.entity';

@Injectable()
export class InventoriesService {
  constructor(
    @InjectRepository(Inventory)
    private readonly inventoriesRepository: Repository<Inventory>,
    private readonly productsService: ProductsService,
  ) {}

  async create(createInventoryDto: CreateInventoryDto): Promise<Inventory> {
    const { productId, quantity } = createInventoryDto;

    await this.productsService.findOne(productId);

    const existingInventory = await this.inventoriesRepository.findOne({
      where: { productId },
    });

    if (existingInventory) {
      throw new ConflictException(
        `Inventory for product id ${productId} already exists`,
      );
    }

    const inventory = this.inventoriesRepository.create({
      productId,
      quantity,
    });

    return this.inventoriesRepository.save(inventory);
  }

  async findByProductId(productId: number): Promise<Inventory> {
    const inventory = await this.inventoriesRepository.findOne({
      where: { productId },
    });

    if (!inventory) {
      throw new NotFoundException(
        `Inventory for product id ${productId} not found`,
      );
    }

    return inventory;
  }
}
