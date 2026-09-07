import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { FindManyOptions, LessThan, Repository } from 'typeorm';
import { CreateProductDto } from './dto/create-product.dto';
import { GetProductsCursorQueryDto } from './dto/get-products-cursor-query.dto';
import { GetProductsQueryDto } from './dto/get-products-query.dto';
import { Product } from './entities/product.entity';

@Injectable()
export class ProductsService {
  constructor(
    @InjectRepository(Product)
    private readonly productsRepository: Repository<Product>,
  ) {}

  async create(createProductDto: CreateProductDto): Promise<Product> {
    const product = this.productsRepository.create({
      ...createProductDto,
      description: createProductDto.description ?? null,
    });

    return this.productsRepository.save(product);
  }

  async findOne(id: number): Promise<Product> {
    const product = await this.productsRepository.findOne({ where: { id } });

    if (!product) {
      throw new NotFoundException(`Product with id ${id} not found`);
    }

    return product;
  }

  async findAll(query: GetProductsQueryDto) {
    const { page, limit } = query;
    const [items, total] = await this.productsRepository.findAndCount({
      order: { id: 'DESC' },
      skip: (page - 1) * limit,
      take: limit,
    });

    return {
      items,
      page,
      limit,
      total,
    };
  }

  async findAllByCursor(query: GetProductsCursorQueryDto) {
    const { cursorId, limit } = query;
    const options: FindManyOptions<Product> = {
      order: { id: 'DESC' },
      take: limit + 1,
    };

    if (cursorId) {
      options.where = { id: LessThan(cursorId) };
    }

    const rows = await this.productsRepository.find(options);
    const hasNext = rows.length > limit;
    const items = rows.slice(0, limit);
    const nextCursor =
      hasNext && items.length > 0 ? items[items.length - 1].id : null;

    return {
      items,
      limit,
      nextCursor,
      hasNext,
    };
  }
}
