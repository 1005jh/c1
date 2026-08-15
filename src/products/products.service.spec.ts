import { NotFoundException } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Product } from './entities/product.entity';
import { ProductsService } from './products.service';

type MockRepository<T = unknown> = Partial<
  Record<keyof Repository<T>, jest.Mock>
>;

const createMockRepository = (): MockRepository<Product> => ({
  create: jest.fn(),
  save: jest.fn(),
  findOne: jest.fn(),
  findAndCount: jest.fn(),
});

describe('ProductsService', () => {
  let service: ProductsService;
  let repository: MockRepository<Product>;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ProductsService,
        {
          provide: getRepositoryToken(Product),
          useValue: createMockRepository(),
        },
      ],
    }).compile();

    service = module.get<ProductsService>(ProductsService);
    repository = module.get<MockRepository<Product>>(
      getRepositoryToken(Product),
    );
  });

  describe('create', () => {
    it('saves and returns a product', async () => {
      const createProductDto = {
        name: 'MacBook Pro',
        price: 2990000,
        description: 'Test product',
      };
      const product = { id: 1, ...createProductDto } as Product;

      repository.create?.mockReturnValue(product);
      repository.save?.mockResolvedValue(product);

      await expect(service.create(createProductDto)).resolves.toBe(product);
      expect(repository.create).toHaveBeenCalledWith(createProductDto);
      expect(repository.save).toHaveBeenCalledWith(product);
    });
  });

  describe('findOne', () => {
    it('returns a product when it exists', async () => {
      const product = { id: 1, name: 'MacBook Pro', price: 2990000 } as Product;

      repository.findOne?.mockResolvedValue(product);

      await expect(service.findOne(1)).resolves.toBe(product);
      expect(repository.findOne).toHaveBeenCalledWith({ where: { id: 1 } });
    });

    it('throws NotFoundException when product does not exist', async () => {
      repository.findOne?.mockResolvedValue(null);

      await expect(service.findOne(1)).rejects.toBeInstanceOf(
        NotFoundException,
      );
    });
  });

  describe('findAll', () => {
    it('passes pagination options to the repository', async () => {
      const items = [
        { id: 1, name: 'MacBook Pro', price: 2990000 },
      ] as Product[];

      repository.findAndCount?.mockResolvedValue([items, 1]);

      await expect(service.findAll({ page: 2, limit: 10 })).resolves.toEqual({
        items,
        page: 2,
        limit: 10,
        total: 1,
      });
      expect(repository.findAndCount).toHaveBeenCalledWith({
        order: { id: 'DESC' },
        skip: 10,
        take: 10,
      });
    });
  });
});
