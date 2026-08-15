import { ConflictException, NotFoundException } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { ProductsService } from '../products/products.service';
import { Inventory } from './entities/inventory.entity';
import { InventoriesService } from './inventories.service';

type MockRepository<T = unknown> = Partial<
  Record<keyof Repository<T>, jest.Mock>
>;

const createMockRepository = (): MockRepository<Inventory> => ({
  create: jest.fn(),
  save: jest.fn(),
  findOne: jest.fn(),
});

const createMockProductsService = (): Pick<ProductsService, 'findOne'> => ({
  findOne: jest.fn(),
});

describe('InventoriesService', () => {
  let service: InventoriesService;
  let repository: MockRepository<Inventory>;
  let productsService: jest.Mocked<Pick<ProductsService, 'findOne'>>;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        InventoriesService,
        {
          provide: getRepositoryToken(Inventory),
          useValue: createMockRepository(),
        },
        {
          provide: ProductsService,
          useValue: createMockProductsService(),
        },
      ],
    }).compile();

    service = module.get<InventoriesService>(InventoriesService);
    repository = module.get<MockRepository<Inventory>>(
      getRepositoryToken(Inventory),
    );
    productsService = module.get(ProductsService);
  });

  describe('create', () => {
    it('saves and returns an inventory when product exists and inventory does not exist', async () => {
      const createInventoryDto = { productId: 1, quantity: 100 };
      const inventory = { id: 1, ...createInventoryDto } as Inventory;

      productsService.findOne.mockResolvedValue({
        id: 1,
        name: 'MacBook Pro',
        price: 2990000,
      });
      repository.findOne?.mockResolvedValue(null);
      repository.create?.mockReturnValue(inventory);
      repository.save?.mockResolvedValue(inventory);

      await expect(service.create(createInventoryDto)).resolves.toBe(inventory);
      expect(productsService.findOne).toHaveBeenCalledWith(1);
      expect(repository.findOne).toHaveBeenCalledWith({
        where: { productId: 1 },
      });
      expect(repository.create).toHaveBeenCalledWith(createInventoryDto);
      expect(repository.save).toHaveBeenCalledWith(inventory);
    });

    it('throws NotFoundException when product does not exist', async () => {
      productsService.findOne.mockRejectedValue(new NotFoundException());

      await expect(
        service.create({ productId: 999, quantity: 100 }),
      ).rejects.toBeInstanceOf(NotFoundException);
      expect(repository.findOne).not.toHaveBeenCalled();
      expect(repository.save).not.toHaveBeenCalled();
    });

    it('throws ConflictException when inventory already exists for product', async () => {
      productsService.findOne.mockResolvedValue({
        id: 1,
        name: 'MacBook Pro',
        price: 2990000,
      });
      repository.findOne?.mockResolvedValue({
        id: 1,
        productId: 1,
        quantity: 100,
      } as Inventory);

      await expect(
        service.create({ productId: 1, quantity: 100 }),
      ).rejects.toBeInstanceOf(ConflictException);
      expect(repository.save).not.toHaveBeenCalled();
    });
  });

  describe('findByProductId', () => {
    it('returns an inventory when it exists', async () => {
      const inventory = { id: 1, productId: 1, quantity: 100 } as Inventory;

      repository.findOne?.mockResolvedValue(inventory);

      await expect(service.findByProductId(1)).resolves.toBe(inventory);
      expect(repository.findOne).toHaveBeenCalledWith({
        where: { productId: 1 },
      });
    });

    it('throws NotFoundException when inventory does not exist', async () => {
      repository.findOne?.mockResolvedValue(null);

      await expect(service.findByProductId(1)).rejects.toBeInstanceOf(
        NotFoundException,
      );
    });
  });
});
