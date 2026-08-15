import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ProductsModule } from '../products/products.module';
import { Inventory } from './entities/inventory.entity';
import { InventoriesController } from './inventories.controller';
import { InventoriesService } from './inventories.service';

@Module({
  imports: [TypeOrmModule.forFeature([Inventory]), ProductsModule],
  controllers: [InventoriesController],
  providers: [InventoriesService],
})
export class InventoriesModule {}
