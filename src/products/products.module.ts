import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { RedisModule } from '../redis/redis.module';
import { Product } from './entities/product.entity';
import { ProductCursorCacheService } from './product-cursor-cache.service';
import { ProductsController } from './products.controller';
import { ProductsService } from './products.service';

@Module({
  imports: [TypeOrmModule.forFeature([Product]), RedisModule],
  controllers: [ProductsController],
  providers: [ProductsService, ProductCursorCacheService],
  exports: [ProductsService],
})
export class ProductsModule {}
