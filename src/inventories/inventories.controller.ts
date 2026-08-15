import {
  Body,
  Controller,
  Get,
  Param,
  ParseIntPipe,
  Post,
} from '@nestjs/common';
import { CreateInventoryDto } from './dto/create-inventory.dto';
import { InventoriesService } from './inventories.service';

@Controller('inventories')
export class InventoriesController {
  constructor(private readonly inventoriesService: InventoriesService) {}

  @Post()
  create(@Body() createInventoryDto: CreateInventoryDto) {
    return this.inventoriesService.create(createInventoryDto);
  }

  @Get('products/:productId')
  findByProductId(@Param('productId', ParseIntPipe) productId: number) {
    return this.inventoriesService.findByProductId(productId);
  }
}
