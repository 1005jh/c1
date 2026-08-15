import { Controller, Param, ParseIntPipe, Post } from '@nestjs/common';
import { PaymentsService } from './payments.service';

@Controller('orders/:orderId/payments')
export class PaymentsController {
  constructor(private readonly paymentsService: PaymentsService) {}

  @Post()
  payOrder(@Param('orderId', ParseIntPipe) orderId: number) {
    return this.paymentsService.payOrder(orderId);
  }

  @Post('reconcile')
  reconcileOrder(@Param('orderId', ParseIntPipe) orderId: number) {
    return this.paymentsService.reconcileOrder(orderId);
  }
}
