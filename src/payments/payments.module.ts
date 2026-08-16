import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { MessagingModule } from '../messaging/messaging.module';
import { FakePaymentClient } from './clients/fake-payment.client';
import { Payment } from './entities/payment.entity';
import { PaymentsController } from './payments.controller';
import { PaymentsService } from './payments.service';

@Module({
  imports: [TypeOrmModule.forFeature([Payment]), MessagingModule],
  controllers: [PaymentsController],
  providers: [FakePaymentClient, PaymentsService],
})
export class PaymentsModule {}
