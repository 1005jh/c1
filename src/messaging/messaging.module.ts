import { Module } from '@nestjs/common';
import { PaymentCompletedConsumer } from './events/payment-completed.consumer';
import { PaymentCompletedPublisher } from './events/payment-completed.publisher';
import { RabbitMqService } from './rabbitmq/rabbitmq.service';

@Module({
  providers: [
    RabbitMqService,
    PaymentCompletedPublisher,
    PaymentCompletedConsumer,
  ],
  exports: [PaymentCompletedPublisher],
})
export class MessagingModule {}
