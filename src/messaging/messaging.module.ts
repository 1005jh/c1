import { Module } from '@nestjs/common';
import { PaymentCompletedConsumerFaultInjector } from './events/payment-completed.consumer-fault-injector';
import { PaymentCompletedConsumer } from './events/payment-completed.consumer';
import { PaymentCompletedPublisher } from './events/payment-completed.publisher';
import { RabbitMqService } from './rabbitmq/rabbitmq.service';

@Module({
  providers: [
    RabbitMqService,
    PaymentCompletedPublisher,
    PaymentCompletedConsumerFaultInjector,
    PaymentCompletedConsumer,
  ],
  exports: [PaymentCompletedPublisher],
})
export class MessagingModule {}
