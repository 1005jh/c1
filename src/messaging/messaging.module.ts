import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ProcessedMessage } from './entities/processed-message.entity';
import { PaymentCompletedConsumerFaultInjector } from './events/payment-completed.consumer-fault-injector';
import { PaymentCompletedConsumer } from './events/payment-completed.consumer';
import { PaymentCompletedPublisher } from './events/payment-completed.publisher';
import { PaymentCompletedPublisherFaultInjector } from './events/payment-completed.publisher-fault-injector';
import { ProcessedMessageService } from './processed-message.service';
import { RabbitMqService } from './rabbitmq/rabbitmq.service';

@Module({
  imports: [TypeOrmModule.forFeature([ProcessedMessage])],
  providers: [
    RabbitMqService,
    PaymentCompletedPublisher,
    PaymentCompletedPublisherFaultInjector,
    PaymentCompletedConsumerFaultInjector,
    PaymentCompletedConsumer,
    ProcessedMessageService,
  ],
  exports: [PaymentCompletedPublisher],
})
export class MessagingModule {}
