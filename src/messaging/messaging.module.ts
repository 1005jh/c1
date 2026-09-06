import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ProcessedMessage } from './entities/processed-message.entity';
import { PaymentCompletedConsumerFaultInjector } from './events/payment-completed.consumer-fault-injector';
import { PaymentCompletedConsumer } from './events/payment-completed.consumer';
import { PaymentCompletedPublisher } from './events/payment-completed.publisher';
import { PaymentCompletedPublisherFaultInjector } from './events/payment-completed.publisher-fault-injector';
import { OutboxEvent } from './outbox/entities/outbox-event.entity';
import { OutboxRelayService } from './outbox/outbox-relay.service';
import { PaymentCompletedOutboxWriter } from './outbox/payment-completed-outbox.writer';
import { ProcessedMessageService } from './processed-message.service';
import { RabbitMqService } from './rabbitmq/rabbitmq.service';

@Module({
  imports: [TypeOrmModule.forFeature([ProcessedMessage, OutboxEvent])],
  providers: [
    RabbitMqService,
    PaymentCompletedPublisher,
    PaymentCompletedPublisherFaultInjector,
    PaymentCompletedOutboxWriter,
    OutboxRelayService,
    PaymentCompletedConsumerFaultInjector,
    PaymentCompletedConsumer,
    ProcessedMessageService,
  ],
  exports: [PaymentCompletedOutboxWriter],
})
export class MessagingModule {}
