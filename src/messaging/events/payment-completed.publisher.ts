import { Injectable } from '@nestjs/common';
import {
  COMMERCE_EVENTS_EXCHANGE,
  PAYMENT_COMPLETED_ROUTING_KEY,
} from '../rabbitmq/rabbitmq.constants';
import { RabbitMqService } from '../rabbitmq/rabbitmq.service';
import { PaymentCompletedEvent } from './payment-completed.event';
import { PaymentCompletedPublisherFaultInjector } from './payment-completed.publisher-fault-injector';

@Injectable()
export class PaymentCompletedPublisher {
  constructor(
    private readonly rabbitMqService: RabbitMqService,
    private readonly faultInjector: PaymentCompletedPublisherFaultInjector,
  ) {}

  async publish(event: PaymentCompletedEvent): Promise<PaymentCompletedEvent> {
    this.faultInjector.throwIfEnabled(event);

    await this.rabbitMqService.publishJson(
      COMMERCE_EVENTS_EXCHANGE,
      PAYMENT_COMPLETED_ROUTING_KEY,
      event,
    );

    return event;
  }
}
