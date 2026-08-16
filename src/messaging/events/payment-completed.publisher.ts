import { Injectable } from '@nestjs/common';
import { Payment } from '../../payments/entities/payment.entity';
import {
  COMMERCE_EVENTS_EXCHANGE,
  PAYMENT_COMPLETED_ROUTING_KEY,
} from '../rabbitmq/rabbitmq.constants';
import { RabbitMqService } from '../rabbitmq/rabbitmq.service';
import {
  PaymentCompletedEvent,
  createPaymentCompletedEvent,
} from './payment-completed.event';

@Injectable()
export class PaymentCompletedPublisher {
  constructor(private readonly rabbitMqService: RabbitMqService) {}

  async publish(payment: Payment): Promise<PaymentCompletedEvent> {
    const event = createPaymentCompletedEvent(payment);

    await this.rabbitMqService.publishJson(
      COMMERCE_EVENTS_EXCHANGE,
      PAYMENT_COMPLETED_ROUTING_KEY,
      event,
    );

    return event;
  }
}
