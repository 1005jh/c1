import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Channel, ConsumeMessage } from 'amqplib';
import { PAYMENT_COMPLETED_QUEUE } from '../rabbitmq/rabbitmq.constants';
import { RabbitMqService } from '../rabbitmq/rabbitmq.service';
import {
  PaymentCompletedEvent,
  isPaymentCompletedEvent,
} from './payment-completed.event';

@Injectable()
export class PaymentCompletedConsumer implements OnModuleInit {
  private readonly logger = new Logger(PaymentCompletedConsumer.name);

  constructor(private readonly rabbitMqService: RabbitMqService) {}

  async onModuleInit(): Promise<void> {
    await this.rabbitMqService.consume(
      PAYMENT_COMPLETED_QUEUE,
      (message, channel) => this.handleMessage(message, channel),
    );
  }

  handleMessage(message: ConsumeMessage | null, channel: Channel): void {
    if (!message) {
      return;
    }

    try {
      const event = this.parseEvent(message);
      this.process(event);
      channel.ack(message);
    } catch (error) {
      this.logger.error(
        `Payment completed event handling failed: ${this.messageFrom(error)}`,
      );
      channel.nack(message, false, false);
    }
  }

  private parseEvent(message: ConsumeMessage): PaymentCompletedEvent {
    let payload: unknown;

    try {
      payload = JSON.parse(message.content.toString('utf8'));
    } catch {
      throw new Error('Invalid JSON payload');
    }

    if (!isPaymentCompletedEvent(payload)) {
      throw new Error('Invalid payment.completed payload');
    }

    return payload;
  }

  private process(event: PaymentCompletedEvent): void {
    this.logger.log(
      `Payment completed event consumed: eventId=${event.eventId} paymentId=${event.paymentId} orderId=${event.orderId} amount=${event.amount} providerTransactionId=${event.providerTransactionId}`,
    );
  }

  private messageFrom(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
  }
}
