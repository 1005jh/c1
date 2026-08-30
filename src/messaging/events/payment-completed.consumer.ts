import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Channel, ConsumeMessage } from 'amqplib';
import {
  PAYMENT_COMPLETED_DEAD_ROUTING_KEY,
  PAYMENT_COMPLETED_DLX,
  PAYMENT_COMPLETED_FAILURE_TYPE_HEADER,
  PAYMENT_COMPLETED_QUEUE,
  PAYMENT_COMPLETED_RETRY_COUNT_HEADER,
  PAYMENT_COMPLETED_RETRY_EXCHANGE,
  PAYMENT_COMPLETED_RETRY_ROUTING_KEY,
} from '../rabbitmq/rabbitmq.constants';
import { RabbitMqService } from '../rabbitmq/rabbitmq.service';
import {
  PaymentCompletedEvent,
  isPaymentCompletedEvent,
} from './payment-completed.event';
import { PaymentCompletedConsumerFaultInjector } from './payment-completed.consumer-fault-injector';
import {
  ProcessedMessagePersistenceError,
  ProcessedMessageService,
} from '../processed-message.service';

export const PAYMENT_COMPLETED_CONSUMER_NAME = 'payment-completed-consumer';

@Injectable()
export class PaymentCompletedConsumer implements OnModuleInit {
  private readonly logger = new Logger(PaymentCompletedConsumer.name);
  private readonly maxRetries: number;

  constructor(
    private readonly rabbitMqService: RabbitMqService,
    private readonly faultInjector: PaymentCompletedConsumerFaultInjector,
    private readonly processedMessageService: ProcessedMessageService,
    configService: ConfigService,
  ) {
    this.maxRetries = Number(
      configService.get<number>('PAYMENT_COMPLETED_MAX_RETRIES') ?? 3,
    );
  }

  async onModuleInit(): Promise<void> {
    await this.rabbitMqService.consume(
      PAYMENT_COMPLETED_QUEUE,
      (message, channel) => this.handleMessage(message, channel),
    );
  }

  async handleMessage(
    message: ConsumeMessage | null,
    channel: Channel,
  ): Promise<void> {
    if (!message) {
      return;
    }

    let event: PaymentCompletedEvent;

    try {
      event = this.parseEvent(message);
    } catch (error) {
      this.logger.error(
        `Payment completed event handling failed: ${this.messageFrom(error)}`,
      );
      await this.moveToDlq(
        message,
        this.eventIdFromMessage(message),
        this.retryCountFrom(message),
        this.failureTypeFrom(error),
      );
      channel.ack(message);

      return;
    }

    try {
      this.faultInjector.throwIfEnabled(event);
      const result = await this.processedMessageService.processOnce(
        PAYMENT_COMPLETED_CONSUMER_NAME,
        event.eventId,
        event.eventType,
        () => this.process(event),
      );

      if (result === 'duplicate') {
        this.logger.warn(
          `Payment completed duplicate event skipped: eventId=${event.eventId}`,
        );
      }
    } catch (error) {
      if (error instanceof ProcessedMessagePersistenceError) {
        throw error;
      }

      this.logger.error(
        `Payment completed event handling failed: ${this.messageFrom(error)}`,
      );
      await this.handleProcessingFailure(message, event);
      channel.ack(message);

      return;
    }

    channel.ack(message);
  }

  private parseEvent(message: ConsumeMessage): PaymentCompletedEvent {
    let payload: unknown;

    try {
      payload = JSON.parse(message.content.toString('utf8'));
    } catch {
      throw new NonRetryablePaymentCompletedEventError(
        'Invalid JSON payload',
        'invalid-json',
      );
    }

    if (!isPaymentCompletedEvent(payload)) {
      throw new NonRetryablePaymentCompletedEventError(
        'Invalid payment.completed payload',
        'invalid-payload',
      );
    }

    return payload;
  }

  private async handleProcessingFailure(
    message: ConsumeMessage,
    event: PaymentCompletedEvent,
  ): Promise<void> {
    const retryCount = this.retryCountFrom(message);

    if (retryCount < this.maxRetries) {
      const nextRetryCount = retryCount + 1;

      await this.rabbitMqService.publishMessage(
        PAYMENT_COMPLETED_RETRY_EXCHANGE,
        PAYMENT_COMPLETED_RETRY_ROUTING_KEY,
        message.content,
        this.publishOptionsFrom(message, {
          [PAYMENT_COMPLETED_RETRY_COUNT_HEADER]: nextRetryCount,
        }),
      );

      this.logger.warn(
        `Payment completed event retry scheduled: eventId=${event.eventId} retry=${nextRetryCount}/${this.maxRetries}`,
      );

      return;
    }

    await this.moveToDlq(
      message,
      event.eventId,
      retryCount,
      'processing-failed',
    );
  }

  private async moveToDlq(
    message: ConsumeMessage,
    eventId: string,
    retryCount: number,
    failureType: string,
  ): Promise<void> {
    await this.rabbitMqService.publishMessage(
      PAYMENT_COMPLETED_DLX,
      PAYMENT_COMPLETED_DEAD_ROUTING_KEY,
      message.content,
      this.publishOptionsFrom(message, {
        [PAYMENT_COMPLETED_RETRY_COUNT_HEADER]: retryCount,
        [PAYMENT_COMPLETED_FAILURE_TYPE_HEADER]: failureType,
      }),
    );

    this.logger.warn(
      `Payment completed event moved to DLQ: eventId=${eventId} retryCount=${retryCount}`,
    );
  }

  private publishOptionsFrom(
    message: ConsumeMessage,
    headers: Record<string, unknown>,
  ) {
    return {
      contentType: message.properties.contentType,
      messageId: message.properties.messageId,
      type: message.properties.type,
      headers: {
        ...(message.properties.headers ?? {}),
        ...headers,
      },
    };
  }

  private retryCountFrom(message: ConsumeMessage): number {
    const value =
      message.properties.headers?.[PAYMENT_COMPLETED_RETRY_COUNT_HEADER];

    if (typeof value === 'number' && Number.isInteger(value) && value >= 0) {
      return value;
    }

    if (typeof value === 'string' && /^\d+$/.test(value)) {
      return Number(value);
    }

    return 0;
  }

  private eventIdFromMessage(message: ConsumeMessage): string {
    if (typeof message.properties.messageId === 'string') {
      return message.properties.messageId;
    }

    return 'unknown';
  }

  private failureTypeFrom(error: unknown): string {
    if (error instanceof NonRetryablePaymentCompletedEventError) {
      return error.failureType;
    }

    return 'unknown';
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

class NonRetryablePaymentCompletedEventError extends Error {
  constructor(
    message: string,
    readonly failureType: string,
  ) {
    super(message);
  }
}
