import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Channel, ChannelModel, ConsumeMessage, connect } from 'amqplib';
import {
  COMMERCE_EVENTS_EXCHANGE,
  COMMERCE_EVENTS_EXCHANGE_TYPE,
  PAYMENT_COMPLETED_QUEUE,
  PAYMENT_COMPLETED_ROUTING_KEY,
} from './rabbitmq.constants';

export type RabbitMqMessageHandler = (
  message: ConsumeMessage | null,
  channel: Channel,
) => void | Promise<void>;

@Injectable()
export class RabbitMqService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RabbitMqService.name);
  private readonly url: string;
  private connection?: ChannelModel;
  private publisherChannel?: Channel;
  private consumerChannel?: Channel;
  private initializePromise?: Promise<void>;

  constructor(configService: ConfigService) {
    this.url = configService.getOrThrow<string>('RABBITMQ_URL');
  }

  async onModuleInit(): Promise<void> {
    await this.initialize();
  }

  async onModuleDestroy(): Promise<void> {
    await this.closeChannel('consumer channel', this.consumerChannel);
    await this.closeChannel('publisher channel', this.publisherChannel);

    if (this.connection) {
      try {
        await this.connection.close();
      } catch (error) {
        this.logger.warn(
          `Failed to close RabbitMQ connection: ${this.messageFrom(error)}`,
        );
      }
    }
  }

  async publishJson(
    exchange: string,
    routingKey: string,
    payload: unknown,
  ): Promise<void> {
    const channel = await this.getPublisherChannel();
    const content = Buffer.from(JSON.stringify(payload));
    const eventLike = payload as { eventId?: unknown; eventType?: unknown };

    const published = channel.publish(exchange, routingKey, content, {
      contentType: 'application/json',
      persistent: true,
      messageId:
        typeof eventLike.eventId === 'string' ? eventLike.eventId : undefined,
      type:
        typeof eventLike.eventType === 'string'
          ? eventLike.eventType
          : routingKey,
    });

    if (!published) {
      this.logger.warn('RabbitMQ publish buffer is full');
    }
  }

  async consume(queue: string, handler: RabbitMqMessageHandler): Promise<void> {
    const channel = await this.getConsumerChannel();

    await channel.consume(
      queue,
      (message) => {
        void Promise.resolve(handler(message, channel)).catch((error) => {
          this.logger.error(
            `RabbitMQ message handler failed: ${this.messageFrom(error)}`,
          );
        });
      },
      { noAck: false },
    );
  }

  private async getPublisherChannel(): Promise<Channel> {
    await this.initialize();

    if (!this.publisherChannel) {
      throw new Error('RabbitMQ publisher channel is not initialized');
    }

    return this.publisherChannel;
  }

  private async getConsumerChannel(): Promise<Channel> {
    await this.initialize();

    if (!this.consumerChannel) {
      throw new Error('RabbitMQ consumer channel is not initialized');
    }

    return this.consumerChannel;
  }

  private async initialize(): Promise<void> {
    this.initializePromise ??= this.connectAndDeclareTopology();
    await this.initializePromise;
  }

  private async connectAndDeclareTopology(): Promise<void> {
    this.connection = await connect(this.url);
    this.publisherChannel = await this.connection.createChannel();
    this.consumerChannel = await this.connection.createChannel();

    await this.assertTopology(this.publisherChannel);
    await this.assertTopology(this.consumerChannel);
  }

  private async assertTopology(channel: Channel): Promise<void> {
    await channel.assertExchange(
      COMMERCE_EVENTS_EXCHANGE,
      COMMERCE_EVENTS_EXCHANGE_TYPE,
      {
        durable: true,
      },
    );
    await channel.assertQueue(PAYMENT_COMPLETED_QUEUE, {
      durable: true,
    });
    await channel.bindQueue(
      PAYMENT_COMPLETED_QUEUE,
      COMMERCE_EVENTS_EXCHANGE,
      PAYMENT_COMPLETED_ROUTING_KEY,
    );
  }

  private async closeChannel(name: string, channel?: Channel): Promise<void> {
    if (!channel) {
      return;
    }

    try {
      await channel.close();
    } catch (error) {
      this.logger.warn(
        `Failed to close RabbitMQ ${name}: ${this.messageFrom(error)}`,
      );
    }
  }

  private messageFrom(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
  }
}
