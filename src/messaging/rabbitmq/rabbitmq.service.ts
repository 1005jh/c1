import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import {
  Channel,
  ChannelModel,
  ConfirmChannel,
  ConsumeMessage,
  Options,
  connect,
} from 'amqplib';
import {
  COMMERCE_EVENTS_EXCHANGE,
  COMMERCE_EVENTS_EXCHANGE_TYPE,
  PAYMENT_COMPLETED_DEAD_ROUTING_KEY,
  PAYMENT_COMPLETED_DLQ,
  PAYMENT_COMPLETED_DLX,
  PAYMENT_COMPLETED_QUEUE,
  PAYMENT_COMPLETED_RETRY_EXCHANGE,
  PAYMENT_COMPLETED_RETRY_QUEUE,
  PAYMENT_COMPLETED_RETRY_ROUTING_KEY,
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
  private readonly paymentCompletedRetryDelayMs: number;
  private readonly publishConfirmTimeoutMs: number;
  private connection?: ChannelModel;
  private publisherChannel?: ConfirmChannel;
  private consumerChannel?: Channel;
  private initializePromise?: Promise<void>;

  constructor(configService: ConfigService) {
    this.url = configService.getOrThrow<string>('RABBITMQ_URL');
    this.paymentCompletedRetryDelayMs = Number(
      configService.get<number>('PAYMENT_COMPLETED_RETRY_DELAY_MS') ?? 1000,
    );
    this.publishConfirmTimeoutMs = Number(
      configService.get<number>('RABBITMQ_PUBLISH_CONFIRM_TIMEOUT_MS') ?? 3000,
    );
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
    const content = Buffer.from(JSON.stringify(payload));
    const eventLike = payload as { eventId?: unknown; eventType?: unknown };

    await this.publishConfirmed(exchange, routingKey, content, {
      contentType: 'application/json',
      persistent: true,
      messageId:
        typeof eventLike.eventId === 'string' ? eventLike.eventId : undefined,
      type:
        typeof eventLike.eventType === 'string'
          ? eventLike.eventType
          : routingKey,
    });
  }

  async publishMessage(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options: Options.Publish = {},
  ): Promise<void> {
    await this.publishConfirmed(exchange, routingKey, content, {
      ...options,
      persistent: true,
    });
  }

  private async publishConfirmed(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options: Options.Publish,
  ): Promise<void> {
    const channel = await this.getPublisherChannel();

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const finish = (error?: unknown): void => {
        if (settled) {
          return;
        }

        settled = true;
        clearTimeout(timeout);
        if (error != null) {
          reject(
            error instanceof Error ? error : new Error(this.messageFrom(error)),
          );
        } else {
          this.logger.log(
            `RabbitMQ publish confirmed: exchange=${exchange} routingKey=${routingKey} messageId=${options.messageId ?? 'unknown'}`,
          );
          resolve();
        }
      };
      const timeout = setTimeout(() => {
        finish(
          new Error(
            `RabbitMQ publisher confirm not observed within ${this.publishConfirmTimeoutMs}ms`,
          ),
        );
      }, this.publishConfirmTimeoutMs);

      try {
        const accepted = channel.publish(
          exchange,
          routingKey,
          content,
          options,
          finish,
        );
        if (!accepted) {
          this.logger.warn('RabbitMQ publish buffer is full');
        }
      } catch (error) {
        finish(error);
      }
    });
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

  private async getPublisherChannel(): Promise<ConfirmChannel> {
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
    this.publisherChannel = await this.connection.createConfirmChannel();
    this.publisherChannel.on('error', (error: Error) => {
      this.logger.error(
        `RabbitMQ publisher channel error: ${this.messageFrom(error)}`,
      );
    });
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

    await channel.assertExchange(
      PAYMENT_COMPLETED_RETRY_EXCHANGE,
      COMMERCE_EVENTS_EXCHANGE_TYPE,
      {
        durable: true,
      },
    );
    await channel.assertQueue(PAYMENT_COMPLETED_RETRY_QUEUE, {
      durable: true,
      arguments: {
        'x-message-ttl': this.paymentCompletedRetryDelayMs,
        'x-dead-letter-exchange': COMMERCE_EVENTS_EXCHANGE,
        'x-dead-letter-routing-key': PAYMENT_COMPLETED_ROUTING_KEY,
      },
    });
    await channel.bindQueue(
      PAYMENT_COMPLETED_RETRY_QUEUE,
      PAYMENT_COMPLETED_RETRY_EXCHANGE,
      PAYMENT_COMPLETED_RETRY_ROUTING_KEY,
    );

    await channel.assertExchange(
      PAYMENT_COMPLETED_DLX,
      COMMERCE_EVENTS_EXCHANGE_TYPE,
      {
        durable: true,
      },
    );
    await channel.assertQueue(PAYMENT_COMPLETED_DLQ, {
      durable: true,
    });
    await channel.bindQueue(
      PAYMENT_COMPLETED_DLQ,
      PAYMENT_COMPLETED_DLX,
      PAYMENT_COMPLETED_DEAD_ROUTING_KEY,
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
