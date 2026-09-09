import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createClient } from 'redis';

@Injectable()
export class RedisService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RedisService.name);
  private client?: ReturnType<typeof createClient>;

  constructor(private readonly configService: ConfigService) {}

  onModuleInit(): void {
    if (
      this.configService.get<string>(
        'PRODUCT_CURSOR_CACHE_ENABLED',
        'false',
      ) !== 'true'
    ) {
      return;
    }

    try {
      this.client = createClient({
        url: this.configService.get<string>(
          'REDIS_URL',
          'redis://localhost:6379',
        ),
        socket: { connectTimeout: 1000 },
        disableOfflineQueue: true,
        commandOptions: { timeout: 500 },
      });
      this.client.on('error', (error: Error) => {
        this.logFailure('connection', error);
      });
      this.client.on('ready', () => this.logger.log('Redis ready'));

      // Reconnect in the background so optional cache cannot block startup.
      void this.client.connect().catch((error: unknown) => {
        this.logFailure('connect', error);
      });
    } catch (error) {
      this.logFailure('initialize', error);
    }
  }

  onModuleDestroy(): void {
    if (this.client?.isOpen) {
      this.client.destroy();
    }
  }

  async get(key: string): Promise<string | null> {
    if (!this.client?.isReady) {
      return null;
    }

    try {
      return await this.client.get(key);
    } catch (error) {
      this.logFailure('GET', error);
      return null;
    }
  }

  async set(key: string, value: string, ttlSeconds: number): Promise<void> {
    if (!this.client?.isReady) {
      return;
    }

    try {
      await this.client.set(key, value, { EX: ttlSeconds });
    } catch (error) {
      this.logFailure('SET', error);
    }
  }

  async del(key: string): Promise<void> {
    if (!this.client?.isReady) {
      return;
    }

    try {
      await this.client.del(key);
    } catch (error) {
      this.logFailure('DEL', error);
    }
  }

  private logFailure(operation: string, error: unknown): void {
    const causes: unknown[] =
      error instanceof AggregateError ? error.errors : [error];
    const message = causes
      .map((cause) =>
        cause instanceof Error ? cause.message || cause.name : String(cause),
      )
      .join('; ');
    this.logger.warn(`Redis ${operation} failed: ${message}`);
  }
}
