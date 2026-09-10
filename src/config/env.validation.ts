import { Type, plainToInstance } from 'class-transformer';
import {
  IsInt,
  IsIn,
  IsNotEmpty,
  IsOptional,
  IsString,
  Max,
  Min,
  validateSync,
} from 'class-validator';

class EnvironmentVariables {
  @IsString()
  @IsNotEmpty()
  DB_HOST!: string;

  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(65535)
  DB_PORT!: number;

  @IsString()
  @IsNotEmpty()
  DB_USERNAME!: string;

  @IsString()
  @IsNotEmpty()
  DB_PASSWORD!: string;

  @IsString()
  @IsNotEmpty()
  DB_DATABASE!: string;

  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(65535)
  PORT!: number;

  @IsString()
  @IsNotEmpty()
  RABBITMQ_URL!: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  RABBITMQ_PUBLISH_CONFIRM_TIMEOUT_MS?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  OUTBOX_MARK_PUBLISHED_FAIL_COUNT?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  PAYMENT_COMPLETED_PUBLISH_FAIL_COUNT?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  PAYMENT_COMPLETED_CONSUMER_FAIL_COUNT?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  PAYMENT_COMPLETED_MAX_RETRIES?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  PAYMENT_COMPLETED_RETRY_DELAY_MS?: number;

  @IsOptional()
  @IsString()
  @IsIn(['true', 'false'])
  OUTBOX_RELAY_ENABLED?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  OUTBOX_RELAY_INTERVAL_MS?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  OUTBOX_RELAY_BATCH_SIZE?: number;

  @IsOptional()
  @IsString()
  REDIS_URL?: string;

  @IsOptional()
  @IsString()
  @IsIn(['true', 'false'])
  PRODUCT_CURSOR_CACHE_ENABLED?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  PRODUCT_CURSOR_CACHE_TTL_SECONDS?: number;
}

export function validate(config: Record<string, unknown>) {
  const validatedConfig = plainToInstance(EnvironmentVariables, config, {
    enableImplicitConversion: true,
  });

  const errors = validateSync(validatedConfig, {
    skipMissingProperties: false,
  });

  if (errors.length > 0) {
    const messages = errors
      .flatMap((error) => Object.values(error.constraints ?? {}))
      .join('; ');

    throw new Error(`Environment validation failed: ${messages}`);
  }

  return validatedConfig;
}
