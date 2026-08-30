import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { ProcessedMessage } from './entities/processed-message.entity';

export type ProcessedMessageProcessResult = 'processed' | 'duplicate';

@Injectable()
export class ProcessedMessageService {
  constructor(private readonly dataSource: DataSource) {}

  async processOnce(
    consumerName: string,
    eventId: string,
    eventType: string,
    process: () => void | Promise<void>,
  ): Promise<ProcessedMessageProcessResult> {
    try {
      await this.dataSource.transaction(async (manager) => {
        try {
          await manager.getRepository(ProcessedMessage).insert({
            consumerName,
            eventId,
            eventType,
          });
        } catch (error) {
          if (this.isDuplicateKeyError(error)) {
            throw new ProcessedMessageAlreadyProcessedError();
          }

          throw new ProcessedMessagePersistenceError(error);
        }

        try {
          await process();
        } catch (error) {
          throw new ProcessedMessageHandlerError(error);
        }
      });

      return 'processed';
    } catch (error) {
      if (error instanceof ProcessedMessageAlreadyProcessedError) {
        return 'duplicate';
      }

      if (error instanceof ProcessedMessageHandlerError) {
        throw error.originalError;
      }

      if (error instanceof ProcessedMessagePersistenceError) {
        throw error;
      }

      throw new ProcessedMessagePersistenceError(error);
    }
  }

  private isDuplicateKeyError(error: unknown): boolean {
    const queryError = error as {
      driverError?: { code?: string; errno?: number };
    };

    return (
      queryError.driverError?.code === 'ER_DUP_ENTRY' ||
      queryError.driverError?.errno === 1062
    );
  }
}

export class ProcessedMessagePersistenceError extends Error {
  constructor(readonly originalError: unknown) {
    super('Failed to record processed message');
  }
}

class ProcessedMessageAlreadyProcessedError extends Error {}

class ProcessedMessageHandlerError extends Error {
  constructor(readonly originalError: unknown) {
    super('Processed message handler failed');
  }
}
