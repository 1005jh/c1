import { DataSource } from 'typeorm';
import { ProcessedMessage } from './entities/processed-message.entity';
import {
  ProcessedMessagePersistenceError,
  ProcessedMessageService,
} from './processed-message.service';

const createDuplicateKeyError = () => ({
  driverError: {
    code: 'ER_DUP_ENTRY',
    errno: 1062,
  },
});

describe('ProcessedMessageService', () => {
  let dataSource: jest.Mocked<Pick<DataSource, 'transaction'>>;
  let repository: { insert: jest.Mock };
  let manager: { getRepository: jest.Mock };
  let service: ProcessedMessageService;

  beforeEach(() => {
    repository = {
      insert: jest.fn().mockResolvedValue(undefined),
    };
    manager = {
      getRepository: jest.fn().mockReturnValue(repository),
    };
    dataSource = {
      transaction: jest.fn(async (callback) => callback(manager)),
    };
    service = new ProcessedMessageService(dataSource as unknown as DataSource);
  });

  it('inserts a processed message marker and runs processing once', async () => {
    const process = jest.fn();

    await expect(
      service.processOnce(
        'payment-completed-consumer',
        'payment.completed:1',
        'payment.completed',
        process,
      ),
    ).resolves.toBe('processed');

    expect(manager.getRepository).toHaveBeenCalledWith(ProcessedMessage);
    expect(repository.insert).toHaveBeenCalledWith({
      consumerName: 'payment-completed-consumer',
      eventId: 'payment.completed:1',
      eventType: 'payment.completed',
    });
    expect(process).toHaveBeenCalledTimes(1);
  });

  it('treats only duplicate-key errors as already processed and skips processing', async () => {
    repository.insert.mockRejectedValueOnce(createDuplicateKeyError());
    const process = jest.fn();

    await expect(
      service.processOnce(
        'payment-completed-consumer',
        'payment.completed:1',
        'payment.completed',
        process,
      ),
    ).resolves.toBe('duplicate');

    expect(process).not.toHaveBeenCalled();
  });

  it('throws persistence errors for general DB failures without running processing', async () => {
    repository.insert.mockRejectedValueOnce(new Error('database unavailable'));
    const process = jest.fn();

    await expect(
      service.processOnce(
        'payment-completed-consumer',
        'payment.completed:1',
        'payment.completed',
        process,
      ),
    ).rejects.toBeInstanceOf(ProcessedMessagePersistenceError);

    expect(process).not.toHaveBeenCalled();
  });

  it('uses consumerName with eventId so another consumer can process the same event id independently', async () => {
    const firstConsumerProcess = jest.fn();
    const secondConsumerProcess = jest.fn();

    await service.processOnce(
      'payment-completed-consumer',
      'payment.completed:1',
      'payment.completed',
      firstConsumerProcess,
    );
    await service.processOnce(
      'other-consumer',
      'payment.completed:1',
      'payment.completed',
      secondConsumerProcess,
    );

    expect(repository.insert).toHaveBeenNthCalledWith(1, {
      consumerName: 'payment-completed-consumer',
      eventId: 'payment.completed:1',
      eventType: 'payment.completed',
    });
    expect(repository.insert).toHaveBeenNthCalledWith(2, {
      consumerName: 'other-consumer',
      eventId: 'payment.completed:1',
      eventType: 'payment.completed',
    });
    expect(firstConsumerProcess).toHaveBeenCalledTimes(1);
    expect(secondConsumerProcess).toHaveBeenCalledTimes(1);
  });

  it('processes once when concurrent duplicate inserts race on the unique key', async () => {
    repository.insert
      .mockResolvedValueOnce(undefined)
      .mockRejectedValueOnce(createDuplicateKeyError());
    const firstProcess = jest.fn();
    const secondProcess = jest.fn();

    const results = await Promise.all([
      service.processOnce(
        'payment-completed-consumer',
        'payment.completed:1',
        'payment.completed',
        firstProcess,
      ),
      service.processOnce(
        'payment-completed-consumer',
        'payment.completed:1',
        'payment.completed',
        secondProcess,
      ),
    ]);

    expect(results).toEqual(['processed', 'duplicate']);
    expect(firstProcess).toHaveBeenCalledTimes(1);
    expect(secondProcess).not.toHaveBeenCalled();
    expect(repository.insert).toHaveBeenCalledTimes(2);
  });

  it('rolls back the marker transaction by rethrowing processing failures', async () => {
    const processError = new Error('business processing failed');

    await expect(
      service.processOnce(
        'payment-completed-consumer',
        'payment.completed:1',
        'payment.completed',
        () => {
          throw processError;
        },
      ),
    ).rejects.toBe(processError);
  });
});
