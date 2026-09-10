import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

export const OUTBOX_MARK_PUBLISHED_FAILURE_MESSAGE =
  'Injected failure after confirmed publish before outbox mark';

@Injectable()
export class OutboxMarkPublishedFaultInjector {
  private remainingFailures: number;

  constructor(configService: ConfigService) {
    this.remainingFailures =
      configService.get<number>('OUTBOX_MARK_PUBLISHED_FAIL_COUNT') ?? 0;
  }

  throwIfEnabled(): void {
    if (this.remainingFailures < 1) {
      return;
    }

    this.remainingFailures -= 1;
    throw new Error(OUTBOX_MARK_PUBLISHED_FAILURE_MESSAGE);
  }
}
