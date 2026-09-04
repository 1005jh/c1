import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PaymentCompletedEvent } from './payment-completed.event';

export const PAYMENT_COMPLETED_PUBLISH_FAILURE_MESSAGE =
  'Injected payment.completed publish failure';

@Injectable()
export class PaymentCompletedPublisherFaultInjector {
  private remainingFailures: number;

  constructor(configService: ConfigService) {
    this.remainingFailures =
      configService.get<number>('PAYMENT_COMPLETED_PUBLISH_FAIL_COUNT') ?? 0;
  }

  throwIfEnabled(_event: PaymentCompletedEvent): void {
    if (this.remainingFailures < 1) {
      return;
    }

    this.remainingFailures -= 1;
    throw new Error(PAYMENT_COMPLETED_PUBLISH_FAILURE_MESSAGE);
  }
}
