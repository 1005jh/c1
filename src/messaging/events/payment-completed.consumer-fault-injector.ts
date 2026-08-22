import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PaymentCompletedEvent } from './payment-completed.event';

export const PAYMENT_COMPLETED_CONSUMER_FAILURE_MESSAGE =
  'Injected payment.completed consumer failure';

@Injectable()
export class PaymentCompletedConsumerFaultInjector {
  private remainingFailures: number;

  constructor(configService: ConfigService) {
    this.remainingFailures =
      configService.get<number>('PAYMENT_COMPLETED_CONSUMER_FAIL_COUNT') ?? 0;
  }

  throwIfEnabled(_event: PaymentCompletedEvent): void {
    if (this.remainingFailures < 1) {
      return;
    }

    this.remainingFailures -= 1;
    throw new Error(PAYMENT_COMPLETED_CONSUMER_FAILURE_MESSAGE);
  }
}
