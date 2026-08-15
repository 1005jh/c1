import { BadGatewayException } from '@nestjs/common';

export class PaymentProviderUnknownOutcomeException extends BadGatewayException {
  constructor() {
    super('Payment provider outcome is unknown');
  }
}
