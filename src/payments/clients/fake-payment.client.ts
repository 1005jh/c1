import { BadGatewayException, Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PaymentStatus } from '../entities/payment-status.enum';
import { PaymentProviderUnknownOutcomeException } from '../exceptions/payment-provider-unknown-outcome.exception';

type ChargeRequest = {
  orderId: number;
  amount: number;
  idempotencyKey: string;
};

type ChargeResponse = {
  transactionId: string;
  status: PaymentStatus;
};

export type ChargeLookupResult =
  | {
      found: true;
      transactionId: string;
      status: PaymentStatus;
      orderId: number;
      amount: number;
    }
  | {
      found: false;
    };

@Injectable()
export class FakePaymentClient {
  private readonly baseUrl: string;

  constructor(configService: ConfigService) {
    this.baseUrl =
      configService.get<string>('PAYMENT_PROVIDER_BASE_URL') ??
      'http://localhost:4001';
  }

  async charge(request: ChargeRequest): Promise<ChargeResponse> {
    let response: Response;

    try {
      response = await fetch(`${this.baseUrl}/charges`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'idempotency-key': request.idempotencyKey,
        },
        body: JSON.stringify({
          orderId: request.orderId,
          amount: request.amount,
        }),
      });
    } catch {
      throw new PaymentProviderUnknownOutcomeException();
    }

    const body = await this.parseResponseBody(response);

    if (!response.ok) {
      throw new BadGatewayException('Payment provider returned an error');
    }

    if (
      !body ||
      typeof body.transactionId !== 'string' ||
      body.status !== PaymentStatus.SUCCESS
    ) {
      throw new BadGatewayException(
        'Payment provider returned invalid response',
      );
    }

    return {
      transactionId: body.transactionId,
      status: body.status,
    };
  }

  async getChargeByIdempotencyKey(
    idempotencyKey: string,
  ): Promise<ChargeLookupResult> {
    let response: Response;

    try {
      response = await fetch(
        `${this.baseUrl}/charges/idempotency/${encodeURIComponent(idempotencyKey)}`,
      );
    } catch {
      throw new BadGatewayException('Payment provider lookup request failed');
    }

    if (response.status === 404) {
      return { found: false };
    }

    const body = await this.parseResponseBody(response);

    if (!response.ok) {
      throw new BadGatewayException('Payment provider returned an error');
    }

    if (
      !body ||
      typeof body.transactionId !== 'string' ||
      body.status !== PaymentStatus.SUCCESS ||
      typeof body.orderId !== 'number' ||
      typeof body.amount !== 'number'
    ) {
      throw new BadGatewayException(
        'Payment provider returned invalid response',
      );
    }

    return {
      found: true,
      transactionId: body.transactionId,
      status: body.status,
      orderId: body.orderId,
      amount: body.amount,
    };
  }

  private async parseResponseBody(response: Response): Promise<{
    transactionId?: unknown;
    status?: unknown;
    orderId?: unknown;
    amount?: unknown;
  } | null> {
    const text = await response.text();

    if (!text) {
      return null;
    }

    try {
      return JSON.parse(text) as {
        transactionId?: unknown;
        status?: unknown;
        orderId?: unknown;
        amount?: unknown;
      };
    } catch {
      throw new BadGatewayException('Payment provider returned invalid JSON');
    }
  }
}
