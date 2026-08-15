import { BadGatewayException, Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PaymentStatus } from '../entities/payment-status.enum';

type ChargeRequest = {
  orderId: number;
  amount: number;
};

type ChargeResponse = {
  transactionId: string;
  status: PaymentStatus;
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
        },
        body: JSON.stringify(request),
      });
    } catch {
      throw new BadGatewayException('Payment provider request failed');
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

  private async parseResponseBody(response: Response): Promise<{
    transactionId?: unknown;
    status?: unknown;
  } | null> {
    const text = await response.text();

    if (!text) {
      return null;
    }

    try {
      return JSON.parse(text) as {
        transactionId?: unknown;
        status?: unknown;
      };
    } catch {
      throw new BadGatewayException('Payment provider returned invalid JSON');
    }
  }
}
