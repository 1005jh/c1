import { BadGatewayException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PaymentStatus } from '../entities/payment-status.enum';
import { FakePaymentClient } from './fake-payment.client';

describe('FakePaymentClient', () => {
  const originalFetch = global.fetch;
  let fetchMock: jest.Mock;
  let client: FakePaymentClient;

  beforeEach(() => {
    fetchMock = jest.fn();
    global.fetch = fetchMock;
    client = new FakePaymentClient({
      get: jest.fn().mockReturnValue('http://payment-provider.test'),
    } as unknown as ConfigService);
  });

  afterEach(() => {
    global.fetch = originalFetch;
  });

  it('sends idempotency key to provider and returns successful charge', async () => {
    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({
          transactionId: 'tx_1',
          status: PaymentStatus.SUCCESS,
          orderId: 1,
          amount: 30000,
        }),
        { status: 200 },
      ),
    );

    await expect(
      client.charge({
        orderId: 1,
        amount: 30000,
        idempotencyKey: 'payment:order:1',
      }),
    ).resolves.toEqual({
      transactionId: 'tx_1',
      status: PaymentStatus.SUCCESS,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      'http://payment-provider.test/charges',
      {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'idempotency-key': 'payment:order:1',
        },
        body: JSON.stringify({
          orderId: 1,
          amount: 30000,
        }),
      },
    );
  });

  it('throws BadGatewayException on provider error', async () => {
    fetchMock.mockResolvedValue(
      new Response(JSON.stringify({ message: 'payload mismatch' }), {
        status: 409,
      }),
    );

    await expect(
      client.charge({
        orderId: 1,
        amount: 30000,
        idempotencyKey: 'payment:order:1',
      }),
    ).rejects.toBeInstanceOf(BadGatewayException);
  });
});
