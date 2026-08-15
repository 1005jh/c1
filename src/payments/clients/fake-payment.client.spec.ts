import { BadGatewayException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PaymentStatus } from '../entities/payment-status.enum';
import { PaymentProviderUnknownOutcomeException } from '../exceptions/payment-provider-unknown-outcome.exception';
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

  it('throws PaymentProviderUnknownOutcomeException on network error', async () => {
    fetchMock.mockRejectedValue(new Error('socket closed'));

    await expect(
      client.charge({
        orderId: 1,
        amount: 30000,
        idempotencyKey: 'payment:order:1',
      }),
    ).rejects.toBeInstanceOf(PaymentProviderUnknownOutcomeException);
  });

  it('finds charge by idempotency key', async () => {
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
      client.getChargeByIdempotencyKey('payment:order:1'),
    ).resolves.toEqual({
      found: true,
      transactionId: 'tx_1',
      status: PaymentStatus.SUCCESS,
      orderId: 1,
      amount: 30000,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      'http://payment-provider.test/charges/idempotency/payment%3Aorder%3A1',
    );
  });

  it('returns not found when provider has no idempotency result', async () => {
    fetchMock.mockResolvedValue(
      new Response(JSON.stringify({ message: 'Charge not found' }), {
        status: 404,
      }),
    );

    await expect(
      client.getChargeByIdempotencyKey('payment:order:1'),
    ).resolves.toEqual({
      found: false,
    });
  });
});
