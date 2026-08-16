import { Payment } from '../../payments/entities/payment.entity';

export const PAYMENT_COMPLETED_EVENT_TYPE = 'payment.completed';
export const PAYMENT_COMPLETED_EVENT_VERSION = 1;

export type PaymentCompletedEvent = {
  eventId: string;
  eventType: typeof PAYMENT_COMPLETED_EVENT_TYPE;
  eventVersion: typeof PAYMENT_COMPLETED_EVENT_VERSION;
  occurredAt: string;
  paymentId: number;
  orderId: number;
  amount: number;
  providerTransactionId: string;
};

export function createPaymentCompletedEvent(
  payment: Payment,
): PaymentCompletedEvent {
  if (!payment.providerTransactionId) {
    throw new Error('Payment completed event requires providerTransactionId');
  }

  return {
    eventId: `payment.completed:${payment.id}`,
    eventType: PAYMENT_COMPLETED_EVENT_TYPE,
    eventVersion: PAYMENT_COMPLETED_EVENT_VERSION,
    occurredAt: new Date().toISOString(),
    paymentId: payment.id,
    orderId: payment.orderId,
    amount: payment.amount,
    providerTransactionId: payment.providerTransactionId,
  };
}

export function isPaymentCompletedEvent(
  value: unknown,
): value is PaymentCompletedEvent {
  if (!value || typeof value !== 'object') {
    return false;
  }

  const event = value as Partial<PaymentCompletedEvent>;

  return (
    event.eventType === PAYMENT_COMPLETED_EVENT_TYPE &&
    event.eventVersion === PAYMENT_COMPLETED_EVENT_VERSION &&
    typeof event.eventId === 'string' &&
    event.eventId.length > 0 &&
    typeof event.occurredAt === 'string' &&
    event.occurredAt.length > 0 &&
    typeof event.paymentId === 'number' &&
    Number.isFinite(event.paymentId) &&
    typeof event.orderId === 'number' &&
    Number.isFinite(event.orderId) &&
    typeof event.amount === 'number' &&
    Number.isFinite(event.amount) &&
    typeof event.providerTransactionId === 'string' &&
    event.providerTransactionId.length > 0
  );
}
