export const COMMERCE_EVENTS_EXCHANGE = 'commerce.events';
export const COMMERCE_EVENTS_EXCHANGE_TYPE = 'direct';
export const PAYMENT_COMPLETED_ROUTING_KEY = 'payment.completed';
export const PAYMENT_COMPLETED_QUEUE = 'commerce.payment.completed';
export const PAYMENT_COMPLETED_RETRY_EXCHANGE =
  'commerce.payment.completed.retry';
export const PAYMENT_COMPLETED_RETRY_ROUTING_KEY = 'payment.completed.retry';
export const PAYMENT_COMPLETED_RETRY_QUEUE = 'commerce.payment.completed.retry';
export const PAYMENT_COMPLETED_DLX = 'commerce.payment.completed.dlx';
export const PAYMENT_COMPLETED_DEAD_ROUTING_KEY = 'payment.completed.dead';
export const PAYMENT_COMPLETED_DLQ = 'commerce.payment.completed.dlq';
export const PAYMENT_COMPLETED_RETRY_COUNT_HEADER = 'x-retry-count';
export const PAYMENT_COMPLETED_FAILURE_TYPE_HEADER = 'x-failure-type';
