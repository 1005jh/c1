import amqp from 'amqplib';
import mysql from 'mysql2/promise';

try {
  process.loadEnvFile();
} catch (error) {
  if (error.code !== 'ENOENT') {
    throw error;
  }
}

const DEFAULT_BASE_URL = 'http://localhost:3000';
const DEFAULT_PAYMENT_PROVIDER_BASE_URL = 'http://localhost:4001';
const DEFAULT_RABBITMQ_MANAGEMENT_PORT = 15672;
const DEFAULT_ROUNDS = 5;
const PRODUCT_PRICE = 10000;
const ORDER_QUANTITY = 3;
const INVENTORY_QUANTITY = 100;
const EVENT_TYPE = 'payment.completed';
const EVENT_VERSION = 1;
const EXCHANGE = 'commerce.events';
const ROUTING_KEY = 'payment.completed';
const MAIN_QUEUE = 'commerce.payment.completed';
const RETRY_QUEUE = 'commerce.payment.completed.retry';
const DLQ = 'commerce.payment.completed.dlq';
const CONSUMER_NAME = 'payment-completed-consumer';

const parsePositiveInteger = (value, fallback, name) => {
  if (value === undefined || value === '') {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`${name} must be a positive integer`);
  }

  return parsed;
};

const requiredEnv = (name) => {
  const value = process.env[name];

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
};

const baseUrl = (process.env.BASE_URL ?? DEFAULT_BASE_URL).replace(/\/$/, '');
const paymentProviderBaseUrl = (
  process.env.PAYMENT_PROVIDER_BASE_URL ?? DEFAULT_PAYMENT_PROVIDER_BASE_URL
).replace(/\/$/, '');
const rounds = parsePositiveInteger(
  process.env.ROUNDS,
  DEFAULT_ROUNDS,
  'ROUNDS',
);
const waitTimeoutMs = parsePositiveInteger(
  process.env.WAIT_TIMEOUT_MS,
  10000,
  'WAIT_TIMEOUT_MS',
);
const resetRabbitMqQueues = process.env.RESET_RABBITMQ_QUEUES === '1';
const rabbitMqUrl = requiredEnv('RABBITMQ_URL');
const rabbitMqManagementPort = parsePositiveInteger(
  process.env.RABBITMQ_MANAGEMENT_PORT,
  DEFAULT_RABBITMQ_MANAGEMENT_PORT,
  'RABBITMQ_MANAGEMENT_PORT',
);
const rabbitMqManagementBaseUrl = `http://localhost:${rabbitMqManagementPort}/api`;
const rabbitMqUser = process.env.RABBITMQ_USER ?? 'commerce';
const rabbitMqPassword = process.env.RABBITMQ_PASSWORD ?? 'commerce';
const rabbitMqManagementAuth = `Basic ${Buffer.from(
  `${rabbitMqUser}:${rabbitMqPassword}`,
).toString('base64')}`;

const sleep = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

const requestJson = async (origin, path, options = {}) => {
  const response = await fetch(`${origin}${path}`, {
    ...options,
    headers: {
      'content-type': 'application/json',
      ...(options.headers ?? {}),
    },
  });
  const text = await response.text();
  let body = null;

  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = text;
    }
  }

  return {
    status: response.status,
    ok: response.ok,
    body,
  };
};

const appPostJson = (path, body) =>
  requestJson(baseUrl, path, {
    method: 'POST',
    body: body === undefined ? undefined : JSON.stringify(body),
  });

const providerPostJson = (path, body) =>
  requestJson(paymentProviderBaseUrl, path, {
    method: 'POST',
    body: body === undefined ? undefined : JSON.stringify(body),
  });

const providerGetJson = (path) => requestJson(paymentProviderBaseUrl, path);

const rabbitMqRequest = (path, options = {}) =>
  requestJson(rabbitMqManagementBaseUrl, path, {
    ...options,
    headers: {
      authorization: rabbitMqManagementAuth,
      ...(options.headers ?? {}),
    },
  });

const rabbitMqGetJson = (path) => rabbitMqRequest(path);

const rabbitMqDeleteJson = (path) =>
  rabbitMqRequest(path, { method: 'DELETE' });

const assertOk = (label, response) => {
  if (!response.ok) {
    throw new Error(
      `${label} failed: ${response.status} ${JSON.stringify(response.body)}`,
    );
  }
};

const encodeQueueName = (queueName) => encodeURIComponent(queueName);

const readQueueState = async (queueName) => {
  const response = await rabbitMqGetJson(
    `/queues/%2F/${encodeQueueName(queueName)}`,
  );
  assertOk(`Read queue state for ${queueName}`, response);

  const stats = response.body?.message_stats ?? {};

  return {
    name: queueName,
    messages: response.body?.messages ?? 0,
    messagesReady: response.body?.messages_ready ?? 0,
    messagesUnacknowledged: response.body?.messages_unacknowledged ?? 0,
    consumers: response.body?.consumers ?? 0,
    publish: stats.publish ?? 0,
    deliver: stats.deliver ?? 0,
    deliverGet: stats.deliver_get ?? 0,
    ack: stats.ack ?? 0,
    redeliver: stats.redeliver ?? 0,
  };
};

const readQueueStates = async () => ({
  main: await readQueueState(MAIN_QUEUE),
  retry: await readQueueState(RETRY_QUEUE),
  dlq: await readQueueState(DLQ),
});

const purgeQueue = async (queueName) => {
  const response = await rabbitMqDeleteJson(
    `/queues/%2F/${encodeQueueName(queueName)}/contents`,
  );
  assertOk(`Purge queue ${queueName}`, response);
};

const diffQueueState = (before, after) => ({
  publishDelta: after.publish - before.publish,
  deliverDelta: after.deliver - before.deliver,
  deliverGetDelta: after.deliverGet - before.deliverGet,
  ackDelta: after.ack - before.ack,
  redeliverDelta: after.redeliver - before.redeliver,
});

const diffQueueStates = (before, after) => ({
  main: diffQueueState(before.main, after.main),
  retry: diffQueueState(before.retry, after.retry),
  dlq: diffQueueState(before.dlq, after.dlq),
});

const mainDeliveryObserved = (queueDelta) =>
  queueDelta.main.deliverDelta > 0 || queueDelta.main.deliverGetDelta > 0;

const mainAckObserved = (queueDelta) => queueDelta.main.ackDelta > 0;

const waitForMainAck = async (before) => {
  const startedAt = performance.now();
  let latest = before;

  while (performance.now() - startedAt < waitTimeoutMs) {
    await sleep(500);
    latest = await readQueueStates();
    const delta = diffQueueStates(before, latest);

    if (
      mainDeliveryObserved(delta) &&
      mainAckObserved(delta) &&
      latest.main.messages === 0 &&
      latest.main.messagesReady === 0 &&
      latest.main.messagesUnacknowledged === 0
    ) {
      return latest;
    }
  }

  return latest;
};

const resetProviderCharges = async () => {
  const response = await providerPostJson('/reset');
  assertOk('Reset provider charges', response);
};

const getProviderCharges = async (orderId) => {
  const response = await providerGetJson(`/charges?orderId=${orderId}`);
  assertOk('Get provider charges', response);

  return Array.isArray(response.body?.charges) ? response.body.charges : [];
};

const createProduct = async (round) => {
  const response = await appPostJson('/products', {
    name: `M15 Duplicate Delivery ${Date.now()}-${round}`,
    price: PRODUCT_PRICE,
    description: 'M15 RabbitMQ duplicate delivery experiment',
  });
  assertOk('Create product', response);

  return response.body;
};

const createInventory = async (productId) => {
  const response = await appPostJson('/inventories', {
    productId,
    quantity: INVENTORY_QUANTITY,
  });
  assertOk('Create inventory', response);

  return response.body;
};

const createOrder = async (productId) => {
  const response = await appPostJson('/orders', {
    items: [
      {
        productId,
        quantity: ORDER_QUANTITY,
      },
    ],
  });
  assertOk('Create order', response);

  return response.body;
};

const readPayments = async (connection, orderId) => {
  const [rows] = await connection.execute(
    `
      SELECT
        id,
        orderId,
        amount,
        status,
        providerTransactionId
      FROM payments
      WHERE orderId = ?
      ORDER BY id
    `,
    [orderId],
  );

  return rows;
};

const readOrder = async (connection, orderId) => {
  const [rows] = await connection.execute(
    `
      SELECT
        id,
        status,
        totalAmount
      FROM orders
      WHERE id = ?
      LIMIT 1
    `,
    [orderId],
  );

  return rows[0] ?? null;
};

const readProcessedMessages = async (connection, eventId) => {
  const [rows] = await connection.execute(
    `
      SELECT
        id,
        consumerName,
        eventId,
        eventType,
        processedAt
      FROM processed_messages
      WHERE consumerName = ?
        AND eventId = ?
      ORDER BY id
    `,
    [CONSUMER_NAME, eventId],
  );

  return rows;
};

const createPaymentCompletedEvent = (payment) => ({
  eventId: `payment.completed:${payment.id}`,
  eventType: EVENT_TYPE,
  eventVersion: EVENT_VERSION,
  occurredAt: new Date().toISOString(),
  paymentId: payment.id,
  orderId: payment.orderId,
  amount: payment.amount,
  providerTransactionId: payment.providerTransactionId,
});

const publishDuplicateEvent = (channel, event) => {
  const published = channel.publish(
    EXCHANGE,
    ROUTING_KEY,
    Buffer.from(JSON.stringify(event)),
    {
      contentType: 'application/json',
      persistent: true,
      messageId: event.eventId,
      type: event.eventType,
    },
  );

  if (!published) {
    console.warn('RabbitMQ publish buffer is full');
  }
};

const runRound = async (round, connection, channel) => {
  await resetProviderCharges();

  const product = await createProduct(round);
  await createInventory(product.id);
  const order = await createOrder(product.id);

  const normalBefore = await readQueueStates();
  const paymentResponse = await appPostJson(`/orders/${order.id}/payments`);
  const normalAfter = await waitForMainAck(normalBefore);
  const normalQueueDelta = diffQueueStates(normalBefore, normalAfter);
  const payments = await readPayments(connection, order.id);
  const payment = payments[0] ?? null;
  const persistedOrder = await readOrder(connection, order.id);
  const providerCharges = await getProviderCharges(order.id);

  if (!payment) {
    throw new Error(`Payment for order id ${order.id} was not persisted`);
  }

  if (!payment.providerTransactionId) {
    throw new Error(`Payment ${payment.id} has no providerTransactionId`);
  }

  const duplicateEvent = createPaymentCompletedEvent(payment);
  const expectedEventId = `payment.completed:${payment.id}`;
  const processedMessagesAfterNormal = await readProcessedMessages(
    connection,
    duplicateEvent.eventId,
  );
  const duplicateBefore = await readQueueStates();
  publishDuplicateEvent(channel, duplicateEvent);
  const duplicateAfter = await waitForMainAck(duplicateBefore);
  const duplicateQueueDelta = diffQueueStates(duplicateBefore, duplicateAfter);
  const finalQueueState = await readQueueStates();
  const processedMessagesAfterDuplicate = await readProcessedMessages(
    connection,
    duplicateEvent.eventId,
  );
  const afterDuplicatePayments = await readPayments(connection, order.id);
  const afterDuplicateProviderCharges = await getProviderCharges(order.id);
  const normalEventDelivered = mainDeliveryObserved(normalQueueDelta);
  const normalEventAcked = mainAckObserved(normalQueueDelta);
  const duplicateEventDelivered = mainDeliveryObserved(duplicateQueueDelta);
  const duplicateEventAcked = mainAckObserved(duplicateQueueDelta);
  const sameEventId = duplicateEvent.eventId === expectedEventId;
  const deliveryCount =
    (normalEventDelivered && normalEventAcked ? 1 : 0) +
    (duplicateEventDelivered && duplicateEventAcked ? 1 : 0);
  const ackCount =
    normalQueueDelta.main.ackDelta + duplicateQueueDelta.main.ackDelta;
  const businessProcessCountInferred = processedMessagesAfterDuplicate.length;
  const duplicateSkipCountInferred =
    duplicateEventDelivered &&
    duplicateEventAcked &&
    processedMessagesAfterNormal.length === 1 &&
    processedMessagesAfterDuplicate.length === 1
      ? 1
      : 0;

  return {
    round,
    orderId: order.id,
    paymentId: payment.id,
    eventId: duplicateEvent.eventId,
    expectedEventId,
    sameEventId,
    paymentApiStatus: paymentResponse.status,
    paymentStatus: payment.status,
    orderStatus: persistedOrder?.status ?? null,
    providerChargeCount: providerCharges.length,
    dbPaymentCount: payments.length,
    providerTransactionId: providerCharges[0]?.transactionId ?? null,
    dbProviderTransactionId: payment.providerTransactionId,
    providerTransactionIdMatches:
      providerCharges[0]?.transactionId === payment.providerTransactionId,
    normalEvent: {
      delivered: normalEventDelivered,
      acked: normalEventAcked,
      queueDelta: normalQueueDelta,
    },
    duplicateEvent: {
      delivered: duplicateEventDelivered,
      acked: duplicateEventAcked,
      queueDelta: duplicateQueueDelta,
    },
    deliveryCount,
    ackCount,
    businessProcessCountInferred,
    duplicateSkipCountInferred,
    processedMessageRows: processedMessagesAfterDuplicate.length,
    retryQueueTouched:
      normalQueueDelta.retry.publishDelta !== 0 ||
      duplicateQueueDelta.retry.publishDelta !== 0 ||
      finalQueueState.retry.messages !== 0,
    dlqTouched:
      normalQueueDelta.dlq.publishDelta !== 0 ||
      duplicateQueueDelta.dlq.publishDelta !== 0 ||
      finalQueueState.dlq.messages !== 0,
    providerDuplicateChargeCreated: afterDuplicateProviderCharges.length > 1,
    dbDuplicatePaymentCreated: afterDuplicatePayments.length > 1,
    finalQueues: {
      main: {
        messages: finalQueueState.main.messages,
        messagesReady: finalQueueState.main.messagesReady,
        messagesUnacknowledged: finalQueueState.main.messagesUnacknowledged,
      },
      retry: {
        messages: finalQueueState.retry.messages,
        messagesReady: finalQueueState.retry.messagesReady,
        messagesUnacknowledged: finalQueueState.retry.messagesUnacknowledged,
      },
      dlq: {
        messages: finalQueueState.dlq.messages,
        messagesReady: finalQueueState.dlq.messagesReady,
        messagesUnacknowledged: finalQueueState.dlq.messagesUnacknowledged,
      },
    },
  };
};

const printRound = (result) => {
  console.log(`\nRound ${result.round}`);
  console.log(`  Order ID: ${result.orderId}`);
  console.log(`  Payment ID: ${result.paymentId}`);
  console.log(`  Event ID: ${result.eventId}`);
  console.log(`  Payment Status: ${result.paymentStatus}`);
  console.log(`  Order Status: ${result.orderStatus}`);
  console.log(`  Provider Charge Count: ${result.providerChargeCount}`);
  console.log(
    `  Normal Event: ${JSON.stringify({
      delivered: result.normalEvent.delivered,
      acked: result.normalEvent.acked,
      mainDelta: result.normalEvent.queueDelta.main,
    })}`,
  );
  console.log(
    `  Duplicate Event: ${JSON.stringify({
      delivered: result.duplicateEvent.delivered,
      acked: result.duplicateEvent.acked,
      mainDelta: result.duplicateEvent.queueDelta.main,
    })}`,
  );
  console.log(`  Same Event ID: ${result.sameEventId}`);
  console.log(`  Delivery Count: ${result.deliveryCount}`);
  console.log(`  ACK Count: ${result.ackCount}`);
  console.log(
    `  Business Process Count Inferred: ${result.businessProcessCountInferred}`,
  );
  console.log(
    `  Duplicate Skip Count Inferred: ${result.duplicateSkipCountInferred}`,
  );
  console.log(`  Processed Message Rows: ${result.processedMessageRows}`);
  console.log(`  Final Queues: ${JSON.stringify(result.finalQueues)}`);
};

const main = async () => {
  if (resetRabbitMqQueues) {
    await purgeQueue(MAIN_QUEUE);
    await purgeQueue(RETRY_QUEUE);
    await purgeQueue(DLQ);
  }

  const connection = await mysql.createConnection({
    host: requiredEnv('DB_HOST'),
    port: Number(requiredEnv('DB_PORT')),
    user: requiredEnv('DB_USERNAME'),
    password: requiredEnv('DB_PASSWORD'),
    database: requiredEnv('DB_DATABASE'),
  });
  const rabbitConnection = await amqp.connect(rabbitMqUrl);
  const rabbitChannel = await rabbitConnection.createChannel();

  try {
    const initialQueueState = await readQueueStates();
    const roundResults = [];

    console.log('RabbitMQ Duplicate Delivery Experiment');
    console.log(
      JSON.stringify(
        {
          baseUrl,
          paymentProviderBaseUrl,
          rabbitMqManagementBaseUrl,
          rabbitMqUser,
          rounds,
          resetRabbitMqQueues,
          exchange: EXCHANGE,
          routingKey: ROUTING_KEY,
          queues: {
            main: MAIN_QUEUE,
            retry: RETRY_QUEUE,
            dlq: DLQ,
          },
          expectedNestFailCount:
            process.env.PAYMENT_COMPLETED_CONSUMER_FAIL_COUNT ??
            'set on Nest process',
          initialQueueState,
        },
        null,
        2,
      ),
    );

    for (let round = 1; round <= rounds; round += 1) {
      const result = await runRound(round, connection, rabbitChannel);
      roundResults.push(result);
      printRound(result);
    }

    const totalDeliveryCount = roundResults.reduce(
      (sum, result) => sum + result.deliveryCount,
      0,
    );
    const totalAckCount = roundResults.reduce(
      (sum, result) => sum + result.ackCount,
      0,
    );
    const totalBusinessProcessCount = roundResults.reduce(
      (sum, result) => sum + result.businessProcessCountInferred,
      0,
    );
    const totalDuplicateSkipCount = roundResults.reduce(
      (sum, result) => sum + result.duplicateSkipCountInferred,
      0,
    );
    const totalProcessedMessageRows = roundResults.reduce(
      (sum, result) => sum + result.processedMessageRows,
      0,
    );

    console.log('\nExperiment Summary');
    console.log(
      JSON.stringify(
        {
          duplicateProcessingPrevented: roundResults.every(
            (result) =>
              result.sameEventId &&
              result.paymentStatus === 'SUCCESS' &&
              result.orderStatus === 'PAID' &&
              result.providerChargeCount === 1 &&
              result.dbPaymentCount === 1 &&
              result.normalEvent.delivered &&
              result.normalEvent.acked &&
              result.duplicateEvent.delivered &&
              result.duplicateEvent.acked &&
              result.deliveryCount === 2 &&
              result.ackCount === 2 &&
              result.businessProcessCountInferred === 1 &&
              result.duplicateSkipCountInferred === 1 &&
              result.processedMessageRows === 1 &&
              !result.retryQueueTouched &&
              !result.dlqTouched,
          ),
          totalDeliveryCount,
          totalAckCount,
          totalBusinessProcessCount,
          totalDuplicateSkipCount,
          totalProcessedMessageRows,
          providerDuplicateChargeCreated: roundResults.some(
            (result) => result.providerDuplicateChargeCreated,
          ),
          dbDuplicatePaymentCreated: roundResults.some(
            (result) => result.dbDuplicatePaymentCreated,
          ),
          retryOrDlqInvolved: roundResults.some(
            (result) => result.retryQueueTouched || result.dlqTouched,
          ),
          rounds: roundResults,
        },
        null,
        2,
      ),
    );
  } finally {
    await rabbitChannel.close();
    await rabbitConnection.close();
    await connection.end();
  }
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
