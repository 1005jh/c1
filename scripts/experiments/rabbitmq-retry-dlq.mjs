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
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_RETRY_DELAY_MS = 1000;
const PRODUCT_PRICE = 10000;
const ORDER_QUANTITY = 3;
const INVENTORY_QUANTITY = 100;
const MAIN_QUEUE = 'commerce.payment.completed';
const RETRY_QUEUE = 'commerce.payment.completed.retry';
const DLQ = 'commerce.payment.completed.dlq';

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

const parseNonNegativeInteger = (value, fallback, name) => {
  if (value === undefined || value === '') {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer`);
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
const expectedOutcome = process.env.EXPECTED_OUTCOME ?? 'transient';
const maxRetries = parseNonNegativeInteger(
  process.env.PAYMENT_COMPLETED_MAX_RETRIES,
  DEFAULT_MAX_RETRIES,
  'PAYMENT_COMPLETED_MAX_RETRIES',
);
const retryDelayMs = parsePositiveInteger(
  process.env.PAYMENT_COMPLETED_RETRY_DELAY_MS,
  DEFAULT_RETRY_DELAY_MS,
  'PAYMENT_COMPLETED_RETRY_DELAY_MS',
);
const waitTimeoutMs = parsePositiveInteger(
  process.env.WAIT_TIMEOUT_MS,
  Math.max(10000, retryDelayMs * (maxRetries + 2) + 5000),
  'WAIT_TIMEOUT_MS',
);
const resetRabbitMqQueues = process.env.RESET_RABBITMQ_QUEUES === '1';
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
    expire: stats.expire ?? 0,
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
  expireDelta: after.expire - before.expire,
});

const diffQueueStates = (before, after) => ({
  main: diffQueueState(before.main, after.main),
  retry: diffQueueState(before.retry, after.retry),
  dlq: diffQueueState(before.dlq, after.dlq),
});

const queuesAreEmpty = (states) =>
  states.main.messages === 0 &&
  states.main.messagesReady === 0 &&
  states.main.messagesUnacknowledged === 0 &&
  states.retry.messages === 0 &&
  states.retry.messagesReady === 0 &&
  states.retry.messagesUnacknowledged === 0;

const isExpectedObservation = (before, after) => {
  const delta = diffQueueStates(before, after);
  const dlqDelta = after.dlq.messages - before.dlq.messages;

  if (expectedOutcome === 'permanent' || expectedOutcome === 'dlq') {
    // Queue depth can update before the sampled publish/ACK counters.
    return (
      queuesAreEmpty(after) &&
      dlqDelta >= 1 &&
      delta.main.ackDelta >= maxRetries + 1 &&
      delta.retry.publishDelta >= maxRetries &&
      delta.dlq.publishDelta >= 1
    );
  }

  return (
    queuesAreEmpty(after) &&
    dlqDelta === 0 &&
    delta.main.ackDelta >= 1 &&
    delta.retry.publishDelta >= 1
  );
};

const waitForQueueObservation = async (before) => {
  const startedAt = performance.now();
  let latest = before;

  while (performance.now() - startedAt < waitTimeoutMs) {
    await sleep(500);
    latest = await readQueueStates();

    if (isExpectedObservation(before, latest)) {
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

const createProduct = async () => {
  const response = await appPostJson('/products', {
    name: `M14 Retry DLQ ${Date.now()}`,
    price: PRODUCT_PRICE,
    description: 'M14 RabbitMQ retry and DLQ experiment',
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

const readPayment = async (connection, orderId) => {
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
      LIMIT 1
    `,
    [orderId],
  );

  return rows[0] ?? null;
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

const runPayment = async (connection) => {
  await resetProviderCharges();

  const product = await createProduct();
  await createInventory(product.id);
  const order = await createOrder(product.id);
  const queueBefore = await readQueueStates();
  const paymentResponse = await appPostJson(`/orders/${order.id}/payments`);
  const queueAfter = await waitForQueueObservation(queueBefore);
  const payment = await readPayment(connection, order.id);
  const persistedOrder = await readOrder(connection, order.id);
  const providerCharges = await getProviderCharges(order.id);
  const queueDelta = diffQueueStates(queueBefore, queueAfter);
  const failureCount =
    queueDelta.retry.publishDelta + queueDelta.dlq.publishDelta;
  const processingAttempts = queueDelta.main.ackDelta;
  const successCount = Math.max(processingAttempts - failureCount, 0);

  return {
    orderId: order.id,
    paymentId: payment?.id ?? null,
    paymentApiStatus: paymentResponse.status,
    paymentStatus: payment?.status ?? null,
    orderStatus: persistedOrder?.status ?? null,
    providerChargeCount: providerCharges.length,
    providerTransactionId: providerCharges[0]?.transactionId ?? null,
    dbProviderTransactionId: payment?.providerTransactionId ?? null,
    providerTransactionIdMatches:
      providerCharges[0]?.transactionId === payment?.providerTransactionId,
    eventId: payment ? `payment.completed:${payment.id}` : null,
    queueBefore,
    queueAfter,
    queueDelta,
    processingAttempts,
    failureCount,
    retryCount: queueDelta.retry.publishDelta,
    successCount,
    dlqMessageDelta: queueAfter.dlq.messages - queueBefore.dlq.messages,
    finalQueues: {
      main: {
        messages: queueAfter.main.messages,
        messagesReady: queueAfter.main.messagesReady,
        messagesUnacknowledged: queueAfter.main.messagesUnacknowledged,
      },
      retry: {
        messages: queueAfter.retry.messages,
        messagesReady: queueAfter.retry.messagesReady,
        messagesUnacknowledged: queueAfter.retry.messagesUnacknowledged,
      },
      dlq: {
        messages: queueAfter.dlq.messages,
        messagesReady: queueAfter.dlq.messagesReady,
        messagesUnacknowledged: queueAfter.dlq.messagesUnacknowledged,
      },
    },
  };
};

const printResult = (result) => {
  console.log('\nPayment Result');
  console.log(`  Order ID: ${result.orderId}`);
  console.log(`  Payment ID: ${result.paymentId}`);
  console.log(`  Payment API Status: ${result.paymentApiStatus}`);
  console.log(`  Payment Status: ${result.paymentStatus}`);
  console.log(`  Order Status: ${result.orderStatus}`);
  console.log(`  Provider Charge Count: ${result.providerChargeCount}`);
  console.log(`  Event ID: ${result.eventId}`);
  console.log(`  Processing Attempts Inferred: ${result.processingAttempts}`);
  console.log(`  Failure Count Inferred: ${result.failureCount}`);
  console.log(`  Retry Count Inferred: ${result.retryCount}`);
  console.log(`  Success Count Inferred: ${result.successCount}`);
  console.log(`  Final Queues: ${JSON.stringify(result.finalQueues)}`);
  console.log(`  Queue Delta: ${JSON.stringify(result.queueDelta)}`);
};

const main = async () => {
  if (!['transient', 'permanent', 'dlq'].includes(expectedOutcome)) {
    throw new Error(
      'EXPECTED_OUTCOME must be one of transient, permanent, or dlq',
    );
  }

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

  try {
    const initialQueueState = await readQueueStates();

    console.log('RabbitMQ Retry/DLQ Experiment');
    console.log(
      JSON.stringify(
        {
          baseUrl,
          paymentProviderBaseUrl,
          rabbitMqManagementBaseUrl,
          rabbitMqUser,
          expectedOutcome,
          maxRetries,
          retryDelayMs,
          waitTimeoutMs,
          resetRabbitMqQueues,
          expectedNestFailCount:
            process.env.PAYMENT_COMPLETED_CONSUMER_FAIL_COUNT ??
            'set on Nest process',
          queues: {
            main: MAIN_QUEUE,
            retry: RETRY_QUEUE,
            dlq: DLQ,
          },
          initialQueueState,
        },
        null,
        2,
      ),
    );

    const result = await runPayment(connection);
    printResult(result);

    console.log('\nExperiment Summary');
    console.log(
      JSON.stringify(
        {
          expectedOutcome,
          transientSucceeded:
            expectedOutcome === 'transient' &&
            result.paymentApiStatus >= 200 &&
            result.paymentApiStatus < 300 &&
            result.paymentStatus === 'SUCCESS' &&
            result.orderStatus === 'PAID' &&
            result.providerChargeCount === 1 &&
            result.retryCount > 0 &&
            result.successCount === 1 &&
            result.finalQueues.main.messages === 0 &&
            result.finalQueues.retry.messages === 0 &&
            result.finalQueues.dlq.messages === initialQueueState.dlq.messages,
          permanentMovedToDlq:
            (expectedOutcome === 'permanent' || expectedOutcome === 'dlq') &&
            result.paymentApiStatus >= 200 &&
            result.paymentApiStatus < 300 &&
            result.paymentStatus === 'SUCCESS' &&
            result.orderStatus === 'PAID' &&
            result.providerChargeCount === 1 &&
            result.retryCount === maxRetries &&
            result.successCount === 0 &&
            result.finalQueues.main.messages === 0 &&
            result.finalQueues.retry.messages === 0 &&
            result.dlqMessageDelta === 1,
          noInfiniteRetry:
            result.finalQueues.main.messages === 0 &&
            result.finalQueues.retry.messages === 0,
          dlqMessagePreserved:
            result.finalQueues.dlq.messagesReady ===
              result.finalQueues.dlq.messages &&
            result.finalQueues.dlq.messagesUnacknowledged === 0,
          result,
        },
        null,
        2,
      ),
    );
  } finally {
    await connection.end();
  }
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
