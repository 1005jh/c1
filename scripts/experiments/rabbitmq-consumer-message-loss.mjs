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
const MAIN_QUEUE = 'commerce.payment.completed';

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

const rabbitMqGetJson = (path) =>
  requestJson(rabbitMqManagementBaseUrl, path, {
    headers: {
      authorization: rabbitMqManagementAuth,
    },
  });

const assertOk = (label, response) => {
  if (!response.ok) {
    throw new Error(
      `${label} failed: ${response.status} ${JSON.stringify(response.body)}`,
    );
  }
};

const encodeQueueName = (queueName) => encodeURIComponent(queueName);

const readMainQueueState = async () => {
  const response = await rabbitMqGetJson(
    `/queues/%2F/${encodeQueueName(MAIN_QUEUE)}`,
  );
  assertOk('Read main queue state', response);
  const stats = response.body?.message_stats ?? {};

  return {
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

const readRetryAndDlqState = async () => {
  const response = await rabbitMqGetJson('/queues/%2F');
  assertOk('Read queue list', response);
  const queues = Array.isArray(response.body) ? response.body : [];
  const names = queues.map((queue) => queue.name).filter(Boolean);

  return {
    queues: names,
    retryQueues: names.filter((name) => name.toLowerCase().includes('retry')),
    deadLetterQueues: names.filter((name) => {
      const loweredName = name.toLowerCase();

      return loweredName.includes('dlq') || loweredName.includes('dead');
    }),
  };
};

const diffQueueState = (before, after) => ({
  publishDelta: after.publish - before.publish,
  deliverDelta: after.deliver - before.deliver,
  deliverGetDelta: after.deliverGet - before.deliverGet,
  ackDelta: after.ack - before.ack,
  redeliverDelta: after.redeliver - before.redeliver,
});

const waitForQueueObservation = async (before, expectedOutcome) => {
  const startedAt = performance.now();
  let latest = before;

  while (performance.now() - startedAt < 10000) {
    await sleep(500);
    latest = await readMainQueueState();
    const delta = diffQueueState(before, latest);
    const delivered = delta.deliverDelta > 0 || delta.deliverGetDelta > 0;
    const queueIsEmpty =
      latest.messages === 0 &&
      latest.messagesReady === 0 &&
      latest.messagesUnacknowledged === 0;

    if (
      expectedOutcome === 'failure' &&
      delta.publishDelta > 0 &&
      delivered &&
      delta.ackDelta === 0 &&
      queueIsEmpty
    ) {
      return latest;
    }

    if (
      expectedOutcome === 'success' &&
      delta.publishDelta > 0 &&
      delivered &&
      delta.ackDelta > 0 &&
      queueIsEmpty
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
    name: `M13 Consumer Loss ${Date.now()}-${round}`,
    price: PRODUCT_PRICE,
    description: 'M13 RabbitMQ consumer message loss experiment',
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

const runPaymentRound = async (round, connection, label, expectedOutcome) => {
  await resetProviderCharges();

  const product = await createProduct(`${label}-${round}`);
  await createInventory(product.id);
  const order = await createOrder(product.id);

  const queueBefore = await readMainQueueState();
  const paymentResponse = await appPostJson(`/orders/${order.id}/payments`);
  const queueAfter = await waitForQueueObservation(
    queueBefore,
    expectedOutcome,
  );
  const payment = await readPayment(connection, order.id);
  const persistedOrder = await readOrder(connection, order.id);
  const providerCharges = await getProviderCharges(order.id);
  const queueDelta = diffQueueState(queueBefore, queueAfter);

  return {
    round,
    label,
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
    publishOccurred: queueDelta.publishDelta > 0,
    deliveryOccurred:
      queueDelta.deliverDelta > 0 || queueDelta.deliverGetDelta > 0,
    consumerSuccessInferred: queueDelta.ackDelta > 0,
    consumerFailureInferred:
      queueDelta.ackDelta === 0 &&
      (queueDelta.deliverDelta > 0 || queueDelta.deliverGetDelta > 0) &&
      queueAfter.messages === 0 &&
      queueAfter.messagesReady === 0 &&
      queueAfter.messagesUnacknowledged === 0,
  };
};

const printRound = (result) => {
  console.log(`\n${result.label} Round ${result.round}`);
  console.log(`  Order ID: ${result.orderId}`);
  console.log(`  Payment ID: ${result.paymentId}`);
  console.log(`  Payment API Status: ${result.paymentApiStatus}`);
  console.log(`  Payment Status: ${result.paymentStatus}`);
  console.log(`  Order Status: ${result.orderStatus}`);
  console.log(`  Provider Charge Count: ${result.providerChargeCount}`);
  console.log(`  Event ID: ${result.eventId}`);
  console.log(`  Queue Delta: ${JSON.stringify(result.queueDelta)}`);
  console.log(
    `  Queue Final: ${JSON.stringify({
      messages: result.queueAfter.messages,
      messagesReady: result.queueAfter.messagesReady,
      messagesUnacknowledged: result.queueAfter.messagesUnacknowledged,
    })}`,
  );
  console.log(`  Publish Occurred: ${result.publishOccurred}`);
  console.log(`  Delivery Occurred: ${result.deliveryOccurred}`);
  console.log(`  Consumer Failure Inferred: ${result.consumerFailureInferred}`);
  console.log(`  Consumer Success Inferred: ${result.consumerSuccessInferred}`);
};

const main = async () => {
  const connection = await mysql.createConnection({
    host: requiredEnv('DB_HOST'),
    port: Number(requiredEnv('DB_PORT')),
    user: requiredEnv('DB_USERNAME'),
    password: requiredEnv('DB_PASSWORD'),
    database: requiredEnv('DB_DATABASE'),
  });

  try {
    const queueStateBefore = await readMainQueueState();
    const retryAndDlqBefore = await readRetryAndDlqState();
    const failureResults = [];

    console.log('RabbitMQ Consumer Message Loss Experiment');
    console.log(
      JSON.stringify(
        {
          baseUrl,
          paymentProviderBaseUrl,
          rabbitMqManagementBaseUrl,
          rabbitMqUser,
          rounds,
          expectedNestFailCount:
            process.env.PAYMENT_COMPLETED_CONSUMER_FAIL_COUNT ??
            'set on Nest process',
          mainQueue: MAIN_QUEUE,
          productPrice: PRODUCT_PRICE,
          orderQuantity: ORDER_QUANTITY,
          initialQueueState: queueStateBefore,
          initialQueues: retryAndDlqBefore.queues,
        },
        null,
        2,
      ),
    );

    for (let round = 1; round <= rounds; round += 1) {
      const result = await runPaymentRound(
        round,
        connection,
        'Failure',
        'failure',
      );
      failureResults.push(result);
      printRound(result);
    }

    const sixthResult = await runPaymentRound(
      rounds + 1,
      connection,
      'Post-Fault',
      'success',
    );
    printRound(sixthResult);

    await sleep(3000);
    const queueStateAfter = await readMainQueueState();
    const retryAndDlqAfter = await readRetryAndDlqState();
    const totalQueueDelta = diffQueueState(queueStateBefore, queueStateAfter);
    const inferredDiscardedMessageCount =
      totalQueueDelta.deliverDelta - totalQueueDelta.ackDelta;

    console.log('\nExperiment Summary');
    console.log(
      JSON.stringify(
        {
          messageLossReproduced: failureResults.every(
            (result) =>
              result.paymentApiStatus >= 200 &&
              result.paymentApiStatus < 300 &&
              result.paymentStatus === 'SUCCESS' &&
              result.orderStatus === 'PAID' &&
              result.providerChargeCount === 1 &&
              result.publishOccurred &&
              result.deliveryOccurred &&
              result.consumerFailureInferred,
          ),
          messageLossReproducedByTotalStats:
            failureResults.every(
              (result) =>
                result.paymentApiStatus >= 200 &&
                result.paymentApiStatus < 300 &&
                result.paymentStatus === 'SUCCESS' &&
                result.orderStatus === 'PAID' &&
                result.providerChargeCount === 1,
            ) &&
            totalQueueDelta.publishDelta >= rounds + 1 &&
            totalQueueDelta.deliverDelta >= rounds + 1 &&
            totalQueueDelta.ackDelta === 1 &&
            inferredDiscardedMessageCount >= rounds &&
            queueStateAfter.messages === 0 &&
            queueStateAfter.messagesReady === 0 &&
            queueStateAfter.messagesUnacknowledged === 0,
          postFaultMessageSucceeded:
            sixthResult.paymentApiStatus >= 200 &&
            sixthResult.paymentApiStatus < 300 &&
            sixthResult.paymentStatus === 'SUCCESS' &&
            sixthResult.orderStatus === 'PAID' &&
            sixthResult.publishOccurred &&
            sixthResult.deliveryOccurred &&
            sixthResult.consumerSuccessInferred,
          retryQueuesExist: retryAndDlqAfter.retryQueues.length > 0,
          deadLetterQueuesExist: retryAndDlqAfter.deadLetterQueues.length > 0,
          failureRoundCount: failureResults.length,
          inferredFailureCount: failureResults.filter(
            (result) => result.consumerFailureInferred,
          ).length,
          inferredSuccessCount:
            failureResults.filter((result) => result.consumerSuccessInferred)
              .length + (sixthResult.consumerSuccessInferred ? 1 : 0),
          queueStateBefore,
          queueStateAfter,
          queueDelta: totalQueueDelta,
          inferredDiscardedMessageCount,
          retryAndDlqState: retryAndDlqAfter,
          failureRounds: failureResults,
          postFaultRound: sixthResult,
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
