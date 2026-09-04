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
const DEFAULT_OBSERVATION_DELAY_MS = 1000;
const PRODUCT_PRICE = 10000;
const ORDER_QUANTITY = 3;
const INVENTORY_QUANTITY = 100;
const MAIN_QUEUE = 'commerce.payment.completed';
const RETRY_QUEUE = 'commerce.payment.completed.retry';
const DLQ = 'commerce.payment.completed.dlq';
const CONSUMER_NAME = 'payment-completed-consumer';
const PROVIDER_RESPONSE_LOSS_FAULT_MODE = 'DROP_RESPONSE_AFTER_SUCCESS';

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
const observationDelayMs = parsePositiveInteger(
  process.env.OBSERVATION_DELAY_MS,
  DEFAULT_OBSERVATION_DELAY_MS,
  'OBSERVATION_DELAY_MS',
);
const includeReconciliation = process.env.INCLUDE_RECONCILIATION === '1';
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
  try {
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
      type: 'http',
      status: response.status,
      ok: response.ok,
      body,
    };
  } catch (error) {
    return {
      type: 'network/error',
      status: null,
      ok: false,
      body: null,
      error: error instanceof Error ? error.message : String(error),
    };
  }
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
      `${label} failed: ${response.status ?? response.type} ${JSON.stringify(
        response.body ?? response.error,
      )}`,
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

const queueCounts = (states) => ({
  main: {
    messages: states.main.messages,
    messagesReady: states.main.messagesReady,
    messagesUnacknowledged: states.main.messagesUnacknowledged,
  },
  retry: {
    messages: states.retry.messages,
    messagesReady: states.retry.messagesReady,
    messagesUnacknowledged: states.retry.messagesUnacknowledged,
  },
  dlq: {
    messages: states.dlq.messages,
    messagesReady: states.dlq.messagesReady,
    messagesUnacknowledged: states.dlq.messagesUnacknowledged,
  },
});

const mainPublishObserved = (queueDelta) => queueDelta.main.publishDelta > 0;

const mainConsumerDeliveryObserved = (queueDelta) =>
  queueDelta.main.deliverDelta > 0 || queueDelta.main.deliverGetDelta > 0;

const resetProviderCharges = async () => {
  const response = await providerPostJson('/reset');
  assertOk('Reset provider charges', response);
};

const enableProviderResponseLossFault = async () => {
  const response = await providerPostJson('/fault-mode', {
    mode: PROVIDER_RESPONSE_LOSS_FAULT_MODE,
    count: 1,
  });
  assertOk('Enable provider response loss fault', response);
};

const getProviderCharges = async (orderId) => {
  const response = await providerGetJson(`/charges?orderId=${orderId}`);
  assertOk('Get provider charges', response);

  return Array.isArray(response.body?.charges) ? response.body.charges : [];
};

const createProduct = async (label, round) => {
  const response = await appPostJson('/products', {
    name: `${label} ${Date.now()}-${round}`,
    price: PRODUCT_PRICE,
    description: 'M17 payment event publish gap experiment',
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

const compactResponse = (response) => ({
  type: response.type,
  status: response.status,
  body: response.body,
  error: response.error ?? null,
});

const snapshot = async (connection, orderId, eventId) => {
  const [payments, order, providerCharges, processedMessages] =
    await Promise.all([
      readPayments(connection, orderId),
      readOrder(connection, orderId),
      getProviderCharges(orderId),
      eventId
        ? readProcessedMessages(connection, eventId)
        : Promise.resolve([]),
    ]);

  return {
    paymentRows: payments,
    paymentRowCount: payments.length,
    paymentStatus: payments[0]?.status ?? null,
    paymentId: payments[0]?.id ?? null,
    providerTransactionId: payments[0]?.providerTransactionId ?? null,
    orderStatus: order?.status ?? null,
    providerChargeCount: providerCharges.length,
    providerTransactionIds: providerCharges.map(
      (charge) => charge.transactionId,
    ),
    processedMessageRows: processedMessages.length,
  };
};

const runNormalRound = async (round, connection) => {
  await resetProviderCharges();

  const product = await createProduct('M17 Publish Gap', round);
  await createInventory(product.id);
  const order = await createOrder(product.id);

  const beforePaymentQueues = await readQueueStates();
  const paymentResponse = await appPostJson(`/orders/${order.id}/payments`);
  await sleep(observationDelayMs);
  const afterFailureQueues = await readQueueStates();
  const afterFirstRead = await snapshot(connection, order.id, null);

  if (!afterFirstRead.paymentId) {
    throw new Error(`Payment for order id ${order.id} was not persisted`);
  }

  const eventId = `payment.completed:${afterFirstRead.paymentId}`;
  const afterFailure = await snapshot(connection, order.id, eventId);
  const failureQueueDelta = diffQueueStates(
    beforePaymentQueues,
    afterFailureQueues,
  );
  const publishObservedAfterFailure = mainPublishObserved(failureQueueDelta);
  const consumerDeliveryObservedAfterFailure =
    mainConsumerDeliveryObserved(failureQueueDelta);
  const consumerProcessingObserved =
    afterFailure.processedMessageRows > 0 ||
    consumerDeliveryObservedAfterFailure;

  const beforeRetryQueues = await readQueueStates();
  const retryResponse = await appPostJson(`/orders/${order.id}/payments`);
  await sleep(observationDelayMs);
  const afterRetryQueues = await readQueueStates();
  const afterRetry = await snapshot(connection, order.id, eventId);
  const retryQueueDelta = diffQueueStates(beforeRetryQueues, afterRetryQueues);
  const additionalPublishAfterRetry = retryQueueDelta.main.publishDelta;
  const additionalConsumerProcessingAfterRetry =
    afterRetry.processedMessageRows - afterFailure.processedMessageRows;
  const retryReturnedSamePayment =
    retryResponse.ok && retryResponse.body?.id === afterFailure.paymentId;
  const publishGapReproduced =
    !paymentResponse.ok &&
    afterFailure.paymentStatus === 'SUCCESS' &&
    afterFailure.orderStatus === 'PAID' &&
    afterFailure.providerChargeCount === 1 &&
    afterFailure.paymentRowCount === 1 &&
    afterFailure.processedMessageRows === 0 &&
    !publishObservedAfterFailure &&
    !consumerProcessingObserved &&
    retryReturnedSamePayment &&
    afterRetry.providerChargeCount === 1 &&
    afterRetry.paymentRowCount === 1 &&
    afterRetry.processedMessageRows === 0 &&
    additionalPublishAfterRetry === 0 &&
    additionalConsumerProcessingAfterRetry === 0;

  return {
    round,
    orderId: order.id,
    paymentId: afterFailure.paymentId,
    eventId,
    firstPaymentApi: compactResponse(paymentResponse),
    firstPaymentApiFailed: !paymentResponse.ok,
    providerChargeCount: afterFailure.providerChargeCount,
    paymentStatus: afterFailure.paymentStatus,
    orderStatus: afterFailure.orderStatus,
    paymentRowCount: afterFailure.paymentRowCount,
    providerTransactionId: afterFailure.providerTransactionId,
    providerTransactionIds: afterFailure.providerTransactionIds,
    publishObservedAfterFailure,
    consumerDeliveryObservedAfterFailure,
    consumerProcessingObserved,
    processedMessageRowsAfterFailure: afterFailure.processedMessageRows,
    failureQueueDelta,
    queuesAfterFailure: queueCounts(afterFailureQueues),
    retryPaymentApi: compactResponse(retryResponse),
    retryReturnedSamePayment,
    paymentRowCountAfterRetry: afterRetry.paymentRowCount,
    providerChargeCountAfterRetry: afterRetry.providerChargeCount,
    processedMessageRowsAfterRetry: afterRetry.processedMessageRows,
    additionalPublishAfterRetry,
    additionalConsumerProcessingAfterRetry,
    retryQueueDelta,
    queuesAfterRetry: queueCounts(afterRetryQueues),
    publishGapReproduced,
  };
};

const runReconciliationScenario = async (connection) => {
  await resetProviderCharges();

  const product = await createProduct('M17 Reconcile Publish Gap', 1);
  await createInventory(product.id);
  const order = await createOrder(product.id);

  await enableProviderResponseLossFault();

  const unknownPaymentResponse = await appPostJson(
    `/orders/${order.id}/payments`,
  );
  const afterUnknownRead = await snapshot(connection, order.id, null);

  if (!afterUnknownRead.paymentId) {
    throw new Error(
      `Unknown payment for order id ${order.id} was not persisted`,
    );
  }

  const eventId = `payment.completed:${afterUnknownRead.paymentId}`;
  const afterUnknown = await snapshot(connection, order.id, eventId);
  const beforeReconcileQueues = await readQueueStates();
  const reconcileResponse = await appPostJson(
    `/orders/${order.id}/payments/reconcile`,
  );
  await sleep(observationDelayMs);
  const afterReconcileQueues = await readQueueStates();
  const afterReconcile = await snapshot(connection, order.id, eventId);
  const reconcileQueueDelta = diffQueueStates(
    beforeReconcileQueues,
    afterReconcileQueues,
  );
  const beforeReconcileRetryQueues = await readQueueStates();
  const reconcileRetryResponse = await appPostJson(
    `/orders/${order.id}/payments/reconcile`,
  );
  await sleep(observationDelayMs);
  const afterReconcileRetryQueues = await readQueueStates();
  const afterReconcileRetry = await snapshot(connection, order.id, eventId);
  const reconcileRetryQueueDelta = diffQueueStates(
    beforeReconcileRetryQueues,
    afterReconcileRetryQueues,
  );
  const retryReturnedSamePayment =
    reconcileRetryResponse.ok &&
    reconcileRetryResponse.body?.id === afterReconcile.paymentId;
  const reconciliationGapReproduced =
    !unknownPaymentResponse.ok &&
    afterUnknown.paymentStatus === 'UNKNOWN' &&
    afterUnknown.orderStatus === 'PENDING_PAYMENT' &&
    afterUnknown.providerChargeCount === 1 &&
    !reconcileResponse.ok &&
    afterReconcile.paymentStatus === 'SUCCESS' &&
    afterReconcile.orderStatus === 'PAID' &&
    afterReconcile.providerChargeCount === 1 &&
    afterReconcile.paymentRowCount === 1 &&
    afterReconcile.processedMessageRows === 0 &&
    !mainPublishObserved(reconcileQueueDelta) &&
    !mainConsumerDeliveryObserved(reconcileQueueDelta) &&
    retryReturnedSamePayment &&
    afterReconcileRetry.providerChargeCount === 1 &&
    afterReconcileRetry.paymentRowCount === 1 &&
    afterReconcileRetry.processedMessageRows === 0 &&
    reconcileRetryQueueDelta.main.publishDelta === 0 &&
    !mainConsumerDeliveryObserved(reconcileRetryQueueDelta);

  return {
    orderId: order.id,
    paymentId: afterReconcile.paymentId,
    eventId,
    unknownPaymentApi: compactResponse(unknownPaymentResponse),
    afterUnknown,
    reconcileApi: compactResponse(reconcileResponse),
    afterReconcile,
    reconcileQueueDelta,
    queuesAfterReconcile: queueCounts(afterReconcileQueues),
    reconcileRetryApi: compactResponse(reconcileRetryResponse),
    retryReturnedSamePayment,
    afterReconcileRetry,
    reconcileRetryQueueDelta,
    queuesAfterReconcileRetry: queueCounts(afterReconcileRetryQueues),
    reconciliationGapReproduced,
  };
};

const printRound = (result) => {
  console.log(`\nRound ${result.round}`);
  console.log(`  Order ID: ${result.orderId}`);
  console.log(`  Payment ID: ${result.paymentId}`);
  console.log(`  Event ID: ${result.eventId}`);
  console.log(`  First Payment API Status: ${result.firstPaymentApi.status}`);
  console.log(`  Provider Charge Count: ${result.providerChargeCount}`);
  console.log(`  Payment Status: ${result.paymentStatus}`);
  console.log(`  Order Status: ${result.orderStatus}`);
  console.log(
    `  RabbitMQ Publish Observed: ${result.publishObservedAfterFailure}`,
  );
  console.log(
    `  Consumer Success Observed: ${result.consumerProcessingObserved}`,
  );
  console.log(
    `  Processed Message Rows: ${result.processedMessageRowsAfterFailure}`,
  );
  console.log(`  Main/Retry/DLQ: ${JSON.stringify(result.queuesAfterFailure)}`);
  console.log(`  Retry API Status: ${result.retryPaymentApi.status}`);
  console.log(
    `  Retry Returned Same Payment: ${result.retryReturnedSamePayment}`,
  );
  console.log(
    `  Payment Rows After Retry: ${result.paymentRowCountAfterRetry}`,
  );
  console.log(
    `  Provider Charge Count After Retry: ${result.providerChargeCountAfterRetry}`,
  );
  console.log(
    `  Additional Publish After Retry: ${result.additionalPublishAfterRetry}`,
  );
  console.log(
    `  Additional Consumer Processing After Retry: ${result.additionalConsumerProcessingAfterRetry}`,
  );
  console.log(
    `  Processed Messages After Retry: ${result.processedMessageRowsAfterRetry}`,
  );
  console.log(`  Publish Gap Reproduced: ${result.publishGapReproduced}`);
};

const printReconciliation = (result) => {
  console.log('\nReconciliation Secondary Scenario');
  console.log(`  Order ID: ${result.orderId}`);
  console.log(`  Payment ID: ${result.paymentId}`);
  console.log(`  Event ID: ${result.eventId}`);
  console.log(
    `  Unknown Payment API Status: ${result.unknownPaymentApi.status}`,
  );
  console.log(
    `  After Unknown: ${JSON.stringify({
      providerChargeCount: result.afterUnknown.providerChargeCount,
      paymentStatus: result.afterUnknown.paymentStatus,
      orderStatus: result.afterUnknown.orderStatus,
      processedMessageRows: result.afterUnknown.processedMessageRows,
    })}`,
  );
  console.log(`  Reconcile API Status: ${result.reconcileApi.status}`);
  console.log(
    `  After Reconcile: ${JSON.stringify({
      providerChargeCount: result.afterReconcile.providerChargeCount,
      paymentStatus: result.afterReconcile.paymentStatus,
      orderStatus: result.afterReconcile.orderStatus,
      processedMessageRows: result.afterReconcile.processedMessageRows,
    })}`,
  );
  console.log(
    `  Reconcile RabbitMQ Delta: ${JSON.stringify(result.reconcileQueueDelta)}`,
  );
  console.log(
    `  Reconcile Retry API Status: ${result.reconcileRetryApi.status}`,
  );
  console.log(
    `  Retry Returned Same Payment: ${result.retryReturnedSamePayment}`,
  );
  console.log(
    `  After Reconcile Retry: ${JSON.stringify({
      providerChargeCount: result.afterReconcileRetry.providerChargeCount,
      paymentRowCount: result.afterReconcileRetry.paymentRowCount,
      processedMessageRows: result.afterReconcileRetry.processedMessageRows,
      additionalPublish: result.reconcileRetryQueueDelta.main.publishDelta,
    })}`,
  );
  console.log(
    `  Reconciliation Gap Reproduced: ${result.reconciliationGapReproduced}`,
  );
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

  try {
    const initialQueueState = await readQueueStates();
    const roundResults = [];

    console.log('Payment Event Publish Gap Experiment');
    console.log(
      JSON.stringify(
        {
          baseUrl,
          paymentProviderBaseUrl,
          rabbitMqManagementBaseUrl,
          rabbitMqUser,
          rounds,
          includeReconciliation,
          resetRabbitMqQueues,
          observationDelayMs,
          expectedNestPublishFailCount:
            process.env.PAYMENT_COMPLETED_PUBLISH_FAIL_COUNT ??
            'set on Nest process',
          expectedNestConsumerFailCount:
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

    for (let round = 1; round <= rounds; round += 1) {
      const result = await runNormalRound(round, connection);
      roundResults.push(result);
      printRound(result);
    }

    const reconciliationResult = includeReconciliation
      ? await runReconciliationScenario(connection)
      : null;

    if (reconciliationResult) {
      printReconciliation(reconciliationResult);
    }

    const publishGapReproducedCount = roundResults.filter(
      (result) => result.publishGapReproduced,
    ).length;
    const totalProviderCharges = roundResults.reduce(
      (sum, result) => sum + result.providerChargeCountAfterRetry,
      0,
    );
    const totalPaymentRows = roundResults.reduce(
      (sum, result) => sum + result.paymentRowCountAfterRetry,
      0,
    );
    const missingEventCount = roundResults.filter(
      (result) =>
        result.paymentStatus === 'SUCCESS' &&
        result.orderStatus === 'PAID' &&
        result.processedMessageRowsAfterRetry === 0,
    ).length;
    const retryRecoveredMissingEventCount = roundResults.filter(
      (result) =>
        result.additionalPublishAfterRetry > 0 ||
        result.additionalConsumerProcessingAfterRetry > 0 ||
        result.processedMessageRowsAfterRetry >
          result.processedMessageRowsAfterFailure,
    ).length;

    console.log('\nExperiment Summary');
    console.log(
      JSON.stringify(
        {
          publishGapReproduced: publishGapReproducedCount === rounds,
          publishGapReproducedCount,
          totalProviderCharges,
          totalPaymentRows,
          missingEventCount,
          retryRecoveredMissingEventCount,
          reconciliationExecuted: reconciliationResult !== null,
          reconciliationGapReproduced:
            reconciliationResult?.reconciliationGapReproduced ?? null,
          rounds: roundResults,
          reconciliation: reconciliationResult,
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
