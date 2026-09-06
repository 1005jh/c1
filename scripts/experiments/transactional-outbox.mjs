import { readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
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
const DEFAULT_VERIFY_TIMEOUT_MS = 30000;
const PRODUCT_PRICE = 10000;
const ORDER_QUANTITY = 3;
const INVENTORY_QUANTITY = 100;
const MAIN_QUEUE = 'commerce.payment.completed';
const RETRY_QUEUE = 'commerce.payment.completed.retry';
const DLQ = 'commerce.payment.completed.dlq';
const CONSUMER_NAME = 'payment-completed-consumer';
const OUTBOX_PENDING = 'PENDING';
const OUTBOX_PUBLISHED = 'PUBLISHED';
const PUBLISH_FAILURE_MESSAGE = 'Injected payment.completed publish failure';
const STATE_VERSION = 1;

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

const mode = process.env.MODE ?? 'prepare';
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
const verifyTimeoutMs = parsePositiveInteger(
  process.env.VERIFY_TIMEOUT_MS,
  DEFAULT_VERIFY_TIMEOUT_MS,
  'VERIFY_TIMEOUT_MS',
);
const resetRabbitMqQueues = process.env.RESET_RABBITMQ_QUEUES === '1';
const stateFile =
  process.env.M18_STATE_FILE ??
  join(tmpdir(), 'commerce-m18-outbox-state.json');
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
    name: `M18 Transactional Outbox ${Date.now()}-${round}`,
    price: PRODUCT_PRICE,
    description: 'M18 transactional outbox experiment',
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

const readOutboxEventsByEventIds = async (connection, eventIds) => {
  if (eventIds.length === 0) {
    return [];
  }

  const [rows] = await connection.query(
    `
      SELECT
        id,
        eventId,
        eventType,
        eventVersion,
        status,
        attempts,
        lastError,
        publishedAt
      FROM outbox_events
      WHERE eventId IN (?)
      ORDER BY id
    `,
    [eventIds],
  );

  return rows;
};

const readOutboxEvent = async (connection, eventId) => {
  const rows = await readOutboxEventsByEventIds(connection, [eventId]);

  return rows[0] ?? null;
};

const readPendingOutboxCount = async (connection) => {
  const [rows] = await connection.execute(
    `
      SELECT COUNT(*) AS count
      FROM outbox_events
      WHERE status = ?
    `,
    [OUTBOX_PENDING],
  );

  return Number(rows[0]?.count ?? 0);
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

const readProcessedCounts = async (connection, eventIds) => {
  const entries = await Promise.all(
    eventIds.map(async (eventId) => [
      eventId,
      (await readProcessedMessages(connection, eventId)).length,
    ]),
  );

  return Object.fromEntries(entries);
};

const compactResponse = (response) => ({
  type: response.type,
  status: response.status,
  body: response.body,
  error: response.error ?? null,
});

const snapshot = async (connection, orderId, eventId) => {
  const [payments, order, providerCharges, processedMessages, outboxEvent] =
    await Promise.all([
      readPayments(connection, orderId),
      readOrder(connection, orderId),
      getProviderCharges(orderId),
      eventId
        ? readProcessedMessages(connection, eventId)
        : Promise.resolve([]),
      eventId ? readOutboxEvent(connection, eventId) : Promise.resolve(null),
    ]);

  return {
    paymentRows: payments,
    paymentRowCount: payments.length,
    paymentId: payments[0]?.id ?? null,
    paymentStatus: payments[0]?.status ?? null,
    providerTransactionId: payments[0]?.providerTransactionId ?? null,
    orderStatus: order?.status ?? null,
    providerChargeCount: providerCharges.length,
    providerTransactionIds: providerCharges.map(
      (charge) => charge.transactionId,
    ),
    processedMessageRows: processedMessages.length,
    outboxEvent,
  };
};

const runPrepareRound = async (round, connection) => {
  const product = await createProduct(round);
  await createInventory(product.id);
  const order = await createOrder(product.id);

  const queueBefore = await readQueueStates();
  const paymentResponse = await appPostJson(`/orders/${order.id}/payments`);
  await sleep(observationDelayMs);
  const queueAfter = await readQueueStates();
  const afterPaymentRead = await snapshot(connection, order.id, null);

  if (!afterPaymentRead.paymentId) {
    throw new Error(`Payment for order id ${order.id} was not persisted`);
  }

  const eventId = `payment.completed:${afterPaymentRead.paymentId}`;
  const afterPayment = await snapshot(connection, order.id, eventId);
  const queueDelta = diffQueueStates(queueBefore, queueAfter);
  const outboxEvent = afterPayment.outboxEvent;
  const prepared =
    paymentResponse.status === 201 &&
    afterPayment.providerChargeCount === 1 &&
    afterPayment.paymentStatus === 'SUCCESS' &&
    afterPayment.orderStatus === 'PAID' &&
    afterPayment.paymentRowCount === 1 &&
    outboxEvent?.status === OUTBOX_PENDING &&
    outboxEvent?.attempts === 0 &&
    afterPayment.processedMessageRows === 0 &&
    queueDelta.main.publishDelta === 0 &&
    queueDelta.main.deliverDelta === 0 &&
    queueDelta.main.deliverGetDelta === 0 &&
    queueDelta.main.ackDelta === 0 &&
    queueDelta.retry.publishDelta === 0 &&
    queueDelta.dlq.publishDelta === 0;

  return {
    round,
    orderId: order.id,
    paymentId: afterPayment.paymentId,
    eventId,
    paymentApi: compactResponse(paymentResponse),
    providerChargeCount: afterPayment.providerChargeCount,
    paymentStatus: afterPayment.paymentStatus,
    orderStatus: afterPayment.orderStatus,
    outboxId: outboxEvent?.id ?? null,
    outboxStatus: outboxEvent?.status ?? null,
    outboxAttempts: outboxEvent?.attempts ?? null,
    outboxLastError: outboxEvent?.lastError ?? null,
    processedMessageRows: afterPayment.processedMessageRows,
    queueDelta,
    queuesAfter: queueCounts(queueAfter),
    prepared,
  };
};

const stateRoundFrom = (result) => ({
  round: result.round,
  orderId: result.orderId,
  paymentId: result.paymentId,
  eventId: result.eventId,
  outboxId: result.outboxId,
});

const runPrepare = async (connection) => {
  const pendingCount = await readPendingOutboxCount(connection);

  if (pendingCount !== 0) {
    throw new Error(
      `Existing PENDING outbox rows found: ${pendingCount}. Inspect them before running prepare.`,
    );
  }

  if (resetRabbitMqQueues) {
    await purgeQueue(MAIN_QUEUE);
    await purgeQueue(RETRY_QUEUE);
    await purgeQueue(DLQ);
  }

  await resetProviderCharges();

  const initialQueueState = await readQueueStates();
  const roundResults = [];

  console.log('Transactional Outbox Experiment - PREPARE');
  console.log(
    JSON.stringify(
      {
        baseUrl,
        paymentProviderBaseUrl,
        rabbitMqManagementBaseUrl,
        rabbitMqUser,
        rounds,
        resetRabbitMqQueues,
        observationDelayMs,
        stateFile,
        expectedNestRelayEnabled:
          process.env.OUTBOX_RELAY_ENABLED ?? 'set on Nest process',
        initialQueueState,
      },
      null,
      2,
    ),
  );

  for (let round = 1; round <= rounds; round += 1) {
    const result = await runPrepareRound(round, connection);
    roundResults.push(result);
    printPrepareRound(result);
  }

  const state = {
    version: STATE_VERSION,
    createdAt: new Date().toISOString(),
    rounds: roundResults.map(stateRoundFrom),
  };

  await writeFile(stateFile, `${JSON.stringify(state, null, 2)}\n`, 'utf8');

  console.log('\nPrepare Summary');
  console.log(
    JSON.stringify(
      {
        preparedOutboxCount: roundResults.filter((result) => result.prepared)
          .length,
        prepared: roundResults.every((result) => result.prepared),
        stateFile,
        rounds: roundResults,
      },
      null,
      2,
    ),
  );
};

const readState = async () => {
  const text = await readFile(stateFile, 'utf8');
  const state = JSON.parse(text);

  if (state.version !== STATE_VERSION || !Array.isArray(state.rounds)) {
    throw new Error(`Invalid M18 state file: ${stateFile}`);
  }

  return state;
};

const buildVerifySnapshot = async (connection, state, queueBefore) => {
  const eventIds = state.rounds.map((round) => round.eventId);
  const outboxEvents = await readOutboxEventsByEventIds(connection, eventIds);
  const processedCounts = await readProcessedCounts(connection, eventIds);
  const snapshots = await Promise.all(
    state.rounds.map((round) =>
      snapshot(connection, round.orderId, round.eventId),
    ),
  );
  const queueState = await readQueueStates();

  return {
    outboxEvents,
    processedCounts,
    snapshots,
    queueState,
    queueDelta: diffQueueStates(queueBefore, queueState),
  };
};

const waitForSnapshot = async (connection, state, queueBefore, predicate) => {
  const startedAt = performance.now();
  let latest = await buildVerifySnapshot(connection, state, queueBefore);

  while (performance.now() - startedAt < verifyTimeoutMs) {
    if (predicate(latest)) {
      return {
        observed: true,
        snapshot: latest,
      };
    }

    await sleep(500);
    latest = await buildVerifySnapshot(connection, state, queueBefore);
  }

  return {
    observed: predicate(latest),
    snapshot: latest,
  };
};

const firstFailedSweepPredicate = (state) => (snapshot) => {
  if (snapshot.outboxEvents.length !== state.rounds.length) {
    return false;
  }

  return snapshot.outboxEvents.every(
    (event) =>
      event.status === OUTBOX_PENDING &&
      event.attempts === 1 &&
      typeof event.lastError === 'string' &&
      event.lastError.includes(PUBLISH_FAILURE_MESSAGE),
  );
};

const finalPublishedPredicate = (state) => (snapshot) => {
  if (snapshot.outboxEvents.length !== state.rounds.length) {
    return false;
  }

  const queuesEmpty =
    snapshot.queueState.main.messages === 0 &&
    snapshot.queueState.main.messagesReady === 0 &&
    snapshot.queueState.main.messagesUnacknowledged === 0 &&
    snapshot.queueState.retry.messages === 0 &&
    snapshot.queueState.retry.messagesReady === 0 &&
    snapshot.queueState.retry.messagesUnacknowledged === 0;

  return (
    queuesEmpty &&
    snapshot.outboxEvents.every(
      (event) =>
        event.status === OUTBOX_PUBLISHED &&
        event.attempts === 2 &&
        event.publishedAt !== null,
    ) &&
    state.rounds.every(
      (round, index) =>
        snapshot.processedCounts[round.eventId] === 1 &&
        snapshot.snapshots[index].paymentStatus === 'SUCCESS' &&
        snapshot.snapshots[index].orderStatus === 'PAID' &&
        snapshot.snapshots[index].providerChargeCount === 1,
    )
  );
};

const runVerify = async (connection) => {
  const state = await readState();
  const queueBefore = await readQueueStates();

  console.log('Transactional Outbox Experiment - VERIFY');
  console.log(
    JSON.stringify(
      {
        baseUrl,
        paymentProviderBaseUrl,
        rabbitMqManagementBaseUrl,
        rabbitMqUser,
        rounds: state.rounds.length,
        verifyTimeoutMs,
        stateFile,
        expectedNestRelayEnabled:
          process.env.OUTBOX_RELAY_ENABLED ?? 'set on Nest process',
        expectedNestPublishFailCount:
          process.env.PAYMENT_COMPLETED_PUBLISH_FAIL_COUNT ??
          'set on Nest process',
        queueBefore,
        state,
      },
      null,
      2,
    ),
  );

  const firstFailedSweep = await waitForSnapshot(
    connection,
    state,
    queueBefore,
    firstFailedSweepPredicate(state),
  );
  printVerifySnapshot('First Failed Relay Sweep', firstFailedSweep);

  const finalPublished = await waitForSnapshot(
    connection,
    state,
    queueBefore,
    finalPublishedPredicate(state),
  );
  printVerifySnapshot('Final Published State', finalPublished);

  const finalRows = state.rounds.map((round, index) => {
    const outboxEvent = finalPublished.snapshot.outboxEvents.find(
      (event) => event.eventId === round.eventId,
    );
    const finalSnapshot = finalPublished.snapshot.snapshots[index];

    return {
      round: round.round,
      orderId: round.orderId,
      paymentId: round.paymentId,
      eventId: round.eventId,
      outboxId: outboxEvent?.id ?? null,
      outboxStatus: outboxEvent?.status ?? null,
      attempts: outboxEvent?.attempts ?? null,
      publishedAt: outboxEvent?.publishedAt ?? null,
      processedMessageRows:
        finalPublished.snapshot.processedCounts[round.eventId] ?? 0,
      paymentStatus: finalSnapshot.paymentStatus,
      orderStatus: finalSnapshot.orderStatus,
      providerChargeCount: finalSnapshot.providerChargeCount,
    };
  });
  const publishFailureCount = firstFailedSweep.snapshot.outboxEvents.filter(
    (event) =>
      event.attempts >= 1 &&
      typeof event.lastError === 'string' &&
      event.lastError.includes(PUBLISH_FAILURE_MESSAGE),
  ).length;
  const eventuallyPublishedCount = finalRows.filter(
    (row) =>
      row.outboxStatus === OUTBOX_PUBLISHED &&
      row.attempts === 2 &&
      row.publishedAt !== null,
  ).length;
  const missingEventFinalCount = finalRows.filter(
    (row) => row.processedMessageRows === 0,
  ).length;
  const recoveredEventCount = finalRows.filter(
    (row) =>
      row.outboxStatus === OUTBOX_PUBLISHED && row.processedMessageRows === 1,
  ).length;

  console.log('\nVerify Summary');
  console.log(
    JSON.stringify(
      {
        firstFailedRelaySweepObserved: firstFailedSweep.observed,
        finalPublishedObserved: finalPublished.observed,
        preparedOutboxCount: state.rounds.length,
        publishFailureCount,
        eventuallyPublishedCount,
        missingEventFinalCount,
        recoveredEventCount,
        finalQueueState: queueCounts(finalPublished.snapshot.queueState),
        finalQueueDelta: finalPublished.snapshot.queueDelta,
        rows: finalRows,
      },
      null,
      2,
    ),
  );
};

const printPrepareRound = (result) => {
  console.log(`\nRound ${result.round}`);
  console.log(`  Order ID: ${result.orderId}`);
  console.log(`  Payment ID: ${result.paymentId}`);
  console.log(`  Event ID: ${result.eventId}`);
  console.log(`  Payment API Status: ${result.paymentApi.status}`);
  console.log(`  Provider Charge Count: ${result.providerChargeCount}`);
  console.log(`  Payment Status: ${result.paymentStatus}`);
  console.log(`  Order Status: ${result.orderStatus}`);
  console.log(`  Outbox ID: ${result.outboxId}`);
  console.log(`  Outbox Status: ${result.outboxStatus}`);
  console.log(`  Outbox Attempts: ${result.outboxAttempts}`);
  console.log(`  Processed Messages: ${result.processedMessageRows}`);
  console.log(`  Queue Delta: ${JSON.stringify(result.queueDelta)}`);
  console.log(`  Prepared: ${result.prepared}`);
};

const printVerifySnapshot = (label, result) => {
  console.log(`\n${label}`);
  console.log(`  Observed: ${result.observed}`);
  console.log(
    `  Outbox: ${JSON.stringify(
      result.snapshot.outboxEvents.map((event) => ({
        id: event.id,
        eventId: event.eventId,
        status: event.status,
        attempts: event.attempts,
        lastError: event.lastError,
        publishedAt: event.publishedAt,
      })),
    )}`,
  );
  console.log(
    `  Processed: ${JSON.stringify(result.snapshot.processedCounts)}`,
  );
  console.log(
    `  Queue State: ${JSON.stringify(queueCounts(result.snapshot.queueState))}`,
  );
  console.log(`  Queue Delta: ${JSON.stringify(result.snapshot.queueDelta)}`);
};

const main = async () => {
  if (!['prepare', 'verify'].includes(mode)) {
    throw new Error('MODE must be prepare or verify');
  }

  const connection = await mysql.createConnection({
    host: requiredEnv('DB_HOST'),
    port: Number(requiredEnv('DB_PORT')),
    user: requiredEnv('DB_USERNAME'),
    password: requiredEnv('DB_PASSWORD'),
    database: requiredEnv('DB_DATABASE'),
  });

  try {
    if (mode === 'prepare') {
      await runPrepare(connection);
      return;
    }

    await runVerify(connection);
  } finally {
    await connection.end();
  }
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
