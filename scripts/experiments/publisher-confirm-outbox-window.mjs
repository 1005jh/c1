import assert from 'node:assert/strict';
import { readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import mysql from 'mysql2/promise';

try {
  process.loadEnvFile();
} catch (error) {
  if (error.code !== 'ENOENT') throw error;
}

const positiveInteger = (value, fallback, name) => {
  const number = Number(value ?? fallback);
  assert(
    Number.isInteger(number) && number > 0,
    `${name} must be a positive integer`,
  );
  return number;
};
const requiredEnv = (key) => {
  assert(process.env[key], `Missing ${key}`);
  return process.env[key];
};
const mode = process.env.MODE ?? 'prepare';
const rounds = positiveInteger(process.env.ROUNDS, 5, 'ROUNDS');
const timeoutMs = positiveInteger(
  process.env.VERIFY_TIMEOUT_MS,
  30000,
  'VERIFY_TIMEOUT_MS',
);
const baseUrl = (process.env.BASE_URL ?? 'http://localhost:3000').replace(
  /\/$/,
  '',
);
const providerUrl = (
  process.env.PAYMENT_PROVIDER_BASE_URL ?? 'http://localhost:4001'
).replace(/\/$/, '');
const managementUrl = `http://localhost:${process.env.RABBITMQ_MANAGEMENT_PORT ?? 15672}/api`;
const managementAuth = `Basic ${Buffer.from(`${process.env.RABBITMQ_USER ?? 'commerce'}:${process.env.RABBITMQ_PASSWORD ?? 'commerce'}`).toString('base64')}`;
const stateFile =
  process.env.M22_STATE_FILE ??
  join(tmpdir(), 'commerce-m22-publisher-confirm-state.json');
const resultFile = process.env.M22_RESULT_FILE;
const appLogFile = process.env.APP_LOG_FILE;
const queues = {
  main: 'commerce.payment.completed',
  retry: 'commerce.payment.completed.retry',
  dlq: 'commerce.payment.completed.dlq',
};
const consumerName = 'payment-completed-consumer';
const failureMessage =
  'Injected failure after confirmed publish before outbox mark';

async function request(origin, path, body, headers = {}) {
  const response = await fetch(`${origin}${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(10000),
  });
  const text = await response.text();
  const data = text ? JSON.parse(text) : null;
  assert(
    response.ok,
    `${path}: HTTP ${response.status} ${JSON.stringify(data)}`,
  );
  return { status: response.status, data };
}

async function queueState() {
  const result = {};
  for (const [label, name] of Object.entries(queues)) {
    const { data } = await request(
      managementUrl,
      `/queues/%2F/${encodeURIComponent(name)}`,
      undefined,
      { authorization: managementAuth },
    );
    result[label] = {
      messages: data.messages,
      ready: data.messages_ready,
      unacked: data.messages_unacknowledged,
      consumers: data.consumers,
      publish: data.message_stats?.publish ?? 0,
      deliver: data.message_stats?.deliver ?? 0,
      ack: data.message_stats?.ack ?? 0,
    };
  }
  return result;
}

const queuesEmpty = (snapshot) =>
  Object.values(snapshot).every(
    (queue) => queue.messages === 0 && queue.ready === 0 && queue.unacked === 0,
  );
const queueDelta = (before, after) =>
  Object.fromEntries(
    Object.keys(queues).map((name) => [
      name,
      {
        publish: after[name].publish - before[name].publish,
        deliver: after[name].deliver - before[name].deliver,
        ack: after[name].ack - before[name].ack,
      },
    ]),
  );

async function businessState(db, rows) {
  const results = [];
  for (const round of rows) {
    const [payments] = await db.execute(
      'SELECT id, status, providerTransactionId FROM payments WHERE orderId = ?',
      [round.orderId],
    );
    const [orders] = await db.execute(
      'SELECT status FROM orders WHERE id = ?',
      [round.orderId],
    );
    const { data } = await request(
      providerUrl,
      `/charges?orderId=${round.orderId}`,
    );
    assert.equal(payments.length, 1);
    assert.equal(payments[0].status, 'SUCCESS');
    assert.equal(orders[0].status, 'PAID');
    assert.equal(data.charges.length, 1);
    assert.equal(
      payments[0].providerTransactionId,
      data.charges[0].transactionId,
    );
    results.push({
      ...round,
      providerChargeCount: data.charges.length,
      paymentRowCount: payments.length,
      paymentStatus: payments[0].status,
      orderStatus: orders[0].status,
    });
  }
  return results;
}

async function outboxRows(db, eventIds) {
  const [rows] = await db.query(
    `
    SELECT o.id, o.eventId, o.status, o.attempts, o.lastError, o.publishedAt,
      (SELECT COUNT(*) FROM processed_messages p WHERE p.eventId = o.eventId AND p.consumerName = ?) AS processedMessages
    FROM outbox_events o WHERE o.eventId IN (?) ORDER BY o.id
  `,
    [consumerName, eventIds],
  );
  return rows;
}

async function logEvidence(eventIds) {
  if (!appLogFile) return null;
  const lines = (await readFile(appLogFile, 'utf8')).split('\n');
  return Object.fromEntries(
    eventIds.map((eventId) => {
      const target = new RegExp(
        `(?:messageId|eventId)=${eventId.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(?![0-9])`,
      );
      const relevant = lines.filter((line) => target.test(line));
      return [
        eventId,
        {
          confirms: relevant.filter((line) =>
            line.includes('RabbitMQ publish confirmed:'),
          ).length,
          processing: relevant.filter((line) =>
            line.includes('Payment completed event consumed:'),
          ).length,
          duplicateSkips: relevant.filter((line) =>
            line.includes('Payment completed duplicate event skipped:'),
          ).length,
        },
      ];
    }),
  );
}

async function snapshot(db, state) {
  const eventIds = state.rounds.map((round) => round.eventId);
  const rows = await outboxRows(db, eventIds);
  const broker = await queueState();
  return {
    at: new Date().toISOString(),
    rows,
    queues: broker,
    queueDelta: queueDelta(state.queueBaseline, broker),
    logs: await logEvidence(eventIds),
  };
}

const firstSweep = (value, count) =>
  value.rows.length === count &&
  value.rows.every(
    (row) =>
      row.status === 'PENDING' &&
      row.attempts === 1 &&
      row.lastError === failureMessage &&
      row.publishedAt === null &&
      row.processedMessages === 1,
  );
const finalSweep = (value, count) =>
  value.rows.length === count &&
  value.rows.every(
    (row) =>
      row.status === 'PUBLISHED' &&
      row.attempts === 2 &&
      row.lastError === null &&
      row.publishedAt !== null &&
      row.processedMessages === 1,
  ) &&
  queuesEmpty(value.queues) &&
  value.queueDelta.main.publish === count * 2 &&
  value.queueDelta.main.deliver === count * 2 &&
  value.queueDelta.main.ack === count * 2;

async function writeResult(value) {
  if (resultFile)
    await writeFile(resultFile, `${JSON.stringify(value, null, 2)}\n`);
}

async function prepare(db) {
  const [[pending]] = await db.query(
    "SELECT COUNT(*) AS count FROM outbox_events WHERE status = 'PENDING'",
  );
  assert.equal(
    pending.count,
    0,
    'Inspect existing PENDING Outbox rows before this isolated experiment',
  );
  const before = await queueState();
  assert(
    queuesEmpty(before),
    'Queues must be empty; this script does not purge existing messages',
  );
  assert.equal(before.main.consumers, 1, 'Run only one application consumer');
  const state = {
    version: 1,
    createdAt: new Date().toISOString(),
    queueBaseline: before,
    rounds: [],
  };

  for (let round = 1; round <= rounds; round++) {
    const product = await request(baseUrl, '/products', {
      name: `M22 Confirm ${Date.now()}-${round}`,
      price: 10000,
    });
    await request(baseUrl, '/inventories', {
      productId: product.data.id,
      quantity: 100,
    });
    const order = await request(baseUrl, '/orders', {
      items: [{ productId: product.data.id, quantity: 3 }],
    });
    const payment = await request(
      baseUrl,
      `/orders/${order.data.id}/payments`,
      {},
    );
    assert.equal(payment.status, 201);
    const [payments] = await db.execute(
      'SELECT id FROM payments WHERE orderId = ?',
      [order.data.id],
    );
    assert.equal(payments.length, 1);
    state.rounds.push({
      round,
      productId: product.data.id,
      orderId: order.data.id,
      paymentId: payments[0].id,
      eventId: `payment.completed:${payments[0].id}`,
    });
    await writeFile(stateFile, `${JSON.stringify(state, null, 2)}\n`);
  }

  // RabbitMQ management counters are sampled; let them catch up before asserting zero publication.
  await sleep(5500);
  const prepared = await snapshot(db, state);
  assert.equal(prepared.rows.length, rounds);
  for (const row of prepared.rows) {
    assert.equal(row.status, 'PENDING');
    assert.equal(row.attempts, 0);
    assert.equal(row.lastError, null);
    assert.equal(row.publishedAt, null);
    assert.equal(row.processedMessages, 0);
  }
  for (const delta of Object.values(prepared.queueDelta)) {
    assert.equal(delta.publish, 0);
    assert.equal(delta.deliver, 0);
    assert.equal(delta.ack, 0);
  }
  const business = await businessState(db, state.rounds);
  const result = { mode, passed: true, rounds, stateFile, business, prepared };
  await writeResult(result);
  console.log(JSON.stringify(result, null, 2));
}

async function verify(db) {
  const state = JSON.parse(await readFile(stateFile, 'utf8'));
  assert.equal(state.version, 1);
  assert(Array.isArray(state.rounds) && state.rounds.length > 0);
  const count = state.rounds.length;
  const eventIds = state.rounds.map((round) => round.eventId);
  assert.equal(new Set(eventIds).size, count);
  const deadline = Date.now() + timeoutMs;
  let first = null;
  let final = null;

  while (Date.now() < deadline) {
    const current = await snapshot(db, state);
    // DB rows and logs are read separately; reject samples spanning two sweeps.
    const firstLogsMatch =
      current.logs === null ||
      Object.values(current.logs).every(
        (entry) =>
          entry.confirms === 1 &&
          entry.processing === 1 &&
          entry.duplicateSkips === 0,
      );
    if (firstSweep(current, count) && firstLogsMatch) {
      if (!first)
        console.log(
          'Observed post-confirm PENDING state for all target events',
        );
      first = current;
    }
    if (finalSweep(current, count)) {
      final = current;
      break;
    }
    await writeResult({ mode, passed: false, first, latest: current });
    await sleep(100);
  }
  assert(
    first,
    'Did not observe all PENDING/attempts=1 rows with processed_messages=1 before the second sweep',
  );
  assert(
    final,
    'Outbox/broker counters did not reach the expected second-sweep state',
  );
  for (const name of ['retry', 'dlq']) {
    assert.equal(final.queueDelta[name].publish, 0);
    assert.equal(final.queues[name].messages, 0);
  }
  if (final.logs) {
    for (const eventId of eventIds) {
      assert.deepEqual(first.logs[eventId], {
        confirms: 1,
        processing: 1,
        duplicateSkips: 0,
      });
      assert.deepEqual(final.logs[eventId], {
        confirms: 2,
        processing: 1,
        duplicateSkips: 1,
      });
    }
  }
  const business = await businessState(db, state.rounds);
  const result = {
    mode,
    passed: true,
    rounds: count,
    stateFile,
    business,
    first,
    final,
    summary: {
      confirmedPublications: final.logs
        ? Object.values(final.logs).reduce(
            (sum, value) => sum + value.confirms,
            0,
          )
        : null,
      processingLogs: final.logs
        ? Object.values(final.logs).reduce(
            (sum, value) => sum + value.processing,
            0,
          )
        : null,
      duplicateSkipLogs: final.logs
        ? Object.values(final.logs).reduce(
            (sum, value) => sum + value.duplicateSkips,
            0,
          )
        : null,
      processedMessageRows: final.rows.reduce(
        (sum, row) => sum + row.processedMessages,
        0,
      ),
    },
  };
  await writeResult(result);
  console.log(JSON.stringify(result, null, 2));
}

async function main() {
  assert(
    ['prepare', 'verify'].includes(mode),
    'MODE must be prepare or verify',
  );
  const db = await mysql.createConnection({
    host: requiredEnv('DB_HOST'),
    port: Number(requiredEnv('DB_PORT')),
    user: requiredEnv('DB_USERNAME'),
    password: requiredEnv('DB_PASSWORD'),
    database: requiredEnv('DB_DATABASE'),
  });
  try {
    if (mode === 'prepare') await prepare(db);
    else await verify(db);
  } finally {
    await db.end();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
