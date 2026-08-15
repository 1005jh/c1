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
const DEFAULT_CONCURRENCY = 20;
const DEFAULT_ROUNDS = 5;
const PRODUCT_PRICE = 10000;
const ORDER_QUANTITY = 3;
const INVENTORY_QUANTITY = 1000;

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
const concurrency = parsePositiveInteger(
  process.env.CONCURRENCY,
  DEFAULT_CONCURRENCY,
  'CONCURRENCY',
);
const rounds = parsePositiveInteger(
  process.env.ROUNDS,
  DEFAULT_ROUNDS,
  'ROUNDS',
);

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

const assertOk = (label, response) => {
  if (!response.ok) {
    throw new Error(
      `${label} failed: ${response.status} ${JSON.stringify(response.body)}`,
    );
  }
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
    name: `M8 Payment Race ${Date.now()}-${round}`,
    price: PRODUCT_PRICE,
    description: 'M8 concurrent duplicate payment experiment',
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

const createStartGate = () => {
  let release;
  const promise = new Promise((resolve) => {
    release = resolve;
  });

  return { promise, release };
};

const bucketFor = (result) => {
  if (result.type === 'network/error') {
    return 'network/error';
  }

  if (result.status >= 200 && result.status < 300) {
    return '2xx';
  }

  if (result.status === 400) {
    return '400';
  }

  if (result.status === 404) {
    return '404';
  }

  if (result.status === 409) {
    return '409';
  }

  if (result.status >= 500 && result.status < 600) {
    return '5xx';
  }

  return 'other';
};

const summarizeBuckets = (results) => {
  const buckets = {
    '2xx': 0,
    400: 0,
    404: 0,
    409: 0,
    '5xx': 0,
    'network/error': 0,
    other: 0,
  };

  for (const result of results) {
    buckets[bucketFor(result)] += 1;
  }

  return buckets;
};

const summarizeStatusCodes = (results) => {
  const statusCodes = {};

  for (const result of results) {
    const key =
      result.type === 'network/error' ? 'network/error' : String(result.status);
    statusCodes[key] = (statusCodes[key] ?? 0) + 1;
  }

  return statusCodes;
};

const findSampleBody = (results, predicate) => {
  const result = results.find(predicate);

  if (!result) {
    return null;
  }

  return result.type === 'network/error' ? result.error : result.body;
};

const runPaymentWorker = async ({ gate, orderId, workerId }) => {
  await gate.promise;
  const startedAt = performance.now();

  try {
    const response = await appPostJson(`/orders/${orderId}/payments`);

    return {
      workerId,
      type: 'http',
      status: response.status,
      body: response.body,
      elapsedMs: Math.round(performance.now() - startedAt),
    };
  } catch (error) {
    return {
      workerId,
      type: 'network/error',
      error: error instanceof Error ? error.message : String(error),
      elapsedMs: Math.round(performance.now() - startedAt),
    };
  }
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

const runRound = async (round, connection) => {
  await resetProviderCharges();

  const product = await createProduct(round);
  await createInventory(product.id);
  const order = await createOrder(product.id);

  const gate = createStartGate();
  const workers = Array.from({ length: concurrency }, (_, index) =>
    runPaymentWorker({ gate, orderId: order.id, workerId: index + 1 }),
  );

  await new Promise((resolve) => setImmediate(resolve));
  const startedAt = performance.now();
  gate.release();
  const settled = await Promise.allSettled(workers);
  const elapsedMs = Math.round(performance.now() - startedAt);

  const results = settled.map((result, index) => {
    if (result.status === 'fulfilled') {
      return result.value;
    }

    return {
      workerId: index + 1,
      type: 'network/error',
      error:
        result.reason instanceof Error
          ? result.reason.message
          : String(result.reason),
      elapsedMs,
    };
  });

  const providerCharges = await getProviderCharges(order.id);
  const dbPayments = await readPayments(connection, order.id);
  const persistedOrder = await readOrder(connection, order.id);
  const providerChargeCount = providerCharges.length;
  const dbPaymentCount = dbPayments.length;
  const duplicateProviderCharges = Math.max(providerChargeCount - 1, 0);
  const providerDbMismatch = providerChargeCount - dbPaymentCount;

  return {
    round,
    orderId: order.id,
    concurrentRequests: concurrency,
    orderAmount: order.totalAmount,
    elapsedMs,
    httpBuckets: summarizeBuckets(results),
    httpStatusCodes: summarizeStatusCodes(results),
    sample409Response: findSampleBody(
      results,
      (result) => result.type === 'http' && result.status === 409,
    ),
    sample5xxResponse: findSampleBody(
      results,
      (result) =>
        result.type === 'http' && result.status >= 500 && result.status < 600,
    ),
    providerChargeCount,
    dbPaymentCount,
    orderStatus: persistedOrder?.status ?? null,
    duplicateProviderCharges,
    providerDbMismatch,
    providerTransactionIds: providerCharges.map(
      (charge) => charge.transactionId,
    ),
    dbProviderTransactionIds: dbPayments.map(
      (payment) => payment.providerTransactionId,
    ),
    dbPayments,
  };
};

const printRound = (summary) => {
  console.log(`\nRound ${summary.round}`);
  console.log(`  Order ID: ${summary.orderId}`);
  console.log(`  Concurrent Requests: ${summary.concurrentRequests}`);
  console.log(`  Order Amount: ${summary.orderAmount}`);
  console.log(`  Elapsed: ${summary.elapsedMs}ms`);
  console.log(`  HTTP Buckets: ${JSON.stringify(summary.httpBuckets)}`);
  console.log(
    `  HTTP Status Codes: ${JSON.stringify(summary.httpStatusCodes)}`,
  );
  console.log(`  Provider Charge Count: ${summary.providerChargeCount}`);
  console.log(`  DB Payment Count: ${summary.dbPaymentCount}`);
  console.log(`  Order Status: ${summary.orderStatus}`);
  console.log(
    `  Duplicate Provider Charges: ${summary.duplicateProviderCharges}`,
  );
  console.log(`  Provider/DB Mismatch: ${summary.providerDbMismatch}`);
  console.log(
    `  Provider Transaction IDs: ${JSON.stringify(
      summary.providerTransactionIds,
    )}`,
  );
  console.log(
    `  DB Provider Transaction IDs: ${JSON.stringify(
      summary.dbProviderTransactionIds,
    )}`,
  );
  console.log(
    `  Sample 409 Response: ${JSON.stringify(summary.sample409Response)}`,
  );
  console.log(
    `  Sample 5xx Response: ${JSON.stringify(summary.sample5xxResponse)}`,
  );
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
    const roundResults = [];

    console.log('Concurrent Duplicate Payment Experiment');
    console.log(
      JSON.stringify(
        {
          baseUrl,
          paymentProviderBaseUrl,
          concurrency,
          rounds,
          productPrice: PRODUCT_PRICE,
          orderQuantity: ORDER_QUANTITY,
        },
        null,
        2,
      ),
    );

    for (let round = 1; round <= rounds; round += 1) {
      const summary = await runRound(round, connection);
      roundResults.push(summary);
      printRound(summary);
    }

    const duplicatePaymentReproduced = roundResults.some(
      (result) => result.providerChargeCount > 1,
    );
    const providerDbMismatchDetected = roundResults.some(
      (result) => result.providerChargeCount > result.dbPaymentCount,
    );

    console.log('\nExperiment Summary');
    console.log(
      JSON.stringify(
        {
          duplicatePaymentReproduced,
          providerDbMismatchDetected,
          rounds: roundResults.map((result) => ({
            round: result.round,
            orderId: result.orderId,
            httpBuckets: result.httpBuckets,
            httpStatusCodes: result.httpStatusCodes,
            providerChargeCount: result.providerChargeCount,
            dbPaymentCount: result.dbPaymentCount,
            orderStatus: result.orderStatus,
            duplicateProviderCharges: result.duplicateProviderCharges,
            providerDbMismatch: result.providerDbMismatch,
            providerTransactionIds: result.providerTransactionIds,
            dbProviderTransactionIds: result.dbProviderTransactionIds,
            sample409Response: result.sample409Response,
            sample5xxResponse: result.sample5xxResponse,
          })),
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
