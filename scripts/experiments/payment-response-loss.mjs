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
const DEFAULT_ROUNDS = 5;
const PRODUCT_PRICE = 10000;
const ORDER_QUANTITY = 3;
const INVENTORY_QUANTITY = 100;
const FAULT_MODE = 'DROP_RESPONSE_AFTER_SUCCESS';

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

const providerPostJson = (path, body, headers = {}) =>
  requestJson(paymentProviderBaseUrl, path, {
    method: 'POST',
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });

const providerGetJson = (path) => requestJson(paymentProviderBaseUrl, path);

const assertOk = (label, response) => {
  if (!response.ok) {
    throw new Error(
      `${label} failed: ${response.status ?? response.type} ${JSON.stringify(
        response.body ?? response.error,
      )}`,
    );
  }
};

const resetProvider = async () => {
  const response = await providerPostJson('/reset');
  assertOk('Reset provider', response);
};

const enableDropResponseFault = async () => {
  const response = await providerPostJson('/fault-mode', {
    mode: FAULT_MODE,
    count: 1,
  });
  assertOk('Enable response loss fault', response);
};

const getProviderCharges = async (orderId) => {
  const response = await providerGetJson(`/charges?orderId=${orderId}`);
  assertOk('Get provider charges', response);

  return Array.isArray(response.body?.charges) ? response.body.charges : [];
};

const createProduct = async (round) => {
  const response = await appPostJson('/products', {
    name: `M10 Response Loss ${Date.now()}-${round}`,
    price: PRODUCT_PRICE,
    description: 'M10 payment response loss experiment',
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

const runDirectProviderVerification = async () => {
  const orderId = 990001;
  const amount = PRODUCT_PRICE * ORDER_QUANTITY;
  const idempotencyKey = `payment:order:${orderId}`;

  await resetProvider();
  await enableDropResponseFault();

  const droppedResponse = await providerPostJson(
    '/charges',
    { orderId, amount },
    { 'idempotency-key': idempotencyKey },
  );
  const chargesAfterDrop = await getProviderCharges(orderId);
  const firstTransactionId = chargesAfterDrop[0]?.transactionId ?? null;
  const replayResponse = await providerPostJson(
    '/charges',
    { orderId, amount },
    { 'idempotency-key': idempotencyKey },
  );
  const chargesAfterReplay = await getProviderCharges(orderId);
  const replayTransactionId = replayResponse.body?.transactionId ?? null;

  return {
    orderId,
    amount,
    idempotencyKey,
    droppedResponse: {
      type: droppedResponse.type,
      status: droppedResponse.status,
      body: droppedResponse.body,
      error: droppedResponse.error ?? null,
    },
    replayResponse: {
      type: replayResponse.type,
      status: replayResponse.status,
      body: replayResponse.body,
      error: replayResponse.error ?? null,
    },
    transactionIdAfterDrop: firstTransactionId,
    replayTransactionId,
    transactionIdMatches: firstTransactionId === replayTransactionId,
    chargeCountAfterDrop: chargesAfterDrop.length,
    chargeCountAfterReplay: chargesAfterReplay.length,
    chargeCountIncreased: chargesAfterReplay.length > chargesAfterDrop.length,
  };
};

const runRound = async (round, connection) => {
  await resetProvider();

  const product = await createProduct(round);
  await createInventory(product.id);
  const order = await createOrder(product.id);

  await enableDropResponseFault();

  const paymentResponse = await appPostJson(`/orders/${order.id}/payments`);
  const providerCharges = await getProviderCharges(order.id);
  const dbPayments = await readPayments(connection, order.id);
  const persistedOrder = await readOrder(connection, order.id);
  const providerChargeCount = providerCharges.length;
  const dbPaymentCount = dbPayments.length;
  const providerDbMismatch = providerChargeCount - dbPaymentCount;

  return {
    round,
    orderId: order.id,
    orderAmount: order.totalAmount,
    paymentApi: {
      type: paymentResponse.type,
      status: paymentResponse.status,
      body: paymentResponse.body,
      error: paymentResponse.error ?? null,
    },
    providerChargeCount,
    providerTransactionIds: providerCharges.map(
      (charge) => charge.transactionId,
    ),
    dbPaymentCount,
    dbProviderTransactionIds: dbPayments.map(
      (payment) => payment.providerTransactionId,
    ),
    orderStatus: persistedOrder?.status ?? null,
    providerDbMismatch,
  };
};

const printDirectVerification = (summary) => {
  console.log('\nDirect Provider Response Loss Verification');
  console.log(JSON.stringify(summary, null, 2));
};

const printRound = (summary) => {
  console.log(`\nRound ${summary.round}`);
  console.log(`  Order ID: ${summary.orderId}`);
  console.log(`  Order Amount: ${summary.orderAmount}`);
  console.log(`  Payment API Type: ${summary.paymentApi.type}`);
  console.log(`  Payment API Status: ${summary.paymentApi.status}`);
  console.log(`  Payment API Body: ${JSON.stringify(summary.paymentApi.body)}`);
  console.log(
    `  Payment API Error: ${JSON.stringify(summary.paymentApi.error)}`,
  );
  console.log(`  Provider Charge Count: ${summary.providerChargeCount}`);
  console.log(
    `  Provider Transaction IDs: ${JSON.stringify(
      summary.providerTransactionIds,
    )}`,
  );
  console.log(`  DB Payment Count: ${summary.dbPaymentCount}`);
  console.log(
    `  DB Provider Transaction IDs: ${JSON.stringify(
      summary.dbProviderTransactionIds,
    )}`,
  );
  console.log(`  Order Status: ${summary.orderStatus}`);
  console.log(`  Provider/DB Mismatch: ${summary.providerDbMismatch}`);
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

    console.log('Payment Response Loss Experiment');
    console.log(
      JSON.stringify(
        {
          baseUrl,
          paymentProviderBaseUrl,
          rounds,
          productPrice: PRODUCT_PRICE,
          orderQuantity: ORDER_QUANTITY,
          faultMode: FAULT_MODE,
        },
        null,
        2,
      ),
    );

    const directVerification = await runDirectProviderVerification();
    printDirectVerification(directVerification);

    for (let round = 1; round <= rounds; round += 1) {
      const summary = await runRound(round, connection);
      roundResults.push(summary);
      printRound(summary);
    }

    const responseLossReproduced = roundResults.some(
      (result) =>
        result.providerChargeCount === 1 &&
        result.dbPaymentCount === 0 &&
        result.orderStatus === 'PENDING_PAYMENT',
    );

    console.log('\nExperiment Summary');
    console.log(
      JSON.stringify(
        {
          responseLossReproduced,
          directVerification,
          rounds: roundResults,
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
