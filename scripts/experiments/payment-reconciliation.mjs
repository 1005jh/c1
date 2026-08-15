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

const providerPostJson = (path, body) =>
  requestJson(paymentProviderBaseUrl, path, {
    method: 'POST',
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
    name: `M11 Reconciliation ${Date.now()}-${round}`,
    price: PRODUCT_PRICE,
    description: 'M11 payment reconciliation experiment',
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

const snapshot = async (connection, orderId) => {
  const providerCharges = await getProviderCharges(orderId);
  const dbPayments = await readPayments(connection, orderId);
  const order = await readOrder(connection, orderId);

  return {
    providerChargeCount: providerCharges.length,
    providerTransactionIds: providerCharges.map(
      (charge) => charge.transactionId,
    ),
    dbPaymentCount: dbPayments.length,
    dbPaymentStatus: dbPayments[0]?.status ?? null,
    dbProviderTransactionIds: dbPayments.map(
      (payment) => payment.providerTransactionId,
    ),
    dbProviderTransactionId: dbPayments[0]?.providerTransactionId ?? null,
    orderStatus: order?.status ?? null,
    providerDbMismatch: providerCharges.length - dbPayments.length,
  };
};

const compactResponse = (response) => ({
  type: response.type,
  status: response.status,
  body: response.body,
  error: response.error ?? null,
});

const runRound = async (round, connection) => {
  await resetProvider();

  const product = await createProduct(round);
  await createInventory(product.id);
  const order = await createOrder(product.id);

  await enableDropResponseFault();

  const paymentResponse = await appPostJson(`/orders/${order.id}/payments`);
  const afterLoss = await snapshot(connection, order.id);

  const paymentRetryResponse = await appPostJson(
    `/orders/${order.id}/payments`,
  );
  const afterPaymentRetry = await snapshot(connection, order.id);

  const reconcileResponse = await appPostJson(
    `/orders/${order.id}/payments/reconcile`,
  );
  const afterReconcile = await snapshot(connection, order.id);

  const providerTransactionId =
    afterReconcile.providerTransactionIds[0] ?? null;
  const dbProviderTransactionId = afterReconcile.dbProviderTransactionId;

  return {
    round,
    orderId: order.id,
    orderAmount: order.totalAmount,
    paymentResponse: compactResponse(paymentResponse),
    afterLoss,
    paymentRetryResponse: compactResponse(paymentRetryResponse),
    afterPaymentRetry,
    reconcileResponse: compactResponse(reconcileResponse),
    afterReconcile,
    transactionIdMatches: providerTransactionId === dbProviderTransactionId,
    paymentRetryAddedCharge:
      afterPaymentRetry.providerChargeCount > afterLoss.providerChargeCount,
    paymentRetryAddedPayment:
      afterPaymentRetry.dbPaymentCount > afterLoss.dbPaymentCount,
  };
};

const printRound = (summary) => {
  console.log(`\nRound ${summary.round}`);
  console.log(`  Order ID: ${summary.orderId}`);
  console.log(`  Order Amount: ${summary.orderAmount}`);
  console.log(
    `  Payment API: ${summary.paymentResponse.status} ${JSON.stringify(
      summary.paymentResponse.body,
    )}`,
  );
  console.log(
    `  After Loss: ${JSON.stringify({
      providerChargeCount: summary.afterLoss.providerChargeCount,
      dbPaymentCount: summary.afterLoss.dbPaymentCount,
      dbPaymentStatus: summary.afterLoss.dbPaymentStatus,
      dbProviderTransactionId: summary.afterLoss.dbProviderTransactionId,
      orderStatus: summary.afterLoss.orderStatus,
      providerDbMismatch: summary.afterLoss.providerDbMismatch,
    })}`,
  );
  console.log(
    `  Payment Retry: ${summary.paymentRetryResponse.status} ${JSON.stringify(
      summary.paymentRetryResponse.body,
    )}`,
  );
  console.log(
    `  Payment Retry Added Charge: ${summary.paymentRetryAddedCharge}`,
  );
  console.log(
    `  Payment Retry Added Payment: ${summary.paymentRetryAddedPayment}`,
  );
  console.log(
    `  Reconcile API: ${summary.reconcileResponse.status} ${JSON.stringify(
      summary.reconcileResponse.body,
    )}`,
  );
  console.log(
    `  After Reconcile: ${JSON.stringify({
      providerChargeCount: summary.afterReconcile.providerChargeCount,
      providerTransactionIds: summary.afterReconcile.providerTransactionIds,
      dbPaymentCount: summary.afterReconcile.dbPaymentCount,
      dbPaymentStatus: summary.afterReconcile.dbPaymentStatus,
      dbProviderTransactionId: summary.afterReconcile.dbProviderTransactionId,
      orderStatus: summary.afterReconcile.orderStatus,
      providerDbMismatch: summary.afterReconcile.providerDbMismatch,
      transactionIdMatches: summary.transactionIdMatches,
    })}`,
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

    console.log('Payment Reconciliation Experiment');
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

    for (let round = 1; round <= rounds; round += 1) {
      const summary = await runRound(round, connection);
      roundResults.push(summary);
      printRound(summary);
    }

    console.log('\nExperiment Summary');
    console.log(
      JSON.stringify(
        {
          responseLossRecordedAsUnknown: roundResults.every(
            (result) =>
              result.afterLoss.providerChargeCount === 1 &&
              result.afterLoss.dbPaymentCount === 1 &&
              result.afterLoss.dbPaymentStatus === 'UNKNOWN' &&
              result.afterLoss.dbProviderTransactionId === null &&
              result.afterLoss.orderStatus === 'PENDING_PAYMENT',
          ),
          reconciliationSucceeded: roundResults.every(
            (result) =>
              result.afterReconcile.providerChargeCount === 1 &&
              result.afterReconcile.dbPaymentCount === 1 &&
              result.afterReconcile.dbPaymentStatus === 'SUCCESS' &&
              result.afterReconcile.orderStatus === 'PAID' &&
              result.transactionIdMatches,
          ),
          paymentRetrySafe: roundResults.every(
            (result) =>
              !result.paymentRetryAddedCharge &&
              !result.paymentRetryAddedPayment,
          ),
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
