const DEFAULT_BASE_URL = 'http://localhost:3000';
const DEFAULT_CONCURRENCY = 50;
const DEFAULT_ROUNDS = 5;
const ORDER_QUANTITY = 1;

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

const baseUrl = (process.env.BASE_URL ?? DEFAULT_BASE_URL).replace(/\/$/, '');
const concurrency = parsePositiveInteger(
  process.env.CONCURRENCY,
  DEFAULT_CONCURRENCY,
  'CONCURRENCY',
);
const rounds = parsePositiveInteger(process.env.ROUNDS, DEFAULT_ROUNDS, 'ROUNDS');
const initialStock = concurrency;

const requestJson = async (path, options = {}) => {
  const response = await fetch(`${baseUrl}${path}`, {
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

const postJson = (path, body) =>
  requestJson(path, {
    method: 'POST',
    body: JSON.stringify(body),
  });

const getJson = (path) => requestJson(path);

const createProduct = async (round) => {
  const response = await postJson('/products', {
    name: `M4 Race Test ${Date.now()}-${round}`,
    price: 10000,
    description: 'M4 concurrency experiment',
  });

  if (!response.ok) {
    throw new Error(
      `Failed to create product: ${response.status} ${JSON.stringify(response.body)}`,
    );
  }

  return response.body;
};

const createInventory = async (productId) => {
  const response = await postJson('/inventories', {
    productId,
    quantity: initialStock,
  });

  if (!response.ok) {
    throw new Error(
      `Failed to create inventory: ${response.status} ${JSON.stringify(response.body)}`,
    );
  }

  return response.body;
};

const getInventory = async (productId) => {
  const response = await getJson(`/inventories/products/${productId}`);

  if (!response.ok) {
    throw new Error(
      `Failed to get inventory: ${response.status} ${JSON.stringify(response.body)}`,
    );
  }

  return response.body;
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

const summarizeStatuses = (results) => {
  const buckets = {
    '2xx': 0,
    '400': 0,
    '404': 0,
    '409': 0,
    '5xx': 0,
    'network/error': 0,
    other: 0,
  };

  for (const result of results) {
    buckets[bucketFor(result)] += 1;
  }

  return buckets;
};

const createStartGate = () => {
  let release;
  const promise = new Promise((resolve) => {
    release = resolve;
  });

  return { promise, release };
};

const runWorker = async ({ gate, productId, workerId }) => {
  await gate.promise;
  const startedAt = performance.now();

  try {
    const response = await postJson('/orders', {
      items: [{ productId, quantity: ORDER_QUANTITY }],
    });

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

const runRound = async (round) => {
  const product = await createProduct(round);
  await createInventory(product.id);

  const gate = createStartGate();
  const workers = Array.from({ length: concurrency }, (_, index) =>
    runWorker({ gate, productId: product.id, workerId: index + 1 }),
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

  const statusCounts = summarizeStatuses(results);
  const successfulOrders = statusCounts['2xx'];
  const failedOrders = concurrency - successfulOrders;
  const expectedInventory = initialStock - successfulOrders * ORDER_QUANTITY;
  const inventory = await getInventory(product.id);
  const actualInventory = inventory.quantity;
  const lostUpdates = actualInventory - expectedInventory;
  const sampleFailure = results.find((result) => bucketFor(result) !== '2xx');

  return {
    round,
    productId: product.id,
    initialStock,
    requests: concurrency,
    orderQuantity: ORDER_QUANTITY,
    successfulOrders,
    failedOrders,
    expectedInventory,
    actualInventory,
    lostUpdates,
    elapsedMs,
    statusCounts,
    sampleFailure: sampleFailure
      ? {
          workerId: sampleFailure.workerId,
          status: sampleFailure.status ?? 'network/error',
          body: sampleFailure.body ?? sampleFailure.error,
        }
      : null,
  };
};

const printRound = (result) => {
  console.log(`\nRound ${result.round}`);
  console.log(`Product ID: ${result.productId}`);
  console.log(`Initial inventory: ${result.initialStock}`);
  console.log(`Concurrent requests: ${result.requests}`);
  console.log(`Order quantity: ${result.orderQuantity}`);
  console.log(`Success: ${result.successfulOrders}`);
  console.log(`Failed: ${result.failedOrders}`);
  console.log(`Status counts: ${JSON.stringify(result.statusCounts)}`);
  console.log(`Expected inventory: ${result.expectedInventory}`);
  console.log(`Actual inventory: ${result.actualInventory}`);
  console.log(`Lost updates: ${result.lostUpdates}`);
  console.log(`Elapsed time: ${result.elapsedMs}ms`);

  if (result.sampleFailure) {
    console.log(`Sample failure: ${JSON.stringify(result.sampleFailure)}`);
  }
};

const printSummary = (results) => {
  const total = results.reduce(
    (summary, result) => {
      summary.requests += result.requests;
      summary.successfulOrders += result.successfulOrders;
      summary.failedOrders += result.failedOrders;
      summary.lostUpdates += result.lostUpdates;

      for (const [key, value] of Object.entries(result.statusCounts)) {
        summary.statusCounts[key] += value;
      }

      return summary;
    },
    {
      requests: 0,
      successfulOrders: 0,
      failedOrders: 0,
      lostUpdates: 0,
      statusCounts: {
        '2xx': 0,
        '400': 0,
        '404': 0,
        '409': 0,
        '5xx': 0,
        'network/error': 0,
        other: 0,
      },
    },
  );

  console.log('\nSummary');
  console.log(`Base URL: ${baseUrl}`);
  console.log(`Rounds: ${rounds}`);
  console.log(`Concurrency: ${concurrency}`);
  console.log(`Initial inventory per round: ${initialStock}`);
  console.log(`Order quantity: ${ORDER_QUANTITY}`);
  console.log(`Total requests: ${total.requests}`);
  console.log(`Total successful orders: ${total.successfulOrders}`);
  console.log(`Total failed orders: ${total.failedOrders}`);
  console.log(`Total status counts: ${JSON.stringify(total.statusCounts)}`);
  console.log(`Total lost updates: ${total.lostUpdates}`);
  console.log(
    `Race condition reproduced: ${results.some((result) => result.lostUpdates > 0)}`,
  );
  console.log('\nJSON Summary');
  console.log(JSON.stringify({ config: { baseUrl, concurrency, rounds }, results, total }, null, 2));
};

const main = async () => {
  console.log('Inventory race condition experiment');
  console.log(`Base URL: ${baseUrl}`);
  console.log(`Concurrency: ${concurrency}`);
  console.log(`Rounds: ${rounds}`);
  console.log(`Initial inventory per round: ${initialStock}`);
  console.log(`Order quantity: ${ORDER_QUANTITY}`);

  const results = [];

  for (let round = 1; round <= rounds; round += 1) {
    const result = await runRound(round);
    results.push(result);
    printRound(result);
  }

  printSummary(results);
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
