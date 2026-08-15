import { randomUUID } from 'node:crypto';
import http from 'node:http';

try {
  process.loadEnvFile();
} catch (error) {
  if (error.code !== 'ENOENT') {
    throw error;
  }
}

const port = Number(process.env.FAKE_PAYMENT_PORT ?? 4001);

if (!Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error('FAKE_PAYMENT_PORT must be a valid port number');
}

const readJsonBody = async (request) => {
  const chunks = [];

  for await (const chunk of request) {
    chunks.push(chunk);
  }

  const text = Buffer.concat(chunks).toString('utf8');

  if (!text) {
    return null;
  }

  return JSON.parse(text);
};

const sendJson = (response, statusCode, body) => {
  response.writeHead(statusCode, {
    'content-type': 'application/json',
  });
  response.end(JSON.stringify(body));
};

const charges = [];
const idempotencyResults = new Map();
const faultState = {
  mode: null,
  remaining: 0,
};

const configureFaultMode = (body) => {
  if (body?.mode !== 'DROP_RESPONSE_AFTER_SUCCESS') {
    throw new Error('Unsupported fault mode');
  }

  if (!Number.isInteger(body.count) || body.count < 1) {
    throw new Error('Fault count must be a positive integer');
  }

  faultState.mode = body.mode;
  faultState.remaining = body.count;
};

const shouldDropResponseAfterSuccess = () => {
  if (
    faultState.mode !== 'DROP_RESPONSE_AFTER_SUCCESS' ||
    faultState.remaining < 1
  ) {
    return false;
  }

  faultState.remaining -= 1;

  if (faultState.remaining === 0) {
    faultState.mode = null;
  }

  return true;
};

const server = http.createServer(async (request, response) => {
  const url = new URL(request.url ?? '/', `http://${request.headers.host}`);
  const idempotencyLookupPrefix = '/charges/idempotency/';

  if (
    request.method === 'GET' &&
    url.pathname.startsWith(idempotencyLookupPrefix)
  ) {
    const idempotencyKey = decodeURIComponent(
      url.pathname.slice(idempotencyLookupPrefix.length),
    );
    const existingResult = idempotencyResults.get(idempotencyKey);

    if (!existingResult) {
      sendJson(response, 404, {
        message: 'Charge not found',
      });
      return;
    }

    sendJson(response, 200, existingResult);
    return;
  }

  if (request.method === 'GET' && url.pathname === '/charges') {
    const orderId = url.searchParams.get('orderId');
    const filteredCharges =
      orderId === null
        ? charges
        : charges.filter((charge) => String(charge.orderId) === orderId);

    sendJson(response, 200, {
      charges: filteredCharges,
    });
    return;
  }

  if (request.method === 'POST' && url.pathname === '/reset') {
    charges.length = 0;
    idempotencyResults.clear();
    faultState.mode = null;
    faultState.remaining = 0;

    sendJson(response, 200, {
      reset: true,
    });
    return;
  }

  if (request.method === 'POST' && url.pathname === '/fault-mode') {
    try {
      const body = await readJsonBody(request);
      configureFaultMode(body);

      sendJson(response, 200, {
        mode: faultState.mode,
        remaining: faultState.remaining,
      });
    } catch (error) {
      sendJson(response, 400, {
        message: error instanceof Error ? error.message : 'Invalid fault mode',
      });
    }
    return;
  }

  if (request.method !== 'POST' || url.pathname !== '/charges') {
    sendJson(response, 404, {
      message: 'Not Found',
    });
    return;
  }

  try {
    const body = await readJsonBody(request);
    const idempotencyKey = request.headers['idempotency-key'];

    if (typeof idempotencyKey === 'string') {
      const existingResult = idempotencyResults.get(idempotencyKey);

      if (existingResult) {
        if (
          existingResult.orderId !== body?.orderId ||
          existingResult.amount !== body?.amount
        ) {
          sendJson(response, 409, {
            message: 'Idempotency key payload mismatch',
          });
          return;
        }

        sendJson(response, 200, existingResult);
        return;
      }
    }

    const transactionId = `fake_${Date.now()}_${randomUUID()}`;
    const charge = {
      transactionId,
      orderId: body?.orderId,
      amount: body?.amount,
      chargedAt: new Date().toISOString(),
    };
    const result = {
      transactionId,
      status: 'SUCCESS',
      orderId: charge.orderId,
      amount: charge.amount,
    };

    charges.push(charge);

    if (typeof idempotencyKey === 'string') {
      idempotencyResults.set(idempotencyKey, result);
    }

    if (shouldDropResponseAfterSuccess()) {
      response.destroy();
      return;
    }

    sendJson(response, 200, result);
  } catch {
    sendJson(response, 400, {
      message: 'Invalid JSON',
    });
  }
});

server.listen(port, () => {
  console.log(`Fake payment provider listening on ${port}`);
});

const shutdown = () => {
  server.close(() => {
    process.exit(0);
  });
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
