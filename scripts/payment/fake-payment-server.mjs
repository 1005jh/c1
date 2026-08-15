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

const server = http.createServer(async (request, response) => {
  if (request.method !== 'POST' || request.url !== '/charges') {
    sendJson(response, 404, {
      message: 'Not Found',
    });
    return;
  }

  try {
    const body = await readJsonBody(request);

    sendJson(response, 200, {
      transactionId: `fake_${Date.now()}_${randomUUID()}`,
      status: 'SUCCESS',
      orderId: body?.orderId,
      amount: body?.amount,
    });
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
