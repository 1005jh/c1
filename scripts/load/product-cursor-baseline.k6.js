import { check } from 'k6';
import http from 'k6/http';

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

const baseUrl = (__ENV.BASE_URL || 'http://localhost:3000').replace(/\/$/, '');
const targetRate = parsePositiveInteger(__ENV.RATE, 100, 'RATE');
const duration = __ENV.DURATION || '30s';
const preAllocatedVUs = parsePositiveInteger(
  __ENV.PRE_ALLOCATED_VUS,
  100,
  'PRE_ALLOCATED_VUS',
);
const maxVUs = parsePositiveInteger(__ENV.MAX_VUS, 600, 'MAX_VUS');
const limit = parsePositiveInteger(__ENV.LIMIT, 20, 'LIMIT');
const summaryPath = __ENV.SUMMARY_PATH;
const endpoint = '/products/cursor';

export const options = {
  scenarios: {
    product_cursor: {
      executor: 'constant-arrival-rate',
      rate: targetRate,
      timeUnit: '1s',
      duration,
      preAllocatedVUs,
      maxVUs,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    checks: ['rate>0.99'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

const idsAreDescending = (items) =>
  items.every((item, index) => index === 0 || items[index - 1].id > item.id);

export default function () {
  const response = http.get(`${baseUrl}${endpoint}?limit=${limit}`, {
    tags: {
      endpoint: 'product_cursor_first_page',
    },
  });
  let body = null;
  let jsonParsed = false;

  try {
    body = response.json();
    jsonParsed = true;
  } catch {
    body = null;
  }

  const items = Array.isArray(body && body.items) ? body.items : [];
  const lastItem = items.length > 0 ? items[items.length - 1] : null;

  check(response, {
    'status is 200': (res) => res.status === 200,
    'json parse succeeds': () => jsonParsed,
    'items is array': () => Array.isArray(body && body.items),
    'items length equals limit': () => items.length === limit,
    'body limit equals limit': () => body && body.limit === limit,
    'hasNext is true': () => body && body.hasNext === true,
    'nextCursor is number': () => body && typeof body.nextCursor === 'number',
    'items id desc': () => idsAreDescending(items),
    'nextCursor equals last item id': () =>
      body && lastItem && body.nextCursor === lastItem.id,
  });
}

const metricValue = (data, metricName, valueName) =>
  data.metrics[metricName] && data.metrics[metricName].values
    ? data.metrics[metricName].values[valueName]
    : undefined;

const pickPresent = (value) => (value === undefined ? null : value);

const buildCompactSummary = (data) => ({
  targetRate,
  duration,
  preAllocatedVUs,
  maxVUs,
  endpoint,
  limit,
  httpRequests: pickPresent(metricValue(data, 'http_reqs', 'count')),
  requestRate: pickPresent(metricValue(data, 'http_reqs', 'rate')),
  iterations: pickPresent(metricValue(data, 'iterations', 'count')),
  iterationRate: pickPresent(metricValue(data, 'iterations', 'rate')),
  durationAvg: pickPresent(metricValue(data, 'http_req_duration', 'avg')),
  durationMin: pickPresent(metricValue(data, 'http_req_duration', 'min')),
  durationMedian: pickPresent(metricValue(data, 'http_req_duration', 'med')),
  durationP90: pickPresent(metricValue(data, 'http_req_duration', 'p(90)')),
  durationP95: pickPresent(metricValue(data, 'http_req_duration', 'p(95)')),
  durationP99: pickPresent(metricValue(data, 'http_req_duration', 'p(99)')),
  durationMax: pickPresent(metricValue(data, 'http_req_duration', 'max')),
  httpFailureRate: pickPresent(metricValue(data, 'http_req_failed', 'rate')),
  checksRate: pickPresent(metricValue(data, 'checks', 'rate')),
  droppedIterations: pickPresent(
    metricValue(data, 'dropped_iterations', 'count'),
  ),
});

export function handleSummary(data) {
  const compactSummary = buildCompactSummary(data);
  const output = {
    stdout: `${JSON.stringify({ compactSummary }, null, 2)}\n`,
  };

  if (summaryPath) {
    output[summaryPath] = `${JSON.stringify(data, null, 2)}\n`;
  }

  return output;
}
