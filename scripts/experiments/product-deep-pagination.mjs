import mysql from 'mysql2/promise';

try {
  process.loadEnvFile();
} catch (error) {
  if (error.code !== 'ENOENT') {
    throw error;
  }
}

const DEFAULT_BASE_URL = 'http://localhost:3000';
const DEFAULT_SEED_COUNT = 200000;
const DEFAULT_INSERT_BATCH_SIZE = 1000;
const DEFAULT_LIMIT = 20;
const DEFAULT_DEEP_OFFSET = 180000;
const DEFAULT_WARMUP_RUNS = 5;
const DEFAULT_MEASURED_RUNS = 30;
const DEFAULT_HTTP_WARMUP_RUNS = 2;
const DEFAULT_HTTP_MEASURED_RUNS = 10;
const PRODUCT_DESCRIPTION = 'M19 deep pagination benchmark';
const SELECT_COLUMNS = 'id, name, price, description, createdAt, updatedAt';
const OFFSET_SQL = `
  SELECT
    ${SELECT_COLUMNS}
  FROM products
  ORDER BY id DESC
  LIMIT ?
  OFFSET ?
`;
const CURSOR_SQL = `
  SELECT
    ${SELECT_COLUMNS}
  FROM products
  WHERE id < ?
  ORDER BY id DESC
  LIMIT ?
`;

const requiredEnv = (name) => {
  const value = process.env[name];

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
};

const parseInteger = (value, fallback, name, { min, max }) => {
  if (value === undefined || value === '') {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    throw new Error(`${name} must be an integer between ${min} and ${max}`);
  }

  return parsed;
};

const baseUrl = (process.env.BASE_URL ?? DEFAULT_BASE_URL).replace(/\/$/, '');
const seedCount = parseInteger(
  process.env.SEED_COUNT,
  DEFAULT_SEED_COUNT,
  'SEED_COUNT',
  { min: 0, max: 10_000_000 },
);
const insertBatchSize = parseInteger(
  process.env.INSERT_BATCH_SIZE,
  DEFAULT_INSERT_BATCH_SIZE,
  'INSERT_BATCH_SIZE',
  { min: 1, max: 10000 },
);
const limit = parseInteger(process.env.LIMIT, DEFAULT_LIMIT, 'LIMIT', {
  min: 1,
  max: 100,
});
const deepOffset = parseInteger(
  process.env.DEEP_OFFSET,
  DEFAULT_DEEP_OFFSET,
  'DEEP_OFFSET',
  { min: 1, max: 10_000_000 },
);
const warmupRuns = parseInteger(
  process.env.WARMUP_RUNS,
  DEFAULT_WARMUP_RUNS,
  'WARMUP_RUNS',
  { min: 0, max: 1000 },
);
const measuredRuns = parseInteger(
  process.env.MEASURED_RUNS,
  DEFAULT_MEASURED_RUNS,
  'MEASURED_RUNS',
  { min: 1, max: 1000 },
);
const httpWarmupRuns = parseInteger(
  process.env.HTTP_WARMUP_RUNS,
  DEFAULT_HTTP_WARMUP_RUNS,
  'HTTP_WARMUP_RUNS',
  { min: 0, max: 1000 },
);
const httpMeasuredRuns = parseInteger(
  process.env.HTTP_MEASURED_RUNS,
  DEFAULT_HTTP_MEASURED_RUNS,
  'HTTP_MEASURED_RUNS',
  { min: 1, max: 1000 },
);
const cleanupAfter = process.env.CLEANUP_AFTER === '1';

if (deepOffset % limit !== 0) {
  throw new Error('DEEP_OFFSET must be a multiple of LIMIT for API comparison');
}

const page = deepOffset / limit + 1;

const countProducts = async (connection) => {
  const [rows] = await connection.execute(
    'SELECT COUNT(*) AS count FROM products',
  );

  return Number(rows[0]?.count ?? 0);
};

const seedBenchmarkProducts = async (connection) => {
  let inserted = 0;
  let firstInsertedId = null;
  let lastInsertedId = null;

  while (inserted < seedCount) {
    const batchSize = Math.min(insertBatchSize, seedCount - inserted);
    const now = new Date();
    const values = Array.from({ length: batchSize }, (_, index) => {
      const sequence = inserted + index + 1;

      return [
        `M19 Benchmark Product ${sequence}`,
        1000 + (sequence % 100000),
        PRODUCT_DESCRIPTION,
        now,
        now,
      ];
    });

    const [result] = await connection.query(
      `
        INSERT INTO products
          (name, price, description, createdAt, updatedAt)
        VALUES ?
      `,
      [values],
    );

    if (firstInsertedId === null) {
      firstInsertedId = result.insertId;
    }

    lastInsertedId = result.insertId + result.affectedRows - 1;
    inserted += result.affectedRows;
  }

  return {
    inserted,
    firstInsertedId,
    lastInsertedId,
  };
};

const cleanupBenchmarkProducts = async (connection, insertedRange) => {
  if (
    insertedRange.inserted === 0 ||
    insertedRange.firstInsertedId === null ||
    insertedRange.lastInsertedId === null
  ) {
    return 0;
  }

  const [result] = await connection.execute(
    `
      DELETE FROM products
      WHERE id BETWEEN ? AND ?
        AND description = ?
    `,
    [
      insertedRange.firstInsertedId,
      insertedRange.lastInsertedId,
      PRODUCT_DESCRIPTION,
    ],
  );

  return result.affectedRows;
};

const queryRows = async (connection, sql, params) => {
  const [rows] = await connection.query(sql, params);

  return rows;
};

const idsFrom = (rows) => rows.map((row) => row.id);

const arraysEqual = (left, right) =>
  left.length === right.length &&
  left.every((value, index) => value === right[index]);

const readPreviousCursorId = async (connection) => {
  const rows = await queryRows(
    connection,
    `
      SELECT id
      FROM products
      ORDER BY id DESC
      LIMIT 1
      OFFSET ?
    `,
    [deepOffset - 1],
  );

  if (!rows[0]) {
    throw new Error(`Cannot find previous cursor at offset ${deepOffset - 1}`);
  }

  return rows[0].id;
};

const rawExplainTextFrom = (rows) =>
  rows
    .map((row) =>
      Object.values(row)
        .map((value) =>
          typeof value === 'string' ? value : JSON.stringify(value),
        )
        .join('\n'),
    )
    .join('\n');

const interpolateNumericSql = (sql, params) => {
  let index = 0;

  return sql.replaceAll('?', () => {
    const value = params[index];
    index += 1;

    if (!Number.isFinite(value)) {
      throw new Error('Only numeric EXPLAIN parameters are supported');
    }

    return String(value);
  });
};

const explainQuery = async (connection, label, sql, params) => {
  const explainAnalyzeSql = `EXPLAIN ANALYZE ${interpolateNumericSql(
    sql,
    params,
  )}`;

  try {
    const rows = await queryRows(connection, explainAnalyzeSql, []);
    const raw = rawExplainTextFrom(rows);

    return {
      label,
      kind: 'EXPLAIN ANALYZE',
      sql: explainAnalyzeSql,
      raw,
      highlights: explainHighlightsFrom(raw),
    };
  } catch (error) {
    const fallbackSql = `EXPLAIN FORMAT=JSON ${interpolateNumericSql(
      sql,
      params,
    )}`;
    const rows = await queryRows(connection, fallbackSql, []);
    const raw = rawExplainTextFrom(rows);

    return {
      label,
      kind: 'EXPLAIN FORMAT=JSON',
      fallbackReason: error instanceof Error ? error.message : String(error),
      sql: fallbackSql,
      raw,
      highlights: explainHighlightsFrom(raw),
    };
  }
};

const explainHighlightsFrom = (raw) => {
  const actualMatches = Array.from(
    raw.matchAll(
      /actual time=([0-9.]+)\.\.([0-9.]+) rows=([0-9]+) loops=([0-9]+)/g,
    ),
  ).map((match) => ({
    actualTimeStartMs: Number(match[1]),
    actualTimeEndMs: Number(match[2]),
    actualRows: Number(match[3]),
    loops: Number(match[4]),
  }));
  const usesPrimary = raw.includes('using PRIMARY');
  const access =
    raw.match(/Index range scan[^(\n]*/)?.[0]?.trim() ??
    raw.match(/Index scan[^(\n]*/)?.[0]?.trim() ??
    raw.match(/Table scan[^(\n]*/)?.[0]?.trim() ??
    'unknown';

  return {
    access,
    keyOrIndex: usesPrimary ? 'PRIMARY' : 'unknown',
    maxActualRows:
      actualMatches.length > 0
        ? Math.max(...actualMatches.map((match) => match.actualRows))
        : null,
    maxActualTimeEndMs:
      actualMatches.length > 0
        ? Math.max(...actualMatches.map((match) => match.actualTimeEndMs))
        : null,
    actualNodes: actualMatches,
  };
};

const timeQuery = async (connection, sql, params) => {
  const startedAt = performance.now();
  const rows = await queryRows(connection, sql, params);

  return {
    durationMs: performance.now() - startedAt,
    rowCount: rows.length,
  };
};

const percentile = (values, ratio) => {
  const sorted = [...values].sort((left, right) => left - right);
  const index = Math.ceil(sorted.length * ratio) - 1;

  return sorted[Math.max(0, Math.min(sorted.length - 1, index))];
};

const roundMetric = (value) => Number(value.toFixed(3));

const timingStatsFrom = (values) => {
  const sorted = [...values].sort((left, right) => left - right);
  const sum = sorted.reduce((total, value) => total + value, 0);
  const middle = Math.floor(sorted.length / 2);
  const median =
    sorted.length % 2 === 0
      ? (sorted[middle - 1] + sorted[middle]) / 2
      : sorted[middle];

  return {
    count: sorted.length,
    minMs: roundMetric(sorted[0]),
    avgMs: roundMetric(sum / sorted.length),
    medianMs: roundMetric(median),
    p95Ms: roundMetric(percentile(sorted, 0.95)),
    maxMs: roundMetric(sorted[sorted.length - 1]),
  };
};

const measureRawQueries = async (
  connection,
  previousCursorId,
  offsetRows,
  cursorRows,
) => {
  const offsetDurations = [];
  const cursorDurations = [];

  for (let run = 0; run < warmupRuns; run += 1) {
    if (run % 2 === 0) {
      await timeQuery(connection, OFFSET_SQL, [limit, deepOffset]);
      await timeQuery(connection, CURSOR_SQL, [previousCursorId, limit]);
    } else {
      await timeQuery(connection, CURSOR_SQL, [previousCursorId, limit]);
      await timeQuery(connection, OFFSET_SQL, [limit, deepOffset]);
    }
  }

  for (let run = 0; run < measuredRuns; run += 1) {
    if (run % 2 === 0) {
      offsetDurations.push(
        (await timeQuery(connection, OFFSET_SQL, [limit, deepOffset]))
          .durationMs,
      );
      cursorDurations.push(
        (await timeQuery(connection, CURSOR_SQL, [previousCursorId, limit]))
          .durationMs,
      );
    } else {
      cursorDurations.push(
        (await timeQuery(connection, CURSOR_SQL, [previousCursorId, limit]))
          .durationMs,
      );
      offsetDurations.push(
        (await timeQuery(connection, OFFSET_SQL, [limit, deepOffset]))
          .durationMs,
      );
    }
  }

  return {
    warmupRuns,
    measuredRuns,
    offset: {
      rowCount: offsetRows.length,
      stats: timingStatsFrom(offsetDurations),
    },
    cursor: {
      rowCount: cursorRows.length,
      stats: timingStatsFrom(cursorDurations),
    },
  };
};

const requestJson = async (path) => {
  const startedAt = performance.now();
  const response = await fetch(`${baseUrl}${path}`);
  const elapsedMs = performance.now() - startedAt;
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
    elapsedMs,
    body,
  };
};

const assertOk = (label, response) => {
  if (!response.ok) {
    throw new Error(
      `${label} failed: ${response.status} ${JSON.stringify(response.body)}`,
    );
  }
};

const apiIdsFrom = (response) => {
  assertOk('API request', response);

  if (!Array.isArray(response.body?.items)) {
    throw new Error('API response does not contain items array');
  }

  return response.body.items.map((item) => item.id);
};

const measureHttpQueries = async (offsetPath, cursorPath) => {
  const offsetDurations = [];
  const cursorDurations = [];

  for (let run = 0; run < httpWarmupRuns; run += 1) {
    if (run % 2 === 0) {
      await requestJson(offsetPath);
      await requestJson(cursorPath);
    } else {
      await requestJson(cursorPath);
      await requestJson(offsetPath);
    }
  }

  for (let run = 0; run < httpMeasuredRuns; run += 1) {
    if (run % 2 === 0) {
      offsetDurations.push((await requestJson(offsetPath)).elapsedMs);
      cursorDurations.push((await requestJson(cursorPath)).elapsedMs);
    } else {
      cursorDurations.push((await requestJson(cursorPath)).elapsedMs);
      offsetDurations.push((await requestJson(offsetPath)).elapsedMs);
    }
  }

  return {
    httpWarmupRuns,
    httpMeasuredRuns,
    offset: timingStatsFrom(offsetDurations),
    cursor: timingStatsFrom(cursorDurations),
  };
};

const runHttpVerification = async (previousCursorId) => {
  const offsetPath = `/products?page=${page}&limit=${limit}`;
  const cursorPath = `/products/cursor?cursorId=${previousCursorId}&limit=${limit}`;
  const firstOffsetPath = `/products?page=1&limit=${limit}`;
  const firstCursorPath = `/products/cursor?limit=${limit}`;

  const [
    offsetResponse,
    cursorResponse,
    firstOffsetResponse,
    firstCursorResponse,
  ] = await Promise.all([
    requestJson(offsetPath),
    requestJson(cursorPath),
    requestJson(firstOffsetPath),
    requestJson(firstCursorPath),
  ]);
  const offsetIds = apiIdsFrom(offsetResponse);
  const cursorIds = apiIdsFrom(cursorResponse);
  const firstOffsetIds = apiIdsFrom(firstOffsetResponse);
  const firstCursorIds = apiIdsFrom(firstCursorResponse);
  const timings = await measureHttpQueries(offsetPath, cursorPath);

  return {
    offsetPath,
    cursorPath,
    firstOffsetPath,
    firstCursorPath,
    offsetStatus: offsetResponse.status,
    cursorStatus: cursorResponse.status,
    firstOffsetStatus: firstOffsetResponse.status,
    firstCursorStatus: firstCursorResponse.status,
    offsetIds,
    cursorIds,
    sameApiResultIds: arraysEqual(offsetIds, cursorIds),
    firstOffsetIds,
    firstCursorIds,
    firstPageIdsMatch: arraysEqual(firstOffsetIds, firstCursorIds),
    timing: timings,
    semanticDifference:
      'OFFSET endpoint runs item query plus COUNT via findAndCount; cursor endpoint runs item query only and omits total.',
  };
};

const printSection = (title, value) => {
  console.log(`\n${title}`);
  console.log(JSON.stringify(value, null, 2));
};

const main = async () => {
  const connection = await mysql.createConnection({
    host: requiredEnv('DB_HOST'),
    port: Number(requiredEnv('DB_PORT')),
    user: requiredEnv('DB_USERNAME'),
    password: requiredEnv('DB_PASSWORD'),
    database: requiredEnv('DB_DATABASE'),
  });

  let insertedRange = {
    inserted: 0,
    firstInsertedId: null,
    lastInsertedId: null,
  };

  try {
    console.log('Product Deep Pagination Experiment');
    printSection('Config', {
      baseUrl,
      seedCount,
      insertBatchSize,
      limit,
      deepOffset,
      page,
      warmupRuns,
      measuredRuns,
      httpWarmupRuns,
      httpMeasuredRuns,
      cleanupAfter,
    });

    const existingProductCount = await countProducts(connection);
    insertedRange = await seedBenchmarkProducts(connection);
    const countAfterSeed = await countProducts(connection);

    if (countAfterSeed < deepOffset + limit) {
      throw new Error(
        `Not enough products for deep offset: count=${countAfterSeed}, required=${deepOffset + limit}`,
      );
    }

    const previousCursorId = await readPreviousCursorId(connection);
    const offsetRows = await queryRows(connection, OFFSET_SQL, [
      limit,
      deepOffset,
    ]);
    const cursorRows = await queryRows(connection, CURSOR_SQL, [
      previousCursorId,
      limit,
    ]);
    const offsetIds = idsFrom(offsetRows);
    const cursorIds = idsFrom(cursorRows);
    const sameResultIds = arraysEqual(offsetIds, cursorIds);
    const [offsetExplain, cursorExplain] = await Promise.all([
      explainQuery(connection, 'OFFSET', OFFSET_SQL, [limit, deepOffset]),
      explainQuery(connection, 'CURSOR', CURSOR_SQL, [previousCursorId, limit]),
    ]);
    const queryTiming = await measureRawQueries(
      connection,
      previousCursorId,
      offsetRows,
      cursorRows,
    );
    const http = await runHttpVerification(previousCursorId);

    const finalProductCountBeforeCleanup = await countProducts(connection);
    let cleanupDeletedCount = 0;

    if (cleanupAfter) {
      cleanupDeletedCount = await cleanupBenchmarkProducts(
        connection,
        insertedRange,
      );
    }

    const finalProductCount = await countProducts(connection);
    const summary = {
      dataset: {
        existingProductCount,
        insertedBenchmarkCount: insertedRange.inserted,
        insertedIdRange: {
          first: insertedRange.firstInsertedId,
          last: insertedRange.lastInsertedId,
        },
        countAfterSeed,
        cleanupAfter,
        cleanupDeletedCount,
        finalProductCountBeforeCleanup,
        finalProductCount,
      },
      query: {
        deepOffset,
        limit,
        page,
        previousCursorId,
        offsetSql: OFFSET_SQL.trim(),
        cursorSql: CURSOR_SQL.trim(),
      },
      correctness: {
        rawSql: {
          offsetIds,
          cursorIds,
          sameResultIds,
        },
        api: {
          sameApiResultIds: http.sameApiResultIds,
          firstPageIdsMatch: http.firstPageIdsMatch,
        },
      },
      explainAnalyze: {
        offset: offsetExplain,
        cursor: cursorExplain,
      },
      timing: {
        rawSql: queryTiming,
        http,
        observedMedianDifferenceMs: roundMetric(
          queryTiming.offset.stats.medianMs - queryTiming.cursor.stats.medianMs,
        ),
        observedP95DifferenceMs: roundMetric(
          queryTiming.offset.stats.p95Ms - queryTiming.cursor.stats.p95Ms,
        ),
      },
    };

    printSection('Summary', summary);

    if (!sameResultIds) {
      throw new Error('Raw SQL offset and cursor ids differ');
    }

    if (!http.sameApiResultIds) {
      throw new Error('Offset API and cursor API ids differ');
    }

    if (!http.firstPageIdsMatch) {
      throw new Error('Offset first page and cursor first page ids differ');
    }
  } finally {
    await connection.end();
  }
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
