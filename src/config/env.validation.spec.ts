import 'reflect-metadata';
import { validate } from './env.validation';

describe('environment validation', () => {
  const required = {
    DB_HOST: 'localhost',
    DB_PORT: '3307',
    DB_USERNAME: 'test',
    DB_PASSWORD: 'test',
    DB_DATABASE: 'test',
    PORT: '3000',
    RABBITMQ_URL: 'amqp://localhost',
  };

  it('allows existing environments without cache settings', () => {
    expect(() => validate(required)).not.toThrow();
  });

  it('accepts and converts publisher-confirm configuration', () => {
    const result = validate({
      ...required,
      RABBITMQ_PUBLISH_CONFIRM_TIMEOUT_MS: '3000',
      OUTBOX_MARK_PUBLISHED_FAIL_COUNT: '0',
    });
    expect(result.RABBITMQ_PUBLISH_CONFIRM_TIMEOUT_MS).toBe(3000);
    expect(result.OUTBOX_MARK_PUBLISHED_FAIL_COUNT).toBe(0);
  });

  it.each(['0', '-1', '1.5', 'invalid'])(
    'rejects confirm timeout %s',
    (value) => {
      expect(() =>
        validate({ ...required, RABBITMQ_PUBLISH_CONFIRM_TIMEOUT_MS: value }),
      ).toThrow();
    },
  );

  it.each(['-1', '1.5', 'invalid'])(
    'rejects post-confirm failure count %s',
    (value) => {
      expect(() =>
        validate({ ...required, OUTBOX_MARK_PUBLISHED_FAIL_COUNT: value }),
      ).toThrow();
    },
  );

  it.each(['true', 'false'])(
    'accepts cache enabled=%s and converts TTL to an integer',
    (enabled) => {
      const result = validate({
        ...required,
        PRODUCT_CURSOR_CACHE_ENABLED: enabled,
        PRODUCT_CURSOR_CACHE_TTL_SECONDS: '60',
        REDIS_URL: 'redis://localhost:6379',
      });
      expect(result.PRODUCT_CURSOR_CACHE_ENABLED).toBe(enabled);
      expect(result.PRODUCT_CURSOR_CACHE_TTL_SECONDS).toBe(60);
    },
  );

  it('rejects invalid enabled values', () => {
    expect(() =>
      validate({ ...required, PRODUCT_CURSOR_CACHE_ENABLED: 'yes' }),
    ).toThrow();
  });

  it.each(['0', '-1', '1.5', 'invalid'])('rejects TTL %s', (ttl) => {
    expect(() =>
      validate({ ...required, PRODUCT_CURSOR_CACHE_TTL_SECONDS: ttl }),
    ).toThrow();
  });
});
