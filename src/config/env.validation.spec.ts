import 'reflect-metadata';
import { validate } from './env.validation';

describe('cache environment validation', () => {
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
