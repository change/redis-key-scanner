const _ = require('lodash');
const { expect } = require('chai'); // eslint-disable-line import/no-extraneous-dependencies
const Redis = require('ioredis');
const RedisKeyScanner = require('.');

const defaultArgs = {
  host: 'localhost',
  port: 6379,
};
const prefix = 'redis-key-scanner:test:';

const defaultRedis = () => new Redis(_.pick(defaultArgs, ['host', 'port']));

const deleteTestKeys = () => {
  const redis = defaultRedis();
  return redis.keys(`${prefix}*`).then((result) => {
    const deletePipeline = redis.pipeline();
    _.each(result, key => deletePipeline.del(key));
    return deletePipeline.exec();
  });
};

const extendArgs = o => (
  () => {
    const opts = { ...defaultArgs, ...o };
    if (opts.pattern) { opts.pattern = `${prefix}${opts.pattern}`; }
    return new RedisKeyScanner(opts);
  }
);

// From a result set of selected keys, return a sorted, comma-separated list of just the key names
// (with the prefix removed).
const matchedKeys = selected => String(_.map(
  selected,
  item => item.key.substr(prefix.length)
).sort());

const omitArgs = keys => (() => new RedisKeyScanner(_.omit(defaultArgs, keys)));

// Perform a scan, and return only the selected keys via the callback
const scan = options => new Promise((resolve, reject) => { // eslint-disable-line promise/avoid-new
  const results = [];
  const scanner = extendArgs(options)();
  scanner.on('data', data => data.key && results.push(data));
  scanner.on('end', () => resolve(results));
  scanner.on('error', err => reject(err));
});

function setTestKeys(keys) {
  const pipeline = defaultRedis().pipeline();
  _.each(keys, (ttl, key) => {
    let args = [`${prefix}${key}`, key];
    if (ttl > -1) { args = args.concat(['ex', ttl]); }
    pipeline.set(...args);
  });
  return pipeline.exec();
}

describe('class RedisKeyScanner', () => {
  describe('instantiation throws a TypeError when', () => {
    it('no arguments', () => expect(() => new RedisKeyScanner()).to.throw(TypeError));

    it('no host', () => expect(omitArgs('host')).to.throw(TypeError));

    it('no port', () => expect(omitArgs('port')).to.throw(TypeError));

    it('unrecognized argument', () => expect(extendArgs({ foo: 1 })).to.throw(TypeError));

    describe('an invalid timeframe value is provided;', () => {
      it('`w` for max-idle', () => expect(extendArgs({ 'max-idle': 'w' })).to.throw(TypeError));

      it('`foo` for max-ttl', () => expect(extendArgs({ 'max-ttl': 'foo' })).to.throw(TypeError));

      it('`true` for min-idle', () => expect(extendArgs({ 'min-idle': true })).to.throw(TypeError));

      it('`null` for min-ttl', () => expect(extendArgs({ 'min-ttl': null })).to.throw(TypeError));
    });
  });

  describe('when redis not running on the specified port', () => {
    it('emits an error event', (done) => {
      extendArgs({ port: 80 })().on('error', () => done());
    });
  });

  describe('when redis connection succeeds', () => {
    const assortedExpiryKeys = {
      noExpiry1: -1, noExpiry2: -1, expires10: 10, expires100: 100,
    };
    const fourKeys = {
      one: -1, two: -1, three: -1, four: -1,
    };

    before(deleteTestKeys);

    describe('basic key selection', () => {
      describe('when no keys exist', () => {
        it('a scan for `*` returns no results', () => scan({ pattern: '*' }).then((result) => {
          expect(result).to.have.length(0);
          return result;
        }));
      });

      describe('when exactly one key exists', () => {
        before(() => setTestKeys({ foo: -1 }));

        it('a scan for `*` selects that key (only)', () => scan({ pattern: '*' }).then((result) => {
          expect(matchedKeys(result)).to.equal('foo');
          return result;
        }));
      });
    });

    describe('pattern matching', () => {
      beforeEach(() => setTestKeys(fourKeys));

      it('pattern `*` results in all keys', () => scan({ pattern: '*' }).then((result) => {
        expect(matchedKeys(result)).to.equal('four,one,three,two');
        return result;
      }));

      it('prefix wildcard works', () => scan({ pattern: '*e' }).then((result) => {
        expect(matchedKeys(result)).to.equal('one,three');
        return result;
      }));

      it('postfix wildcard works', () => scan({ pattern: 't*' }).then((result) => {
        expect(matchedKeys(result)).to.equal('three,two');
        return result;
      }));
    });

    describe('limits', () => {
      beforeEach(() => setTestKeys(fourKeys));

      it(
        'limit gets honored as an upper bound on results',
        () => scan({ pattern: '*', limit: 3 }).then((result) => {
          expect(result).to.have.length(3);
          return result;
        })
      );

      it(
        'even when limit is greater than scan-limit',
        () => scan({ pattern: '*', limit: 3, scanLimit: 2 }).then((result) => {
          expect(result).to.have.length(3);
          return result;
        })
      );
    });

    describe('TTL checks', () => {
      beforeEach(() => setTestKeys(assortedExpiryKeys));

      it(
        'when no-expiry is used, selects only keys that have TTL of -1',
        () => scan({ pattern: '*', noExpiry: true }).then((result) => {
          expect(matchedKeys(result)).to.equal('noExpiry1,noExpiry2');
          return result;
        })
      );

      it(
        'minTTL and maxTTL effectively constrain the range of TTL values',
        () => scan({ pattern: '*', minTTL: 9, maxTTL: 11 }).then((result) => {
          expect(matchedKeys(result)).to.equal('expires10');
          return result;
        })
      );
    });

    describe('IDLETIME checks (takes 10 seconds)', function () { // eslint-disable-line func-names
      this.timeout(10000);

      before((done) => {
        _.each([
          () => setTestKeys({ '8-seconds-ago': -1 }),
          () => setTestKeys({ '6-seconds-ago': -1 }),
          () => setTestKeys({ '4-seconds-ago': -1 }),
          () => setTestKeys({ '2-seconds-ago': -1 }),
          () => { done(); },
        ], (func, idx) => setTimeout(func, idx * 2000));
      });

      it(
        'minIdle and maxIdle effectively constrain the range of IDLETIME values',
        () => scan({ pattern: '*-seconds-ago', minIdle: '3s', maxIdle: '7s' }).then((result) => {
          expect(matchedKeys(result)).to.equal('4-seconds-ago,6-seconds-ago');
          return result;
        })
      );
    });

    afterEach(deleteTestKeys);
  });
});
