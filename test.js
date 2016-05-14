var _ = require('lodash'),
  defaultArgs = {
    host: 'localhost',
    port: 6379
  },
  expect = require('chai').expect,
  Redis = require('ioredis'),
  RedisKeyScanner = require('.'),
  prefix = 'redis-key-scanner:test:';

function defaultRedis() {
  return new Redis(_.pick(defaultArgs, ['host', 'port']));
}

function deleteTestKeys(done) {
  var redis = defaultRedis();
  redis.keys(`${prefix}*`).then(result => {
    var deletePipeline = redis.pipeline();
    _.each(result, key => deletePipeline.del(key));
    deletePipeline.exec().then(() => done());
  });
}

function extendArgs(o) {
  return () => {
    var opts = _.extend({}, defaultArgs, o);
    if (opts.pattern) {opts.pattern = prefix + opts.pattern;}
    return new RedisKeyScanner(opts);
  };
}

// From a result set of selected keys, return a sorted, comma-separated list of
// just the key names (with the prefix removed).
function matchedKeys(selected) {
  return String(_.map(selected, item => item.key.substr(prefix.length)).sort());
}

function omitArgs(keys) {
  return () => new RedisKeyScanner(_.omit(defaultArgs, keys));
}

// Perform a scan, and return only the selected keys via the callback
function scan(options, callback) {
  var scanner = extendArgs(options)(), results = [];
  scanner.on('data', data => data.key && results.push(data));
  scanner.on('end', () => callback(results));
}

function setTestKeys(keys) {
  var pipeline = defaultRedis().pipeline();
  _.each(keys, (ttl, key) => {
    var args = [prefix + key, key];
    if (ttl > -1) {args = args.concat(['ex', ttl]);}
    pipeline.set.apply(pipeline, args);
  });
  return pipeline.exec();
}

describe('class RedisKeyScanner', () => {
  describe('instantiation throws a TypeError when', () => {
    it('no arguments', () => {
      expect(() => new RedisKeyScanner()).to.throw(TypeError);
    });

    it('no host', () => expect(omitArgs('host')).to.throw(TypeError));

    it('no port', () => expect(omitArgs('port')).to.throw(TypeError));

    it('unrecognized argument', () => expect(extendArgs({foo: 1})).to.throw(TypeError));

    describe('an invalid timeframe value is provided;', () => {
      it('`w` for max-idle', () => {
        expect(extendArgs({'max-idle': 'w'})).to.throw(TypeError);
      });

      it('`foo` for max-ttl', () => {
        expect(extendArgs({'max-ttl': 'foo'})).to.throw(TypeError);
      });

      it('`true` for min-idle', () => {
        expect(extendArgs({'min-idle': true})).to.throw(TypeError);
      });

      it('`null` for min-ttl', () => {
        expect(extendArgs({'min-ttl': null})).to.throw(TypeError);
      });
    });
  });

  describe('when redis not running on the specified port', () => {
    it('emits an error event', done => {
      extendArgs({port: 80})().on('error', () => done());
    });
  });

  describe('when redis connection succeeds', () => {
    var assortedExpiryKeys = {noExpiry1: -1, noExpiry2: -1, expires10: 10, expires100: 100},
      fourKeys = {one: -1, two: -1, three: -1, four: -1};

    before(deleteTestKeys);

    describe('basic key selection', () => {
      describe('when no keys exist', () => {
        it('a scan for `*` returns no results', done => {
          scan({pattern: '*'}, selected => done(selected.length));
        });
      });

      describe('when exactly one key exists', () => {
        before(done => setTestKeys({foo: -1}).then(() => done()));

        it('a scan for `*` selects that key (only)', done => {
          scan({pattern: '*'}, selected => done(matchedKeys(selected) !== 'foo'));
        });
      });
    });

    describe('pattern matching', () => {
      beforeEach(done => setTestKeys(fourKeys).then(() => done()));

      it('pattern `*` results in all keys', done => {
        scan({pattern: '*'}, selected => done(matchedKeys(selected) !== 'four,one,three,two'));
      });

      it('prefix wildcard works', done => {
        scan({pattern: '*e'}, selected => done(matchedKeys(selected) !== 'one,three'));
      });

      it('postfix wildcard works', done => {
        scan({pattern: 't*'}, selected => done(matchedKeys(selected) !== 'three,two'));
      });
    });

    describe('when no-expiry is used', () => {
      beforeEach(done => setTestKeys(assortedExpiryKeys).then(() => done()));

      it('selects only keys that have TTL of -1', done => {
        scan({pattern: '*', noExpiry: true}, selected => done(matchedKeys(selected) !== 'noExpiry1,noExpiry2'));
      });
    });

    describe('limits', () => {
      beforeEach(done => setTestKeys(fourKeys).then(() => done()));

      it('limit gets honored as an upper bound on results', done => {
        scan({pattern: '*', limit: 3}, selected => done(selected.length !== 3));
      });

      it('even when limit is greater than scan-limit', done => {
        scan({pattern: '*', limit: 3, scanLimit: 2}, selected => done(selected.length !== 3));
      });
    });

    afterEach(deleteTestKeys);
  });
});
