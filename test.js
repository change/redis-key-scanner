var _ = require('lodash'),
  defaultArgs = {
    host: 'localhost',
    port: 6379
  },
  expect = require('chai').expect,
  RedisKeyScanner = require('.');

function extend(o) {
  return function() {
    new RedisKeyScanner(_.extend({}, defaultArgs, o));
  };
}
function omit(keys) {
  return function() {
    new RedisKeyScanner(_.omit(defaultArgs, keys));
  };
}

describe('class RedisKeyScanner', function() {
  describe('when instantiated with', function() {
    describe('no arguments', function() {
      it('throws a TypeError', function() {
        expect(function() { new RedisKeyScanner(); }).to.throw(TypeError);
      });
    });

    describe('no host', function() {
      it('throws a TypeError', function() {
        expect(omit('host')).to.throw(TypeError);
      });
    });

    describe('no port', function() {
      it('throws a TypeError', function() {
        expect(omit('port')).to.throw(TypeError);
      });
    });

    describe('invalid timeframe', function() {
      describe('`w` for max-idle', function() {
        it('throws a TypeError', function() {
          expect(extend({'max-idle': 'w'})).to.throw(TypeError);
        });
      });

      describe('`foo` for max-ttl', function() {
        it('throws a TypeError', function() {
          expect(extend({'max-ttl': 'foo'})).to.throw(TypeError);
        });
      });

      describe('`true` for min-idle', function() {
        it('throws a TypeError', function() {
          expect(extend({'min-idle': true})).to.throw(TypeError);
        });
      });

      describe('`null` for min-ttl', function() {
        it('throws a TypeError', function() {
          expect(extend({'min-ttl': null})).to.throw(TypeError);
        });
      });
    });
  });
});
