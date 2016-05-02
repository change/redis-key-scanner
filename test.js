var expect = require('chai').expect,
  RedisKeyScanner = require('.');

function noArgs() {
  new RedisKeyScanner();
}
function noHost() {
  new RedisKeyScanner({});
}
function noPort() {
  new RedisKeyScanner({host: '127.0.0.1'});
}

describe('class RedisKeyScanner', function() {
  describe('when instantiated with', function() {
    describe('no arguments', function() {
      it('throws a TypeError', function() {
        expect(noArgs).to.throw(TypeError);
      });
    });

    describe('no host', function() {
      it('throws a TypeError', function() {
        expect(noHost).to.throw(TypeError);
      });
    });

    describe('no port', function() {
      it('throws a TypeError', function() {
        expect(noPort).to.throw(TypeError);
      });
    });
  });
});
