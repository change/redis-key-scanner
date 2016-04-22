var _ = require('lodash'),
  args = require('minimist')(process.argv.slice(2)),
  host = args.h,
  port = Number(args.p) || 26379,
  oneWeekInSeconds = 604800,
  Redis = require('ioredis'),
  when = require('when');

function printUsage() {
  console.log('Usage:');
  console.log('  node redis-keys-cleanup.js -h <sentinel_host> [-p <port>] [--delete]');
}

if (!host) {
  printUsage();
  process.exit(1);
}

var cruft = {
  misc: [
    'resque:*',
    'veracity:*'
  ],
  resque: [
    'production:*',
    'veracity:*'
  ],
  session: [
    'production:*',
    'resque:*',
    'veracity:*'
  ]
};

function log(obj) {
  console.log(JSON.stringify(obj));
}

function redisConnect(options) {
  return new Redis(_.extend({
    sentinels: [{host: host, port: port}],
  }, options));
}

function scanKeys(redisName, keyPattern, resolve, reject) {
  var redis = redisConnect({name: redisName, role: 'slave'}),
    scanStream = redis.scanStream({match: keyPattern, count: 1000}),
    totalKeys = 0;

  scanStream.on('data', function(resultKeys) {
    totalKeys += resultKeys.length;
    _.each(resultKeys, function(key) {
      redis.ttl(key, function(err, ttl) {
        redis.object('IDLETIME', key, function(err, idletime) {
          if (ttl === -1 && idletime > oneWeekInSeconds) {
            log({name: redisName, key: key, ttl: ttl, idletime: idletime});
          }
        });
      });
    });
  });

  scanStream.on('end', function() {
    resolve({name: redisName, pattern: keyPattern, totalKeys: totalKeys});
  });
}

when.map(_.keys(cruft), function(redisName) {
  return when.map(cruft[redisName], function(keyPattern) {
    return when.promise(function(resolve, reject) {
      scanKeys(redisName, keyPattern, resolve, reject);
    });
  });
})
  .then(function(results) {
    log(results);
    process.exit(0);
  });
