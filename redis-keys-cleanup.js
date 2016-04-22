var
  defaultSentinelPort = 26379,

  usage = [
    'Usage:',
    '  node redis-keys-cleanup.js -h <sentinel_host> [options]',
    '',
    '   Options:',
    '    -h <sentinel_host>  [Required] Host or IP of redis sentinel',
    '    -p <sentinel_port>  Default is ' + defaultSentinelPort,
    '',
    '   Select keys that:',
    '    --max-idle=<T>      have been inactive for no more than <T>',
    '    --max-ttl=<T>       have a TTL of no more than <T>',
    '    --min-idle=<T>      have been inactive for at least <T>',
    '    --min-ttl=<T>       have a TTL of at least <T>',
    '    --no-expiry         that have TTL of -1 (ie. no expiry)',
    '',
    '    --scan-limit=N      Limit total number of keys to scan',
    '    --limit=N           Limit total number of keys to select (output)',
    '',
    '   Timeframes <T> are of the form "<number><unit>" where unit may be any',
    "   of 's' (seconds), 'm' (minutes), 'h' (hours), 'd' (days), or 'w' weeks."
  ].join('\n'),

  _ = require('lodash'),
  Redis = require('ioredis'),
  timeframeToSeconds = require('timeframe-to-seconds'),
  when = require('when'),

  args = require('minimist')(process.argv.slice(2)),
  host = args.h,
  port = Number(args.p) || defaultSentinelPort,
  scanLimit = args['scan-limit'] || Infinity,
  selectOptions = _.pickBy({
    limit: args.limit || Infinity,
    maxIdle: timeframeToSeconds(args['max-idle']),
    maxTTL: timeframeToSeconds(args['max-ttl']),
    minIdle: timeframeToSeconds(args['min-idle']),
    minTTL: timeframeToSeconds(args['min-ttl']),
    noExpiry: args['expiry'] === false
  }, function(v) { return !_.isNaN(v) && v !== false; });

//console.log(selectOptions);

if (!host ||
  (args.hasOwnProperty('max-idle') && isNaN(selectOptions.maxIdle)) ||
  (args.hasOwnProperty('max-ttl') && isNaN(selectOptions.maxTTL)) ||
  (args.hasOwnProperty('min-idle') && isNaN(selectOptions.minIdle)) ||
  (args.hasOwnProperty('min-ttl') && isNaN(selectOptions.minTTL)) ||
  isNaN(scanLimit) || isNaN(selectOptions.limit))
{
  console.log(usage);
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

var totalKeysScanned = 0, totalKeysSelected = 0, atSelectLimit = false;

function scanKeys(redisName, keyPattern, resolve, reject) {
  var redis = redisConnect({name: redisName, role: 'slave'}),
    scanStream = redis.scanStream({match: keyPattern, count: 1000}),
    streamKeysScanned = 0,
    streamKeysSelected = 0;

  function endScan() {
    resolve({
      name: redisName,
      pattern: keyPattern,
      keysScanned: streamKeysScanned,
      keysSelected: streamKeysSelected
    });
  }

  scanStream.on('data', function(batchKeys) {
    streamKeysScanned += batchKeys.length;
    totalKeysScanned += batchKeys.length;

    _.each(batchKeys, function(key) {
      if (atSelectLimit) {
        return;
      }
      redis.ttl(key, function(err, ttl) {
        redis.object('IDLETIME', key, function(err, idletime) {
          if (!atSelectLimit &&
            (isNaN(selectOptions.maxIdle) || idletime <= selectOptions.maxIdle) &&
            (isNaN(selectOptions.maxTTL) || ttl <= selectOptions.maxTTL) &&
            (isNaN(selectOptions.minIdle) || idletime >= selectOptions.minIdle) &&
            (isNaN(selectOptions.minTTL) || ttl >= selectOptions.minTTL) &&
            (!selectOptions.noExpiry && ttl === -1))
          {
            streamKeysSelected++;
            totalKeysSelected++;
            atSelectLimit = totalKeysSelected >= selectOptions.limit;
            log({name: redisName, key: key, ttl: ttl, idletime: idletime});
          }
        });
      });
    });

    if (atSelectLimit || totalKeysScanned >= scanLimit) {
      scanStream.pause();
      endScan();
    }
  });

  scanStream.on('end', endScan);
}

when.map(_.keys(cruft), function(redisName) {
  return when.map(cruft[redisName], function(keyPattern) {
    return when.promise(function(resolve, reject) {
      scanKeys(redisName, keyPattern, resolve, reject);
    });
  });
})
  .then(function(results) {
    log({
      scans: _.flatten(results),
      selectOptions: selectOptions,
      totals: {scanned: totalKeysScanned, selected: totalKeysSelected}
    });
    process.exit(0);
  });
