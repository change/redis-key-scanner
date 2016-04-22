var
  defaultSentinelPort = 26379,

  usage = [
    'Usage:',
    '  node redis-key-scanner <sentinel_host> <redis_name> [options]',
    '',
    '   Options:',
    '    -p <sentinel_port>  Default is ' + defaultSentinelPort,
    '    --scan-limit=N      Limit total number of keys to scan',
    '    --limit=N           Limit total number of keys to select (output)',
    '',
    '   Select keys that:',
    '    --max-idle=<T>      have been inactive for no more than <T>',
    '    --max-ttl=<T>       have a TTL of no more than <T>',
    '    --min-idle=<T>      have been inactive for at least <T>',
    '    --min-ttl=<T>       have a TTL of at least <T>',
    '    --no-expiry         that have TTL of -1 (ie. no expiry)',
    '    --pattern=<p>       match key pattern (default: *)',
    '',
    '   Timeframes <T> are of the form "<number><unit>" where unit may be any',
    "   of 's' (seconds), 'm' (minutes), 'h' (hours), 'd' (days), or 'w' weeks."
  ].join('\n'),

  _ = require('lodash'),
  Redis = require('ioredis'),
  timeframeToSeconds = require('timeframe-to-seconds'),

  args = require('minimist')(process.argv.slice(2)),
  sentinelHost = args['_'].length && args['_'][0],
  sentinelPort = Number(args.p) || defaultSentinelPort,
  redisName = args['_'].length > 1 && args['_'][1],
  scanLimit = args['scan-limit'] || Infinity,
  selectOptions = _.pickBy({
    limit: args.limit || Infinity,
    maxIdle: timeframeToSeconds(args['max-idle']),
    maxTTL: timeframeToSeconds(args['max-ttl']),
    minIdle: timeframeToSeconds(args['min-idle']),
    minTTL: timeframeToSeconds(args['min-ttl']),
    noExpiry: args['expiry'] === false,
    pattern: args.pattern || '*'
  }, function(v) { return !_.isNaN(v) && v !== false; });

if (!sentinelHost || !redisName ||
  (args.hasOwnProperty('max-idle') && isNaN(selectOptions.maxIdle)) ||
  (args.hasOwnProperty('max-ttl') && isNaN(selectOptions.maxTTL)) ||
  (args.hasOwnProperty('min-idle') && isNaN(selectOptions.minIdle)) ||
  (args.hasOwnProperty('min-ttl') && isNaN(selectOptions.minTTL)) ||
  isNaN(scanLimit) || isNaN(selectOptions.limit))
{
  console.log(usage);
  process.exit(1);
}

function log(obj) {
  console.log(JSON.stringify(obj));
}

function redisConnect(options) {
  return new Redis(_.extend({
    sentinels: [{host: sentinelHost, port: sentinelPort}],
  }, options));
}

var totalKeysScanned = 0, totalKeysSelected = 0, atSelectLimit = false;

function scanKeys(redisName, keyPattern, callback) {
  var redis = redisConnect({name: redisName, role: 'slave'}),
    scanStream = redis.scanStream({match: keyPattern, count: 1000}),
    streamKeysScanned = 0,
    streamKeysSelected = 0;

  callback = _.once(callback);

  function endScan() {
    callback({
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

scanKeys(redisName, selectOptions.pattern, function (results) {
  log({
    scans: results,
    selectOptions: selectOptions,
    totals: {scanned: totalKeysScanned, selected: totalKeysSelected}
  });
  process.exit(0);
});
