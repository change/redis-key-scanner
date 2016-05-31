var
  defaults = {
    pattern: '*',
    redisPort: 6379,
    sentinelPort: 26379,
    scanBatch: 1000,
    scanLimit: Infinity,
    limit: Infinity
  },

  supportedOptions = [
    'host', 'port', 'redisMaster', 'scanBatch', 'scanLimit', 'limit',
    'maxIdle', 'maxTTL', 'minIdle', 'minTTL', 'noExpiry', 'pattern'
  ],

  usage = [
    'Usage:',
    '  node redis-key-scanner <host>[:<port>] [<master_name>] [options]',
    '',
    '   Synopsis:',
    '    Scan a redis server for keys matching specified criteria, including',
    '    key patterns, TTL and IDLETIME.  Selected/matched keys are output in',
    '    a JSON log format.  The scan is non-destructive, and doesn\'t even',
    '    read any actual key values, so it won\'t affect IDLETIME either.',
    '',
    '   Options:',
    '    <host>              (Required) Hostname or IP of the redis server or',
    '                        sentinel to scan',
    '    <port>              Port number if non-standard.  Default redis port',
    '                        is ' + defaults.redisPort + ', and default sentinel',
    '                        port is ' + defaults.sentinelPort + '.',
    '    <master_name>       Inclusion of this argument inidicates the use of',
    '                        redis sentinel.  When <master_name> is specified,',
    '                        the <host> and <port> options are understood to',
    '                        refer to a sentinel as opposed to a regular redis',
    '                        server.  However, a connection will be attempted to',
    '                        the corresponding *slave*.',
    '',
    '    --scan-batch=N      Batch/count size to use with the redis SCAN',
    '                        operation.  Default is ' + defaults.scanBatch + '.',
    '    --scan-limit=N      Limit total number of keys to scan.  Scanning will',
    '                        cease once scan-limit is reached, regardless of',
    '                        whether any matching keys have been selected.  By',
    '                        default there is no limit.',
    '    --limit=N           Limit total number of keys to select (output)',
    '',
    '   Select keys that:',
    '    --max-idle=<T>      have been inactive for no more than <T>',
    '    --max-ttl=<T>       have a TTL of no more than <T>',
    '    --min-idle=<T>      have been inactive for at least <T>',
    '    --min-ttl=<T>       have a TTL of at least <T>',
    '    --no-expiry         have TTL of -1 (ie. no expiry)',
    '    --pattern=<p>       match key pattern (default: ' + defaults.pattern + ')',
    '',
    '   Timeframes <T> are of the form "<number><unit>" where unit may be any',
    "   of 's' (seconds), 'm' (minutes), 'h' (hours), 'd' (days), or 'w' weeks."
  ].join('\n'),

  _ = require('lodash'),
  EventEmitter = require('events').EventEmitter,
  Promise = require('bluebird'),
  Redis = require('ioredis'),
  timeframeToSeconds = require('timeframe-to-seconds'),
  util = require('util');

function RedisKeyScanner(options) {
  var self = this;

  // Validate options
  if (!options.host) {
    throw new TypeError('Host is required');
  }
  options.port = Number(options.port);
  if (!options.port) {
    throw new TypeError('Port number is required');
  }
  options.pattern = options.pattern || defaults.pattern;
  _.each(['maxIdle', 'maxTTL', 'minIdle', 'minTTL'], function(opt) {
    if (_.has(options, opt)) {
      options[opt] = timeframeToSeconds(options[opt]);
      if (isNaN(options[opt])) {
        throw new TypeError('Expected ' + opt + ' to be a timeframe.');
      }
    }
  });
  _.each(['scanBatch', 'scanLimit', 'limit'], function(opt) {
    if (!_.has(options, opt)) {
      options[opt] = defaults[opt];
    }
    if (isNaN(options[opt])) {
      throw new TypeError('Must be a number: ' + opt);
    }
  });
  var unsupportedOptions = _.keys(_.omit(options, supportedOptions));
  if (unsupportedOptions.length) {
    throw new TypeError('Unsupported option(s): ' + unsupportedOptions);
  }
  this.options = options;

  // Connect to redis server / sentinel
  var server = _.pick(options, ['host', 'port']),
    redisDescription;
  if (options.redisMaster) {
    this.redisOptions = {
      sentinels: [server],
      name: options.redisMaster,
      role: 'slave'
    };
    redisDescription = options.redisMaster;
  } else {
    this.redisOptions = server;
    redisDescription = _.values(server).join(':');
  }
  var redis = new Redis(this.redisOptions);
  redis.on('error', _.once(function(err) {
    self.emit('error', err);
  }));

  // Initiate scan
  var atSelectLimit = false,
    pipelinePromises = [],
    scanStream = redis.scanStream({
      match: options.pattern,
      count: options.scanBatch
    }),
    streamKeysScanned = 0,
    streamKeysSelected = 0;

  var endScan = _.once(function() {
    Promise.all(pipelinePromises).then(function() {
      self.write(_.extend({
        keysScanned: streamKeysScanned,
        keysSelected: streamKeysSelected
      }, options));
      self.end();
    });
  });

  scanStream.on('data', function(batchKeys) {
    var pipeline = redis.pipeline();
    streamKeysScanned += batchKeys.length;
    _.each(batchKeys, function(key) {
      pipeline.ttl(key).object('IDLETIME', key);
    });

    pipelinePromises.push(pipeline.exec().then(function(results) {
      _.each(batchKeys, function(key, i) {
        // Since we're pipelining 2 redis operations per key, the `results`
        // array will have two items per key.  Hence the funky array indexing
        // here:
        var firstIdx = i * 2,
          ttl = results[firstIdx][1],
          idletime = results[firstIdx + 1][1];

        if (!atSelectLimit &&
          (isNaN(options.maxIdle) || idletime <= options.maxIdle) &&
          (isNaN(options.maxTTL) || ttl <= options.maxTTL) &&
          (isNaN(options.minIdle) || idletime >= options.minIdle) &&
          (isNaN(options.minTTL) || ttl >= options.minTTL) &&
          (!_.has(options, 'noExpiry') || (options.noExpiry && ttl === -1)))
        {
          streamKeysSelected++;
          atSelectLimit = streamKeysSelected >= options.limit;
          self.write({
            name: redisDescription,
            key: key,
            ttl: ttl,
            idletime: idletime
          });
        }
      });
    }));

    if (atSelectLimit || streamKeysScanned >= options.scanLimit) {
      scanStream.pause();
      endScan();
    }
  });

  scanStream.on('end', endScan);

  EventEmitter.call(this);
}
util.inherits(RedisKeyScanner, EventEmitter);

RedisKeyScanner.prototype.write = function(data) {
  this.emit('data', data);
};

RedisKeyScanner.prototype.end = function() {
  this.emit('end');
};

function parseCommandLineAndScanKeys() {
  var args = require('minimist')(process.argv.slice(2)),
    hostPort = args['_'].length && args['_'][0].split(':'),
    redisMaster = args['_'].length > 1 && args['_'][1],
    usingSentinel = !!redisMaster,
    defaultPort = defaults[usingSentinel ? 'sentinelPort' : 'redisPort'],
    options = _.pickBy({
      host: hostPort.length && hostPort[0],
      port: hostPort.length > 1 ? hostPort[1] : defaultPort,
      redisMaster: redisMaster || false,
      scanBatch: args['scan-batch'] || defaults.scanBatch,
      scanLimit: args['scan-limit'] || defaults.scanLimit,
      limit: args.limit || defaults.limit,
      maxIdle: args['max-idle'],
      maxTTL: args['max-ttl'],
      minIdle: args['min-idle'],
      minTTL: args['min-ttl'],
      noExpiry: args['expiry'] === false,
      pattern: args.pattern || '*'
    }, function(v) { return !_.isNaN(v) && !_.isUndefined(v) && v !== false; }),
    redisKeyScanner,
    supportedArgs = [
      '_', 'expiry', 'limit', 'max-idle', 'max-ttl', 'min-idle', 'min-ttl',
      'pattern', 'scan-batch', 'scan-limit'
    ],
    unsupportedArgs = _.keys(_.omit(args, supportedArgs));

  try {
    if (unsupportedArgs.length) {
      throw new TypeError('Unsupported arg(s): ' + unsupportedArgs);
    }
    redisKeyScanner = new RedisKeyScanner(options);
    redisKeyScanner.on('data', function(data) {
      console.log(JSON.stringify(data));
    });
    redisKeyScanner.on('end', function() {
      process.exit(0);
    });
  } catch (ex) {
    console.error(ex);
    console.log(usage);
    process.exit(1);
  }
}

if (require.main === module) {
  parseCommandLineAndScanKeys();
}

module.exports = RedisKeyScanner;
