#!/usr/bin/env node
const _ = require('lodash');
const { EventEmitter } = require('events');
const minimist = require('minimist');
const Prompt = require('prompt-password');
const Redis = require('ioredis');
const timeframeToSeconds = require('timeframe-to-seconds');
const util = require('util');

const defaults = {
  debug: false,
  db: 0,
  pattern: '*',
  redisPort: 6379,
  sentinelPort: 26379,
  scanBatch: 1000,
  scanLimit: Infinity,
  limit: 1000,
};

const usage = `Usage:
  node redis-key-scanner <host>[:<port>] [<master_name>] [options]

 Synopsis:
  Scan a redis server for keys matching specified criteria, including
  key patterns, TTL and IDLETIME.  Selected/matched keys are output in
  a JSON log format.  The scan is non-destructive, and doesn't even
  read any actual key values, so it won't affect IDLETIME either.

 Options:
  <host>              (Required) Hostname or IP of the redis server or
                      sentinel to scan.
  <port>              Port number if non-standard.  Default redis port
                      is ${defaults.redisPort}, and default sentinel port is ${defaults.sentinelPort}.
  <master_name>       Inclusion of this argument inidicates the use of
                      redis sentinel.  When <master_name> is specified,
                      the <host> and <port> options are understood to
                      refer to a sentinel as opposed to a regular redis
                      server.  However, a connection will be attempted to
                      the corresponding *slave*.

  -p                  Prompt for a password that will be passed to the
                      redis server.
  --scan-batch=N      Batch/count size to use with the redis SCAN
                      operation.  Default is ${defaults.scanBatch}.
  --scan-limit=N      Limit total number of keys to scan.  Scanning will
                      cease once scan-limit is reached, regardless of
                      whether any matching keys have been selected.  By
                      default there is no limit.
  --limit=N           Limit total number of keys to select (output)
  --debug             Debug mode

 Select keys that:
  --db=N              reside in logical db <N> (defaults to 0)
  --max-idle=<T>      have been inactive for no more than <T>
  --max-ttl=<T>       have a TTL of no more than <T>
  --min-idle=<T>      have been inactive for at least <T>
  --min-ttl=<T>       have a TTL of at least <T>
  --no-expiry         have TTL of -1 (ie. no expiry)
  --pattern=<p>       match key pattern (default: ${defaults.pattern})

 Timeframes <T> are of the form "<number><unit>" where unit may be any
 of 's' (seconds), 'm' (minutes), 'h' (hours), 'd' (days), or 'w' weeks.`;

const supportedCliArgs = [
  '_', 'db', 'debug', 'expiry', 'limit', 'max-idle', 'max-ttl', 'min-idle', 'min-ttl', 'p',
  'pattern', 'scan-batch', 'scan-limit',
];

const supportedOptions = [
  'db', 'debug', 'host', 'limit', 'maxIdle', 'maxTTL', 'minIdle', 'minTTL', 'noExpiry', 'password',
  'pattern', 'port', 'redisMaster', 'scanBatch', 'scanLimit',
];

/* eslint-disable no-console */
const log = (...args) => { console.log(...args); };
const logError = (err) => { console.error(_.get(err, 'message') || String(err)); };
/* eslint-enable no-console */

function sanitizeOptions(rawOptions) {
  const options = _.clone(rawOptions);

  function hasOption(opt) {
    return _.has(options, opt);
  }

  if (!options.host) {
    throw new TypeError('Host is required');
  }
  options.port = Number(options.port);
  if (!options.port) {
    throw new TypeError('Port number is required');
  }

  options.pattern = options.pattern || defaults.pattern;

  _.each(['maxIdle', 'maxTTL', 'minIdle', 'minTTL'], (opt) => {
    if (hasOption(opt)) {
      options[opt] = timeframeToSeconds(options[opt]);
      if (_.isNaN(options[opt])) {
        throw new TypeError(`Expected ${opt} to be a timeframe.`);
      }
    }
  });

  _.each(['scanBatch', 'scanLimit', 'limit'], (opt) => {
    if (!hasOption(opt)) {
      options[opt] = defaults[opt];
    }
    if (_.isNaN(options[opt])) {
      throw new TypeError(`Must be a number: ${opt}`);
    }
  });

  const unsupportedOptions = _.keys(_.omit(options, supportedOptions));
  if (unsupportedOptions.length) {
    throw new TypeError(`Unsupported option(s): ${unsupportedOptions}`);
  }

  options.checkTTL = _.some(['noExpiry', 'maxTTL', 'minTTL'], hasOption);

  return options;
}

const logOptions = (options, redisOptions) => {
  if (options.debug) {
    log('options:', JSON.stringify(options));
    log('redisOptions:', JSON.stringify(redisOptions));
  }
};

const logConnection = (options, redis) => {
  if (options.debug) {
    log('Waiting to connect...');
    redis.on('connect', () => { log('Redis connected'); });
    redis.on('ready', () => { log('Redis ready'); });
  }
};

function connect(options) {
  // Connect to redis server / sentinel
  const server = _.pick(options, ['host', 'port']);
  let redisOptions = server;
  if (options.redisMaster) {
    redisOptions = {
      sentinels: [server],
      name: this.options.redisMaster,
      role: 'master',
    };
  }

  redisOptions.db = options.db || 0;

  if (options.password) {
    redisOptions.password = options.password;
    redisOptions.tls = {};
  }

  logOptions(options, redisOptions);

  const redis = new Redis(redisOptions);
  redis.description = options.redisMaster || `${server.host}:${server.port}:${server.db}`;

  logConnection(options, redis);

  const self = this;
  redis.on('error', _.once((err) => {
    self.emit('error', err);
  }));

  return redis;
}

function scan(options, redis) {
  const { maxIdle, maxTTL, minIdle, minTTL } = options;
  const pipelinePromises = [];
  const scanStream = redis.scanStream({ match: options.pattern, count: options.scanBatch });
  const self = this;

  let atSelectLimit = false;
  let keysScanned = 0;
  let keysSelected = 0;

  // eslint-disable-next-line complexity
  const selectKey = ({ idletime, ttl }) => !atSelectLimit
    && (_.isUndefined(maxIdle) || idletime <= maxIdle)
    && (_.isUndefined(maxTTL) || ttl <= maxTTL)
    && (_.isUndefined(minIdle) || idletime >= minIdle)
    && (_.isUndefined(minTTL) || ttl >= minTTL)
    && (!_.has(options, 'noExpiry') || (options.noExpiry && ttl === -1));

  const endScan = _.once(() => Promise.all(pipelinePromises)
    .then(() => null)
    .catch(err => err)
    .then((err) => {
      self.write({
        keysScanned,
        keysSelected,
        ...err ? { error: err.message || String(err) } : null,
        ..._.omit(options, 'password'),
      });
      self.end();
      return err;
    }));

  scanStream.on('data', (batchKeys) => {
    const pipeline = redis.pipeline();
    keysScanned += batchKeys.length;
    if (options.debug) {
      log(`scanned ${batchKeys.length} keys`);
    }
    _.each(batchKeys, (key) => {
      pipeline.object('IDLETIME', key);
      if (options.checkTTL) { pipeline.ttl(key); }
    });

    pipelinePromises.push(pipeline.exec().then((results) => {
      _.each(batchKeys, (key, i) => {
        // Since we are sometimes pipelining 2 redis operations per key, the
        // `results` array may have two items per key.  Hence the funky array
        // indexing logic here:
        const idleIdx = options.checkTTL ? i * 2 : i;
        const idletime = results[idleIdx][1];
        const ttl = options.checkTTL && results[idleIdx + 1][1];

        if (selectKey({ idletime, ttl })) {
          keysSelected += 1;
          atSelectLimit = keysSelected >= options.limit;
          const entry = { name: redis.description, key, idletime };
          if (options.checkTTL) { entry.ttl = ttl; }
          self.write(entry);
        }
      });
      return true;
    }));

    if (atSelectLimit || keysScanned >= options.scanLimit) {
      scanStream.pause();
      endScan();
    }
  });

  scanStream.on('end', endScan);
}

function RedisKeyScanner(rawOptions) {
  const options = sanitizeOptions(rawOptions);
  const redis = connect.bind(this)(options);
  scan.bind(this)(options, redis);
  EventEmitter.call(this);
}
util.inherits(RedisKeyScanner, EventEmitter);

RedisKeyScanner.prototype.write = function write(data) {
  this.emit('data', data);
};

RedisKeyScanner.prototype.end = function end() {
  this.emit('end');
};

const promptForPassword = () => {
  const prompt = new Prompt({
    type: 'password',
    message: 'Enter redis server password',
    name: 'password',
  });
  return prompt.run();
};

async function parseCommandLineAndScanKeys() { // eslint-disable-line complexity
  const args = minimist(process.argv.slice(2));
  const hostPort = args._.length && args._[0].split(':');
  const redisMaster = args._.length > 1 && args._[1];
  const usingSentinel = !!redisMaster;
  const defaultPort = defaults[usingSentinel ? 'sentinelPort' : 'redisPort'];

  const password = args.p ? await promptForPassword() : undefined;

  const options = _.pickBy({
    debug: !!args.debug,
    host: hostPort.length && hostPort[0],
    port: hostPort.length > 1 ? hostPort[1] : defaultPort,
    redisMaster: redisMaster || false,
    db: args.db || defaults.db,
    scanBatch: args['scan-batch'] || defaults.scanBatch,
    scanLimit: args['scan-limit'] || defaults.scanLimit,
    limit: args.limit || defaults.limit,
    maxIdle: args['max-idle'],
    maxTTL: args['max-ttl'],
    minIdle: args['min-idle'],
    minTTL: args['min-ttl'],
    noExpiry: args.expiry === false,
    password,
    pattern: args.pattern || '*',
  }, v => !_.isNaN(v) && !_.isUndefined(v) && v !== false);

  const unsupportedArgs = _.keys(_.omit(args, supportedCliArgs));

  try {
    if (unsupportedArgs.length) {
      throw new TypeError(`Unsupported arg(s): ${unsupportedArgs}`);
    }
    const redisKeyScanner = new RedisKeyScanner(options);
    redisKeyScanner.on('data', (data) => {
      log(JSON.stringify(data));
    });
    redisKeyScanner.on('end', () => {
      process.exit(0);
    });
    redisKeyScanner.on('error', (err) => {
      logError(err);
      process.exit(2);
    });
  } catch (ex) {
    logError(ex);
    log(usage);
    process.exit(1);
  }
}

if (require.main === module) {
  parseCommandLineAndScanKeys();
}

module.exports = RedisKeyScanner;
