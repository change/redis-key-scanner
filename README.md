redis-key-scanner
=================

Scan a redis server for keys matching specified criteria, including key patterns, TTL and IDLETIME.
Selected/matched keys are output in a JSON log format.  The scan is non-destructive, and doesn't
even read any actual key values.

Note that if you are checking TTL times, due to an acknowledged [bug][redis-ttl-bug] in Redis, this
will actually reset the IDLETIME to 0 for the selected keys, so subsequent scans may not return the
same results.

Example command-line usage, querying a locally running redis server for keys that start with the
prefix "mykeys:" and have been idle (ie., not written or read) for at least one week:
```
> npm install -g redis-key-scanner
> redis-key-scanner localhost --pattern=mykeys:* --min-idle=1w
```

Output will be one JSON line per selected key, followed by a "summary" line with total stats:
```
{"name":"localhost:6379","key":"mykeys:larry","idletime":604800}
{"name":"localhost:6379","key":"mykeys:curly","idletime":900000}
{"name":"localhost:6379","key":"mykeys:moe","idletime":1000000}
{"keysScanned":17,"keysSelected":3,"host":"localhost","port":6379,"scanBatch":1000,"scanLimit":null,"limit":null,"pattern":"mykeys:*"}
```

You can alternatively require `redis-key-scanner` as a Node.js module, in which case it implements a
stream interface and each record will be emitted as a separate 'data' event.
```js
const RedisKeyScanner = require('redis-key-scanner');
const scanner = new RedisKeyScanner({
  host: 'localhost',
  pattern: 'mykeys:*',
  minIdle: '1w'
});
scanner.on('data', (data) => {
  console.log(data);
});
scanner.on('end', () => {
  // clean up
});
```

Run `redis-key-scanner` to see the full list of available options.


[redis-ttl-bug]: https://github.com/antirez/redis/pull/2090#issuecomment-75944263
