redis-key-scanner
=================

Scan a redis server for keys matching certain criteria, including key pattern,
ttl, idle time, in order to help assess key usage.

Importantly, this never actually reads the value for any keys, since that would
reset idle time.
