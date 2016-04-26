redis-key-scanner
=================

Scan a redis server for keys matching specified criteria, including key
patterns, TTL and IDLETIME.  Selected/matched keys are output in a JSON log
format.  The scan is non-destructive, and doesn't even read any actual key
values, so it won't affect IDLETIME either.
