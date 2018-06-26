[MockRedis](https://github.com/machinezone/php-mockredis)
=========================================================
MockRedis is an in-memory, pure-PHP implementation of Redis.

It aims for compatibility first and speed second.
It's intended to take the place of a proper Redis backend, for the purpose of testing and prototyping.
It primarily provides a new `MockRedis` class to represent a Redis connection, but also provides executable scripts that behave like `redis-server` and `redis-cli`.

Basic Usage
-----------
In its most basic form, MockRedis can be used by instantiating it and calling Redis commands on it via methods of the same name.

This PHP:
```php
$redis = new mz\mockredis\MockRedis();
$redis->set('hello', 'world');
var_dump($redis->get('hello'));
```
Is equivalent to these Redis commands:
```redis
SET hello world
GET hello
```
And would print:
```
string(5) "world"
```

In this mode, the effects of commands will persist until the end of the script.
Multiple instances will share the same data and affect one another.

All arguments are treated as strings, and replies are handled as follows:
- Redis integer reply    -> PHP `int` return
- Redis bulk reply       -> PHP `string|null` return
- Redis multi bulk reply -> PHP `array|null` return
- Redis status reply     -> PHP `true|string` return (`true` for "OK", otherwise a `string`)
- Redis error reply      -> throw `MockRedisException` with the error message


Installation
------------
MockRedis will work with composer's autoloading, or you can `require 'include.php'` to load everything directly.


Persistence
-----------
By default, MockRedis only stores its state in memory, shared across instances of the same name.
It also provides two on-disk storage implementations for persisting across PHP scripts or as a troubleshooting aid.

The handlers for these are `MemoryMockRedisPersistence`, `SerializeMockRedisPersistence`, and `JsonMockRedisPersistence`, which you may specify when you instantiate `MockRedis` or by setting `MockRedis::$persistenceClass`.
You may also use your own `MockRedisPersistence` handler, to mock the persistence layer (e.g. seeding data) or to use some other storage destination or format.


Scripting
---------
If you have the `php-lua` extension loaded, MockRedis will use it to handle the Lua scripting commands `EVAL`, `EVALSHA`, and `SCRIPT`.

MockRedis provides the `PhpLuaMockRedisScripting` class to handle this, but you may override that with your own `MockRedisScripting` class when you instantiate `MockRedis` or by setting `MockRedis::$scriptingClass`.
You may want to override this to mock the scripting layer or integrate with a different Lua module.


Configuration
-------------
Static configuration:
- `MockRedis::$exceptionClass` - override the type for error replies
        You may want this if your code is already expecting a different exception type for redis errors.

- `MockRedis::$timeFunc` - override a callback for current system time
        You may want this in unit tests or other environments where you need full control over the clock.

- `MockRedis::$persistenceClass` - default handler for data persistence
- `MockRedis::$scriptingClass` - set a handler for Lua scripting commands

Instance configuration - `new MockRedis($name, $persistence, $scriptingClass)`:
- `$name` - the name of this instance.  instances with the same name will normally share similar storage.
- `$persistence` - a `MockRedisPersistence` object to handle storage.
- `$scriptingClass` - a class to instantiate for Lua scripting commands.


Mock Executables
----------------
MockRedis also provides an executable script `mockredis` that mimics `redis-server` or `redis-cli`.
This is used for troubleshooting and testing during development, but may also be useful as mock implementations of these executables.
It can also be used as a relatively complex example of how to use MockRedis.


Caveats
-------
PHP's default setting for `precision`, which controls how floats are cast to strings, is lower than how Redis would behave.
You may want to set this `precision` to -1, 16, or 17.
The `mockredis` script itself does this.
Also, `serialize_precision` will affect on-disk persistence.


Compatibility, Coverage, Bugs
-----------------------------
MockRedis attempts to provide the same behavior as Redis where it can.
A few commands, such as INFO or OBJECT, will give a valid response that does not match Redis.
It does not try to match the behavior any specific version of Redis, but instead tries to get the greatest coverage across all versions of Redis.

This is an overview of MockRedis's coverage of Redis commands.  Check `MockRedis.php` for specifics.

Group        |      | Notes
:------------|:-----|:-----
Cluster      | none | (always replies as though disabled)
Connection   | full | partial AUTH (no password may be set)
Geo          | none |
Hashes       | full |
HyperLogLog  | none |
Keys         | most | no MIGRATE, partial OBJECT, partial SORT
Lists        | most | no BLPOP, no BRPOP, no BRPOPLPUSH
Pub/Sub      | none |
Scripting    | most | (requires [php-lua](https://github.com/laruence/php-lua), non-deterministic writes are ignored)<br>no SCRIPT DEBUG, no SCRIPT KILL
Server       | some | BGSAVE, BGREWRITEAOF, DBSIZE, FLUSHALL, FLUSHDB, LASTSAVE, SAVE, TIME<br>partial DEBUG, partial INFO
Sets         | full |
Sorted Sets  | full |
Strings      | most | no BITFIELD
Transactions | none |

`mz\mockredis\NotImplementedException` is thrown when Redis functionality is intentionally unimplemented.

Beyond that, `FIXME` is used to tag discrepancies with Redis, while `TODO` is used to tag improvements.

MockRedis can be tested using Redis's own tests, by symlinking `<mockredis>/bin/mockredis` to `<redis>/src/redis-server`.


License
-------
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details