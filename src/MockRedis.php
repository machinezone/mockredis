<?php // Copyright (c) 2017-2018 Machine Zone, Inc. All rights reserved.

namespace mz\mockredis;

// FIXME: array_keys should be coerced to strings
// FIXME: zset values should be coerced to strings
// FIXME: there's no distinction between bulk and status replies.  status
//        replies should probably be a new MockRedisStatusReply class.  not a
//        big change since objects are true-y.
// TODO: add interface checks / type hints to all functions

/**
 * Pure PHP implementation of Redis
 *
 * All public instance methods are Redis commands of the same name, with
 * positional string arguments matching the Redis API.  This includes
 * repositionable name parameters such as 'EX seconds' for SET, and flag-like
 * parameters such as NX and XX for SET.
 * E.g. $mockredis->set('key', 'val', 'NX', 'EX', 60) is equivalent to
 *      $mockredis->set('key', 'val', 'EX', 60, 'NX')
 *
 * Return value type conversions:
 * Redis integer reply    -> int
 * Redis bulk reply       -> string|null
 * Redis multi bulk reply -> array|null
 * Redis status reply     -> true|string - true for "OK", otherwise a string
 * Redis error reply      -> throw MockRedisException with the error message
 *
 * Unimplemented commands will throw a NotImplementedException.
 * Some partially implemented commands will give a more specific exception
 * depending on the error.
 */
class MockRedis {
    /** Value used for OK status replies. */
    const OK = true;

    /** Value used for nil bulk and multi bulk replies. */
    const nil = null;

    /** Value used for PONG status replies. */
    const PONG = 'PONG';

    /** @var string Class name for exceptions generated from Redis errors. */
    public static $exceptionClass = MockRedisException::class;

    /**
     * @var string Class name to be invoked for EVAL, EVALSHA, and SCRIPT.
     *             Must implement MockRedisScripting
     */
    public static $scriptingClass = null;

    /**
     * @var string Class name to be invoked for SAVE, initialization, etc.
     *             Must implement MockRedisPersistence
     */
    public static $persistenceClass = MemoryMockRedisPersistence::class;

    /**
     * @var callable Called to get the current server time as a float.
     *               Called with no arguments, expects a float.
     */
    public static $timeFunc = __CLASS__.'::DefaultFloatTime';

    /**
     * An error reply object for the given string.
     *
     * @see self::$exceptionClass
     * @return Exception
     */
    public static function ErrorReply($error_string) {
        return new self::$exceptionClass($error_string);
    }

    /**
     * A status reply for the given string.
     *
     * @return true|string true for "OK", otherwise a string
     */
    public static function StatusReply($status_string) {
        $status_string = (string)$status_string;
        return $status_string == 'OK' ? self::OK : $status_string;
    }

    /**
     * The current system time as a float. (Default for MockRedis::$timeFunc)
     *
     * @return float
     */
    public static function DefaultFloatTime() {
        return microtime(true);
    }

    /**
     * The current server time as a float.
     *
     * @see MockRedis::$timeFunc
     * @return float
     */
    public static function FloatTime() {
        $timeFunc = self::$timeFunc;
        return (float)$timeFunc();
    }

    /**
     * Construct and load a MockRedis server.
     *
     * The name is the identifier for this instance, which is typically a
     * host name and port, like "host:port" or "host".  This defaults to
     * "mockredis", but the redis default would be "localhost:6379".
     *
     * @param string $name                      This instance's name
     * @param MockRedisPersistence $persistence Object to handle server state
     * @param string $scriptingClass            Class to handle Lua scripting
     */
    public function __construct(
        $name = null,
        $persistence = null,
        $scriptingClass = null)
    {
        $this->_server = $name ?: 'mockredis';

        $this->_persistence = $persistence ?? new self::$persistenceClass;
        $dbs = &$this->_persistence->load($this->_server, self::FloatTime());
        self::ValidateDbs($dbs);
        $this->_dbs = &$dbs;
        $this->select(0);

        $this->_scriptingClass = $scriptingClass ?? self::$scriptingClass;
    }

    /** Persist server state on destruction. */
    public function __destruct() {
        $this->_persistence->save($this->_server, $this->_dbs, self::FloatTime());
    }

    /** Reject undefined commands. */
    public function __call($command, $args) {
        throw self::ErrorReply("ERR unknown command '$command'");
    }


    ///////////////////////////////////////////////////////////////////////////////
    // Cluster (always disabled)

    public function asking() {
        throw self::ErrorReply('ERR This instance has cluster support disabled');
    }

    public function cluster($subcommand, ...$args) {
        throw self::ErrorReply('ERR This instance has cluster support disabled');
    }

    public function readonly() {
        throw self::ErrorReply('ERR This instance has cluster support disabled');
    }

    public function readwrite() {
        throw self::ErrorReply('ERR This instance has cluster support disabled');
    }


    ///////////////////////////////////////////////////////////////////////////
    // Connection

    public function auth($password) {
        throw self::ErrorReply('ERR Client sent AUTH, but no password is set');
    }

    public function echo($message) {
        return (string)$message;
    }

    public function ping($message=null) {
        return isset($message) ? (string)$message : self::PONG;
    }

    public function quit() {
        // do nothing, there's no connection to close.
        return self::OK;
    }

    public function select($index) {
        $dbs = &$this->_getdbs();
        $index = self::IntArg($index, 'DB index');
        if (!isset($dbs[$index])) {
            $dbs[$index] = [];
        }
        $this->_index = $index;
        return self::OK;
    }

    public function swapdb($index1, $index2) {
        $index1 = self::IntArg($index1, 'DB index');
        $index2 = self::IntArg($index2, 'DB index');
        $dbs = &$this->_getdbs();
        $db = $dbs[$index1];
        $dbs[$index1] = $dbs[$index2];
        $dbs[$index2] = $db;
        return self::OK;
    }


    ///////////////////////////////////////////////////////////////////////////////
    // Geo (API reference only, not yet implemented)

    private function &_getgeo($key) {
        return $this->_getzset($key);
    }

    public function geoadd($key, ...$longitude_latitude_member) {
        $score_member = [];
        foreach (self::Tuples($longitude_latitude_member, 3, __FUNCTION__) as $tuple) {
            list($longitude, $latitude, $member) = $tuple;
            $score_member[] = self::ParseLongLat($longitude, $latitude);
            $score_member[] = $member;
        }
        return $this->zadd($key, ...$score_member);
    }

    public function geodist($key, $member1, $member2, $unit=null) {
        $geo = $this->_getgeo($key);
        throw new NotImplementedException;
    }

    public function geohash($key, ...$members) {
        $geo = $this->_getgeo($key);
        foreach ($members as $member) {
            throw new NotImplementedException;
        }
    }

    public function geopos($key, ...$members) {
        $geo = $this->_getgeo($key);
        foreach ($members as $member) {
            throw new NotImplementedException;
        }
    }

    public function georadius($key, $longitude, $latitude, $radius, $unit, ...$args) {
        $geo = $this->_getgeo($key);
        throw new NotImplementedException;
    }

    public function georadius_ro($key, $longitude, $latitude, $radius, $unit, ...$args) {
        $geo = $this->_getgeo($key);
        throw new NotImplementedException;
    }

    public function georadiusbymember($key, $member, $radius, $unit, ...$args) {
        $geo = $this->_getgeo($key);
        throw new NotImplementedException;
    }

    public function georadiusbymember_ro($key, $member, $radius, $unit, ...$args) {
        $geo = $this->_getgeo($key);
        throw new NotImplementedException;
    }


    ///////////////////////////////////////////////////////////////////////////
    // Hashes

    private function &_gethash($key) {
        return $this->_getval($key, 'hash', []);
    }

    private function _weakhget($key, $field) {
        $hash = $this->_getval($key, 'hash', [], false);
        return $hash ? $hash["$field"] ?? self::nil : self::nil;
    }

    public function hdel($key, ...$fields) {
        return self::Remove($this->_gethash($key), $fields);
    }

    public function hexists($key, $field) {
        return (int)isset($this->_gethash($key)["$field"]);
    }

    public function hget($key, $field) {
        return $this->_gethash($key)["$field"] ?? self::nil;
    }

    public function hgetall($key) {
        $result = [];
        foreach ($this->_gethash($key) as $field => $value) {
            $result[] = $field;
            $result[] = $value;
        }
        return $result;
    }

    public function hincrby($key, $field, $increment) {
        $value = $this->hget($key, $field);
        $value = $value === self::nil ? 0 : self::ParseInt($value, 'hash value');
        $value += self::ParseInt($increment, 'value');
        if (!is_int($value)) {
            throw self::ErrorReply('ERR increment or decrement would overflow');
        }
        $this->hset($key, $field, $value);
        return $value;
    }

    public function hincrbyfloat($key, $field, $increment) {
        $value = $this->hget($key, $field);
        $value = $value === self::nil ? 0.0 : self::ParseFloat($value, 'hash value');
        $value += self::ParseFloat($increment, 'value');
        $this->hset($key, $field, $value);
        return (string)$value;
    }

    public function hkeys($key) {
        return array_keys($this->_gethash($key));
    }

    public function hlen($key) {
        return count($this->_gethash($key));
    }

    public function hmget($key, ...$fields) {
        $hash = $this->_gethash($key);

        $values = [];
        foreach ($fields as $field) {
            $values[] = $hash["$field"] ?? self::nil;
        }
        return $values;
    }

    public function hmset($key, ...$field_value) {
        $hash = &$this->_gethash($key);
        if (!$field_value) {
            throw self::ErrorReply("ERR wrong number of arguments for 'hmset' command");
        }
        foreach (self::Tuples($field_value, 2, __FUNCTION__) as $tuple) {
            list($field, $value) = $tuple;
            $hash["$field"] = "$value";
        }
        return self::OK;
    }

    public function hscan($key, $cursor, ...$args) {
        return $this->_scan($this->_gethash($key), true, $args);
    }

    public function hset($key, $field, $value) {
        $field = "$field";
        $hash = &$this->_gethash($key);
        $exists = isset($hash[$field]);
        $hash[$field] = "$value";
        return (int)!$exists;
    }

    public function hsetnx($key, $field, $value) {
        $field = "$field";
        $hash = &$this->_gethash($key);
        $exists = isset($hash[$field]);
        if (!$exists)
            $hash[$field] = "$value";
        return (int)!$exists;
    }

    public function hstrlen($key, $field) {
        return strlen($this->hget($key, $field));
    }

    public function hvals($key) {
        return array_values($this->_gethash($key));
    }


    ///////////////////////////////////////////////////////////////////////////////
    // HyperLogLog (API reference only, not yet implemented)

    private function _getpf($key) {
        return $this->_getstring($key);
    }

    public function pfadd($key, ...$elements) {
        $pf = $this->_getpf($key);
        throw new NotImplementedException;
    }

    public function pfcount(...$keys) {
        foreach ($keys as $key) {
            $pf = $this->_getpf($key);
            throw new NotImplementedException;
        }
    }

    // UNDOCUMENTED
    // public function pfdebug($subcommand, ...$args) {
    //     return $this->_subcommand(__FUNCTION__, $subcommand, $args);
    // }

    public function pfmerge($destkey, ...$sourcekeys) {
        $pf = [];
        foreach ($sourcekeys as $sourcekey) {
            $pf = $this->_getpf($sourcekey);
            throw new NotImplementedException;
        }
    }

    // UNDOCUMENTED
    // public function pfselftest() {
    //     throw new NotImplementedException;
    // }


    ///////////////////////////////////////////////////////////////////////////
    // Keys

    public function del(...$keys) {
        $db = &$this->_getdb();

        $deleted = 0;
        foreach ($keys as $key) {
            $deleted += (bool)$this->_getobj($key);
            unset($db["$key"]);
        }
        return (int)$deleted;
    }

    public function dump($key) {
        $obj = $this->_getobj($key);
        if ($obj) {
            // encode $obj directly, so we can use our load-time validator in
            // RESTORE, although the expiration field is ignored.
            $obj = serialize($obj);

            // serialize with a bogus RDB version and checksum, so a real Redis
            // will reject this dump.
            return "$obj\xff\xff".hash('crc32', $obj);
        } else {
            return self::nil;
        }
    }

    public function exists(...$keys) {
        $count = 0;
        foreach ($keys as $key) {
            $count += $this->_getobj($key) ? 1 : 0;
        }
        return $count;
    }

    public function expire($key, $seconds) {
        $seconds = self::ParseInt($seconds);
        $obj = &$this->_getobj($key);
        if (!$obj) {
            return 0;
        } else {
            $obj[2] = self::FloatTime() + $seconds;
            return 1;
        }
    }

    public function expireat($key, $timestamp) {
        $timestamp = self::ParseInt($timestamp);
        return $this->expire($key, $timestamp - (int)round(self::FloatTime()));
    }

    public function keys($pattern) {
        $result = [];
        foreach ($this->_getdb(true) as $key => $_) {
            $key = "$key";
            if (fnmatch($pattern, $key)) {
                $result[] = $key;
            }
        }
        return $result;
    }

    public function migrate($host, $port, $key, $destination_db, $timeout, ...$opts) {
        throw new NotImplementedException;
    }

    public function move($key, $db) {
        $key = "$key";
        $index = self::IntArg($db, 'DB index');
        if ($index == $this->_index) {
            throw self::ErrorReply('ERR source and destination objects are the same');
        }
        $dbs = &$this->_getdbs();
        $db = &$this->_getdb();
        $obj = $this->_getobj($key);
        if (!$obj || isset($dbs[$index][$key])) {
            return 0;
        } else {
            $dbs[$index][$key] = $obj;
            unset($db[$key]);
            return 1;
        }
    }

    public function object($subcommand, ...$args) {
        return $this->_subcommand(__FUNCTION__, $subcommand, $args);
    }

    private function _object_refcount($key) {
        $obj = $this->_getobj($key);
        return $obj ? 1 : self::nil;
    }

    private function _object_encoding($key) {
        $obj = $this->_getobj($key);
        return $obj ? $obj[0] : self::nil;
    }

    private function _object_idletime($key) {
        $obj = $this->_getobj($key);
        return $obj ? 1 : self::nil;
    }

    private function _object_freq($key) {
        $obj = $this->_getobj($key);
        if ($obj) {
            throw self::ErrorReply('ERR An LFU maxmemory policy is not selected.');
        }
        return self::nil;
    }

    public function persist($key) {
        $obj = &$this->_getobj($key);
        if ($obj && $obj[2] != -1) {
            $obj[2] = -1;
            return 1;
        }
        return 0;
    }

    public function pexpire($key, $milliseconds) {
        $milliseconds = self::ParseInt($milliseconds);
        $obj = &$this->_getobj($key);
        if (!$obj) {
            return 0;
        } else {
            $obj[2] = self::FloatTime() + $milliseconds / 1000;
            return 1;
        }
    }

    public function pexpireat($key, $milliseconds_timestamp) {
        $milliseconds_timestamp = self::ParseInt($milliseconds_timestamp);
        return $this->pexpire($key, $milliseconds_timestamp - (int)round(self::FloatTime()*1000));
    }

    public function pttl($key) {
        $obj = $this->_getobj($key);
        if (!$obj) {
            return -2;
        } elseif ($obj[2] < 0) {
            return $obj[2];
        } else {
            return (int)round(($obj[2] - self::FloatTime())*1000);
        }
    }

    public function randomkey() {
        $db = $this->_getdb();
        return $db ? array_rand($db) : self::nil;
    }

    public function rename($key, $newkey) {
        $db = &$this->_getdb();
        $db["$newkey"] = $this->_getobj($key, true);
        unset($db["$key"]);
        return self::OK;
    }

    public function renamenx($key, $newkey) {
        $obj = $this->_getobj($key, true);
        $db = &$this->_getdb();
        if (isset($db["$newkey"])) {
            return 0;
        } else {
            $db["$newkey"] = $obj;
            unset($db["$key"]);
            return 1;
        }
    }

    public function restore($key, $ttl, $serialized_value, $replace=null) {
        self::AssertToken($replace, 'REPLACE');
        if (!$replace && $this->_getobj($key)) {
            return self::ErrorReply('BUSYKEY Target key name already exists.');
        }

        $data = substr($serialized_value, 0, -10);
        $version = substr($serialized_value, -10, -8);
        $checksum = substr($serialized_value, -8);
        if ($version != "\xff\xff" || $checksum != hash('crc32', $data)) {
            throw self::ErrorReply('DUMP payload version or checksum are wrong');
        }

        $obj = @unserialize($data);
        try {
            self::ValidateObj($obj);
        } catch (\Exception $e) {
            throw self::ErrorReply('Bad data format');
        }
        $this->_setval($key, $obj[0], $obj[1], $ttl ? $ttl/1000 : null);
        return self::OK;
    }

    private function _scan($elements, $pairs, $args) {
        $opts = self::ParseOpts('[MATCH pattern] [COUNT count]', $args);
        if ($args) {
            throw self::ErrorReply('ERR syntax error');
        }
        $match = $opts['MATCH'];

        $result = [];
        foreach ($elements as $k => $v) {
            $k = (string)$k;
            if (!$match || fnmatch($match, $k)) {
                $result[] = $k;
                if ($pairs) {
                    $result[] = $v;
                }
            }
        }
        return [0, $result];
    }

    public function scan($cursor, ...$args) {
        return $this->_scan($this->_getdb(true), false, $args);
    }

    private static function SortSplit($pattern) {
        // [key, key suffix if * is present, hash field if -> is present]
        if (!preg_match('/^(.*?)(\*(.*?))?(?:->(.+))?$/', $pattern, $m)) {
            throw new \Exception('Unexpected regex failure');
        }
        return [$m[1], $m[2] ?? null ? $m[3] : null, $m[4] ?? null];
    }

    public function sort($key, ...$args) {
        $spec = '[BY pattern] [LIMIT offset count] [GET pattern] [ASC|DESC] [ALPHA] [STORE destination]';
        $opts = self::ParseOpts($spec, $args);
        if ($args) {
            throw self::ErrorReply('ERR syntax error');
        }

        // initializing
        $obj = $this->_getobj($key);
        if (!$obj) {
            $values = [];
        } else {
            switch ($obj[0]) {
                case 'list': $values = $obj[1];                break;
                case 'set':  $values = $obj[1]; sort($values); break; // pre-sort
                case 'zset': $values = array_keys($obj[1]);    break;
                default:     throw self::ErrorReply('WRONGTYPE Operation against a key holding the wrong kind of value');
            }
        }

        // sorting
        $order = $opts['ASC|DESC'] == 'DESC' ? SORT_DESC : SORT_ASC;
        $flags = $opts['ALPHA'] ? SORT_STRING : SORT_NUMERIC;
        if ($opts['BY']) {
            list($kp, $ks, $field) = self::SortSplit($opts['BY']);
            $weights = [];
            foreach ($values as $idx => $value) {
                $by = $kp . (isset($ks) ? $value.$ks : '');
                if (isset($field)) {
                    $weights[] = $this->_weakhget($by, $field);
                } else {
                    $weights[] = $this->_weakget($by);
                }
            }

            array_multisort(
                $weights, $order, $flags,
                array_keys($values), $order, // stable sort
                $values
            );
        } else {
            array_multisort(
                $values, $order, $flags,
                array_keys($values), $order // stable sort
            );
        }

        // slicing
        if ($opts['LIMIT']) {
            list($offset, $count) = $opts['LIMIT'];
            $values = array_slice($values, $offset, $count);
        }

        // retrieving
        if ($opts['GET']) {
            $gets = [];
            foreach ([$opts['GET']] as $get) { // FIXME: multiple GET clauses
                if ($get == '#') {
                    $gets[] = [null, null, null];
                } else {
                    $gets[] = self::SortSplit($get);
                }
            }

            $got = [];
            foreach ($values as $value) {
                foreach ($gets as list($kp, $ks, $field)) {
                    if (!isset($kp)) {
                        $got[] = $value;
                    } else {
                        $get = $kp . (isset($ks) ? $value.$ks : '');
                        if ($field) {
                            $got[] = $this->_weakhget($get, $field);
                        } else {
                            $got[] = $this->_weakget($get);
                        }
                    }
                }
            }
            $values = $got;
        }

        // storing / replying
        if ($opts['STORE']) {
            $this->_setval($opts['STORE'], 'list', $values);
            return count($values);
        } else {
            return $values;
        }
    }

    public function touch(...$keys) {
        return $this->exists(...$keys);
    }

    public function ttl($key) {
        $obj = $this->_getobj($key);
        if (!$obj) {
            return -2;
        } elseif ($obj[2] < 0) {
            return $obj[2];
        } else {
            return (int)round($obj[2] - self::FloatTime());
        }
    }

    public function type($key) {
        $obj = $this->_getobj($key);
        return $obj ? $obj[0] : 'none';
    }

    public function unlink(...$keys) {
        return $this->del(...$keys);
    }

    public function wait($numslaves, $timeout) {
        throw new NotImplementedException;
    }


    ///////////////////////////////////////////////////////////////////////////
    // Lists

    private function &_getlist($key) {
        return $this->_getval($key, 'list', []);
    }

    public function blpop(...$keys) {
        if (count($keys) < 2) {
            throw self::ErrorReply("ERR wrong number of arguments for 'blpop' command");
        }
        $timeout = array_pop($keys);
        foreach ($keys as $key) {
            $list = &$this->_getlist($key);
            throw new NotImplementedException;
        }
    }

    public function brpop(...$keys) {
        if (count($keys) < 2) {
            throw self::ErrorReply("ERR wrong number of arguments for 'brpop' command");
        }
        $timeout = array_pop($keys);
        foreach ($keys as $key) {
            $list = &$this->_getlist($key);
            throw new NotImplementedException;
        }
    }

    public function brpoplpush($source, $destination, $timeout) {
        $slist = &$this->_getlist($source);
        $dlist = &$this->_getlist($destination);
        throw new NotImplementedException;
    }

    public function lindex($key, $index) {
        $list = $this->_getlist($key);
        if ($index < 0) {
            $index += count($list);
        }
        return $list[$index] ?? self::nil;
    }

    public function linsert($key, $before_after, $pivot, $value) {
        $before_after = self::ParseChoice($before_after, ['BEFORE', 'AFTER']);
        $list = &$this->_getlist($key);
        $index = array_search($pivot, $list);
        if ($index === false) {
            return -1;
        } else {
            $index += $before_after == 'AFTER';
            array_splice($list, $index, 0, ["$value"]);
            return count($list);
        }
    }

    public function llen($key) {
        return count($this->_getlist($key));
    }

    public function lpop($key) {
        return array_shift($this->_getlist($key)) ?? self::nil;
    }

    public function lpush($key, ...$values) {
        $list = &$this->_getlist($key);
        $values = array_reverse($values);
        return array_unshift($list, ...array_map('self::ToString', $values));
    }

    // contrary to the doc, LPUSHX works with multiple values
    public function lpushx($key, ...$values) {
        return $this->_getlist($key) ? $this->lpush($key, ...$values) : 0;
    }

    public function lrange($key, $start, $end) {
        return self::Slice($this->_getlist($key), $start, $end);
    }

    public function lrem($key, $count, $value) {
        $list = &$this->_getlist($key);
        $value = "$value";
        $llen = count($list);
        $removed = 0;
        if ($count > 0) {
            for ($i = 0; $i < $llen && $removed < $count; $i++) {
                if ($list[$i] == $value) {
                    array_splice($list, $i, 1);
                    $removed++;
                    $i--;
                }
            }
        } else {
            $count = $count ? -$count : PHP_INT_MAX;
            for ($i = $llen-1; $i >= 0 && $removed < $count; $i--) {
                if ($list[$i] == $value) {
                    array_splice($list, $i, 1);
                    $removed++;
                }
            }
        }
        return $removed;
    }

    public function lset($key, $index, $value) {
        $list = &$this->_getlist($key);
        if (!$list) {
            throw self::ErrorReply('ERR no such key');
        }
        $index = self::ParseInt($index);
        if ($index < 0) {
            $index += count($list);
        }
        if ($index < 0 || $index >= count($list)) {
            throw self::ErrorReply('ERR index out of range');
        }
        $list[$index] = "$value";
        return self::OK;
    }

    public function ltrim($key, $start, $stop) {
        $list = &$this->_getlist($key);
        $this->_setval($key, 'list', self::Slice($list, $start, $stop));
        return self::OK;
    }

    public function rpop($key) {
        return array_pop($this->_getlist($key)) ?? self::nil;
    }

    public function rpoplpush($source, $destination) {
        $list = &$this->_getlist($destination); // validate type
        $value = $this->rpop($source);
        if ($value !== self::nil) {
            array_unshift($list, self::ToString($value));
        }
        return $value;
    }

    public function rpush($key, ...$values) {
        $list = &$this->_getlist($key);
        return array_push($list, ...array_map('self::ToString', $values));
    }

    // contrary to the doc, RPUSHX works with multiple values
    public function rpushx($key, ...$values) {
        return $this->_getlist($key) ? $this->rpush($key, ...$values) : 0;
    }


    ///////////////////////////////////////////////////////////////////////////////
    // Pub/Sub (API reference only, will not implement)

    public function psubscribe(...$patterns) {
        throw new NotImplementedException;
    }

    public function pubsub($subcommand, ...$arguments) {
        throw new NotImplementedException;
    }

    public function publish($channel, $message) {
        throw new NotImplementedException;
    }

    public function punsubscribe(...$patterns) {
        throw new NotImplementedException;
    }

    public function subscribe(...$channels) {
        throw new NotImplementedException;
    }

    public function unsubscribe(...$channels) {
        throw new NotImplementedException;
    }


    ///////////////////////////////////////////////////////////////////////////
    // Scripting

    /**
     * Initialize Lua state with Redis-provided functionality.
     *
     * Note: struct, cjson, cmsgpack, and bitop modules are not provided by
     * MockRedis directly, but you may provide them via your Lua extension.
     *
     * @todo Mock struct, cjson, cmsgpack, and bitop modules when missing.
     */
    const LUA_INIT = <<<'LUA_INIT'
        local os_time = os.time

        -- unsuported libs
        package = nil
        os = nil

        -- unsupported functions
        loadfile = nil
        dofile = nil

        local replicate = false

        redis = {
            LOG_DEBUG   = 0,
            LOG_VERBOSE = 1,
            LOG_NOTICE  = 2,
            LOG_WARNING = 3,

            REPL_NONE   = 0,
            REPL_AOF    = 1,
            REPL_SLAVE  = 2,
            REPL_ALL    = 3,
        }

        function redis.log(...)
            local loglevel, message = ...
            loglevel = tonumber(loglevel)
            if select('#', ...) < 2 then
                error('redis.log() requires two arguments or more.')
            elseif loglevel == nil then
                error('First argument must be a number (log level).')
            elseif loglevel < 0 or loglevel > 3 then
                error('Invalid debug level.')
            end
        end

        function redis.sha1hex(...)
            local string = ...
            if select('#', ...) ~= 1 then
                error('wrong number of arguments')
            end
            if type(string) == 'number' then
                string = tostring(string)
            elseif type(string) ~= 'string' then
                string = ''
            end
            return redis.call('mockredis', 'sha1hex', string)
        end

        function redis.error_reply(error_string)
            return {err = error_string}
        end

        function redis.status_reply(status_string)
            return {ok = status_string}
        end

        function redis.replicate_commands()
            -- FIXME: should check if a write has occurred
            replicate = true
            math.randomseed(os_time())
            return true
        end

        function redis.set_repl(...)
            local mode = ...
            if not replicate then
                error('You can set the replication behavior only after turning on single commands replication with redis.replicate_commands().')
            elseif select('#', ...) ~= 1 then
                error('redis.set_repl() requires two arguments.')
            elseif mode ~= math.floor(mode) or mode < 0 or mode > 3 then
                error('Invalid replication flags. Use REPL_AOF, REPL_SLAVE, REPL_ALL or REPL_NONE.')
            end
        end

        function redis.breakpoint()
            return false
        end

        function redis.debug()
        end

        local G = _G
        __mockredis_fenv = setmetatable({}, {
            __newindex = function(t, k, v)
                error("Script attempted to create global variable '"..tostring(k).."'", 2)
            end,
            __index = function(t, k)
                local v = G[k]
                if v ~= nil then
                    return v
                end
                error("Script attempted to access unexisting global variable '"..tostring(k).."'", 2)
            end,
        })
LUA_INIT;

    const NOSCRIPT = [
        'auth'         => true, 'blpop'        => true, 'brpop'        => true,
        'brpoplpush'   => true, 'client'       => true, 'debug'        => true,
        'discard'      => true, 'eval'         => true, 'evalsha'      => true,
        'exec'         => true, 'latency'      => true, 'module'       => true,
        'monitor'      => true, 'multi'        => true, 'psubscribe'   => true,
        'psync'        => true, 'punsubscribe' => true, 'replconf'     => true,
        'role'         => true, 'save'         => true, 'script'       => true,
        'slaveof'      => true, 'subscribe'    => true, 'sync'         => true,
        'unsubscribe'  => true, 'unwatch'      => true, 'wait'         => true,
        'watch'        => true,
    ];

    const SORT_FOR_SCRIPT = [
        'hkeys'        => true, 'hvals'        => true, 'keys'         => true,
        'sdiff'        => true, 'sinter'       => true, 'smembers'     => true,
        'sunion'       => true,
    ];

    /** Only public as a callback, not intended to be called directly. */
    public function _lua_call(...$args) {
        if (!$args) {
            return self::ErrorReply('Please specify at least one argument for redis.call()');
        }
        $name = strtolower(array_shift($args));
        if (!method_exists($this, $name)) {
            return self::ErrorReply('Unknown Redis command called from Lua script');
        }
        if (isset(self::NOSCRIPT[$name])) {
            return self::ErrorReply('This Redis command is not allowed from scripts');
        }
        // TODO: add tracking for CMD_RANDOM / CMD_WRITE to reject
        //       nondeterministic writes.
        try {
            $result = $this->$name(...$args);
        } catch (\Throwable $e) {
            // TODO: this should only catch MockRedisException and some PHP
            //       exceptions (e.g. too few arguments).  it's preferable for
            //       other errors to propagate.
            return $e;
        }
        if (isset(self::SORT_FOR_SCRIPT[$name])) {
            sort($result);
        }
        return $result;
    }

    private function _getscripting() {
        if (!$this->_scripting) {
            if (isset($this->_scriptingClass)) {
                $scriptingClass = $this->_scriptingClass;
            } elseif (extension_loaded('lua')) {
                $scriptingClass = PhpLuaMockRedisScripting::class;
            } else {
                throw self::ErrorReply('ERR This instance has scripting support disabled (set '.__CLASS__.'::$scriptingClass)');
            }
            $this->_scripting = new $scriptingClass(self::LUA_INIT, [$this, '_lua_call']);
        }
        return $this->_scripting;
    }

    private static function ToResult(&$value) {
        if (!isset($value) || $value === false) {
            $value = self::nil;
        } elseif ($value === true || is_float($value) || is_int($value)) {
            $value = (int)$value;
        } elseif (is_string($value)) {
            // noop
        } elseif (is_array($value)) {
            if (isset($value['err'])) {
                $value = self::ErrorReply($value['err']);
            } elseif (isset($value['ok'])) {
                $value = self::StatusReply($value['ok']);
            } else {
                // only numeric arrays
                if ($value != array_values($value)) {
                    throw new \Exception('Malformed array from scripting');
                }
                foreach ($value as &$v) {
                    self::ToResult($v);
                }
            }
        } else {
            // only expect the above types
            throw new \Exception('Malformed value from scripting');
        }
    }

    private function _eval($sha1, $numkeys, $args) {
        if ($numkeys < 0) {
            throw self::ErrorReply("ERR Number of keys can't be negative");
        }
        if ($numkeys > count($args)) {
            throw self::ErrorReply("ERR Number of keys can't be greater than number of args");
        }
        $keys = array_slice($args, 0, $numkeys);
        $argv = array_slice($args, $numkeys);

        $scripting = $this->_getscripting();
        $lua = "math.randomseed(0); return f_$sha1()";
        $index = $this->_index; // SELECT calls only affect this script
        list($success, $result) = $scripting->scriptDo($lua, $keys, $argv);
        $this->_index = $index;
        if ($success) {
            self::ToResult($result);
            return $result;
        } else {
            if ($result instanceof self::$exceptionClass) {
                throw $result;
            } else {
                throw self::ErrorReply("ERR Error running script (call to f_$sha1): $result");
            }
        }
    }

    public function eval($script, $numkeys, ...$args) {
        return $this->_eval($this->_script_load($script), $numkeys, $args);
    }

    public function evalsha($sha1, $numkeys, ...$args) {
        $sha1 = strtolower($sha1);
        list($exists) = $this->_script_exists($sha1);
        if (!$exists) {
            throw self::ErrorReply('NOSCRIPT No matching script. Please use EVAL.');
        }
        return $this->_eval($sha1, $numkeys, $args);
    }

    public function script($subcommand, ...$args) {
        return $this->_subcommand(__FUNCTION__, $subcommand, $args);
    }

    private function _script_debug($mode) {
        throw new NotImplementedException;
    }

    private function _script_exists(...$sha1s) {
        $scripting = $this->_getscripting();
        $results = [];
        foreach ($sha1s as $sha1) {
            $sha1 = strtolower($sha1);
            if (!preg_match('/^[0-9a-f]{40}$/', $sha1)) {
                $results[] = 0;
            } else {
                $lua = "return f_$sha1 ~= nil";
                list($success, $result) = $scripting->scriptDo($lua);
                if (!$success) {
                    throw new \Exception("Unexpected failure [$result] in trivial lua: $lua");
                }
                $results[] = $result ? 1 : 0;
            }
        }
        return $results;
    }

    private function _script_flush() {
        // drop the scripting instance and reinsatiate on demand
        $this->_scripting = null;
        return self::OK;
    }

    private function _script_kill() {
        throw new NotImplementedException;
    }

    private function _script_load($script) {
        $sha1 = sha1($script);
        $lua = "f_$sha1 = setfenv(function() $script\nend, __mockredis_fenv)";
        list($success, $result) = $this->_getscripting()->scriptDo($lua);
        if (!$success) {
            throw self::ErrorReply("ERR Error compiling script (new function): $result");
        }
        return $sha1;
    }

    /** MockRedis-specific commands, currently just for scripting internals. */
    public function mockredis($subcommand, ...$args) {
        return $this->_subcommand(__FUNCTION__, $subcommand, $args);
    }

    private function _mockredis_sha1hex($string) {
        return sha1($string);
    }


    ///////////////////////////////////////////////////////////////////////////
    // Server

    public function bgrewriteaof() {
        return $this->save();
    }

    public function bgsave() {
        return $this->save();
    }

    public function client($subcommand, ...$args) {
        return $this->_subcommand(__FUNCTION__, $subcommand, $args);
    }

    public function command($subcommand=null, ...$args) {
        return $this->_subcommand(__FUNCTION__, $subcommand, $args);
    }

    private function _methodinfo($method) {
        if (!$method->isPublic() || $method->isStatic()) {
            return self::nil;
        }
        if ($method->name[0] == '_') {
            return self::nil;
        }
        $params = $method->getNumberOfParameters();
        $required = $method->getNumberOfRequiredParameters();
        $arity = 1 + $required;
        if ($params != $required) {
            $arity = -$arity;
        }
        $flags = [];
        if (isset(self::NOSCRIPT[$method->name])) {
            $flags[] = 'noscript';
        } elseif (isset(self::SORT_FOR_SCRIPT[$method->name])) {
            $flags[] = 'sort_for_script';
        }
        return [
            $method->name,  // command name
            $arity,         // command arity specification
            $flags,         // command flags
            0,              // position of first key in argument list
            0,              // position of last key in argument list
            0,              // step count for locating repeating keys
        ];
    }

    private function _command_() {
        $object = new \ReflectionObject($this);
        $commands = [];
        foreach ($object->getMethods() as $method) {
            $info = $this->_methodinfo($method);
            if ($info) {
                $commands[] = $info;
            }
        }
        return $commands;
    }

    private function _command_count() {
        return count($this->_command_());
    }

    private function _command_info(...$command_names) {
        $object = new \ReflectionObject($this);
        $commands = [];
        foreach ($command_names as $command_name) {
            $command_name = strtolower($command_name);
            if ($object->hasMethod($command_name)) {
                $method = $object->getMethod($command_name);
                $commands[] = $this->_methodinfo($method);
            } else {
                $commands[] = self::nil;
            }
        }
        return $command;
    }

    public function config($subcommand, ...$args) {
        return $this->_subcommand(__FUNCTION__, $subcommand, $args);
    }

    private function _config_set($parameter, $value) {
        // TODO: implement some subset of configs
        return self::OK;
    }

    public function dbsize() {
        return count($this->_getdb(true));
    }

    public function debug($subcommand, ...$args) {
        return $this->_subcommand(__FUNCTION__, $subcommand, $args);
    }

    private function _debug_reload() {
        $this->_persistence->save($this->_server, $this->_dbs, self::FloatTime());
        $dbs = &$this->_persistence->load($this->_server, self::FloatTime());
        self::ValidateDbs($dbs);
        $this->_dbs = &$dbs;
        return self::OK;
    }

    private function _debug_loadaof() {
        return $this->_debug_reload();
    }

    private function _debug_sleep($seconds) {
        usleep((int)($seconds * 1000000));
        return self::OK;
    }

    private function _debug_set_active_expire($active_expire) {
        // noop, background expiration is never supported
        return self::OK;
    }

    private function _debug_populate($count, $prefix='key', $size=null) {
        $count = self::ParseInt($count);
        if (isset($size)) {
            $size = self::ParseInt($size);
        }
        for ($i = 0; $i < $count; $i++) {
            $value = "value:$i";
            if (isset($size)) {
                $value = str_pad(substr($value, 0, $size), $size, "\x00");
            }
            $this->set("$prefix:$i", $value);
        }
        return self::OK;
    }

    private function _debug_digest() {
        $dbs = $this->_getdbs(true);
        if ($dbs) {
            // FIXME: this doesn't treat unordered structures correctly (but
            //        it seems to be good enough for unit tests)
            return sha1(var_export($dbs, true));
        } else {
            return '0000000000000000000000000000000000000000';
        }
    }

    public function flushall($async=null) {
        self::AssertToken($async, 'ASYNC'); // ignore ASYNC flag
        $dbs = &$this->_getdbs();
        foreach ($dbs as $index => $_) {
            unset($dbs[$index]);
        }
        $dbs[$this->_index] = [];
        return self::OK;
    }

    public function flushdb($async=null) {
        self::AssertToken($async, 'ASYNC'); // ignore ASYNC flag
        $this->_getdbs()[$this->_index] = [];
        return self::OK;
    }

    public function info($section=null) {
        $used_memory = memory_get_usage();

        $info = <<<INFO
# Server
redis_version:3.2.9
redis_git_sha1:00000000

# Memory
used_memory:$used_memory

INFO;

        return str_replace("\n", "\r\n", $info);
    }

    public function lastsave() {
        return (int)$this->_persistence->lastsave();
    }

    public function latency($subcommand, ...$args) {
        return $this->_subcommand(__FUNCTION__, $subcommand, $args);
    }

    public function memory($subcommand, ...$args) {
        return $this->_subcommand(__FUNCTION__, $subcommand, $args);
    }

    public function module($subcommand, ...$args) {
        return $this->_subcommand(__FUNCTION__, $subcommand, $args);
    }

    public function monitor() {
        throw new NotImplementedException;
    }

    public function psync() {
        throw new NotImplementedException;
    }

    public function replconf($ack, $offset) {
        throw new NotImplementedException;
    }

    public function role() {
        throw new NotImplementedException;
    }

    public function save() {
        $this->_persistence->save($this->_server, $this->_dbs, self::FloatTime());
        return self::OK;
    }

    public function shutdown($nosave_save=null) {
        throw new NotImplementedException;
    }

    public function slaveof($host, $port) {
        throw new NotImplementedException;
    }

    public function slowlog($subcommand, ...$args) {
        return $this->_subcommand(__FUNCTION__, $subcommand, $args);
    }

    public function sync() {
        throw new NotImplementedException;
    }

    public function time() {
        $time = self::FloatTime();
        return [
            (string)(int)$time,
            (string)(int)(1000*fmod($time, 1)),
        ];
    }


    ///////////////////////////////////////////////////////////////////////////
    // Sets

    private function &_getset($key) {
        return $this->_getval($key, 'set', []);
    }

    private function _getsets($keys, $command) {
        if (!count($keys)) {
            throw self::ErrorReply("ERR wrong number of arguments for '$command'");
        }
        return array_map([$this, '_getset'], $keys);
    }

    public function sadd($key, ...$members) {
        if (!count($members)) {
            throw self::ErrorReply("ERR wrong number of arguments for 'sadd' command");
        }

        $set = &$this->_getset($key);
        $count = 0;
        foreach ($members as $member) {
            $member = "$member";
            $count += !isset($set[$member]);
            $set[$member] = $member;
        }
        return $count;
    }

    public function scard($key) {
        return count($this->_getset($key));
    }

    private function _sdiff($keys, $command) {
        $sets = $this->_getsets($keys, $command);
        return count($sets) > 1 ? array_diff(...$sets) : $sets[0];
    }

    public function sdiff(...$keys) {
        return array_keys($this->_sdiff($keys, __FUNCTION__));
    }

    public function sdiffstore($destination, ...$keys) {
        $set = $this->_sdiff($keys, __FUNCTION__);
        $this->_setval($destination, 'set', $set);
        return count($set);
    }

    private function _sinter($keys, $command) {
        $sets = $this->_getsets($keys, $command);
        return count($sets) > 1 ? array_intersect(...$sets) : $sets[0];
    }

    public function sinter(...$keys) {
        return array_keys($this->_sinter($keys, __FUNCTION__));
    }

    public function sinterstore($destination, ...$keys) {
        $set = $this->_sinter($keys, __FUNCTION__);
        $this->_setval($destination, 'set', $set);
        return count($set);
    }

    public function sismember($key, $member) {
        return (int)isset($this->_getset($key)["$member"]);
    }

    public function smembers($key) {
        return array_keys($this->_getset($key));
    }

    public function smove($source, $destination, $member) {
        $set = &$this->_getset($destination); // validate type
        $removed = $this->srem($source, $member);
        if ($removed) {
            $member = "$member";
            $set[$member] = $member;
        }
        return $removed;
    }

    public function spop($key, $count=null) {
        $set = &$this->_getset($key);
        if (!$set) {
            return self::nil;
        } elseif (!isset($count)) {
            return array_pop($set);
        } else {
            $count = self::ParseInt($count);
            $result = [];
            for ($i = 0; $i < $count && count($set); $i++) { 
                $result[] = array_pop($set);
            }
            return $result;
        }
    }

    public function srandmember($key, $count=null) {
        $set = $this->_getset($key);
        if (!$set) {
            return self::nil;
        } elseif (!isset($count)) {
            return array_rand($set);
        } elseif (!$count || !$set) {
            return [];
        } elseif ($count == 1) {
            return [array_rand($set)];
        } elseif ($count > 0) {
            return array_rand($set, min($count, count($set)));
        } else {
            $count = -$count;
            $sets = array_fill(0, (int)($count/count($set)), array_keys($set));
            if ($count % count($set)) {
                $sets[] = array_rand($set, $count % count($set));
            }
            return array_merge(...$sets);
        }
    }

    public function srem($key, ...$members) {
        return self::Remove($this->_getset($key), $members);
    }

    public function sscan($key, $cursor, ...$args) {
        return $this->_scan($this->_getset($key), false, $args);
    }

    private function _sunion($keys, $command) {
        $sets = $this->_getsets($keys, $command);
        return count($sets) > 1 ? array_replace(...$sets) : $sets[0];
    }

    public function sunion(...$keys) {
        return array_keys($this->_sunion($keys, __FUNCTION__));
    }

    public function sunionstore($destination, ...$keys) {
        $set = $this->_sunion($keys, __FUNCTION__);
        $this->_setval($destination, 'set', $set);
        return count($set);
    }


    ///////////////////////////////////////////////////////////////////////////
    // Sorted Sets

    private function &_getzset($key) {
        return $this->_getval($key, 'zset', []);
    }

    private static function ZSort(&$zset) {
        $keys = array_keys($zset);
        array_multisort($zset, SORT_ASC, $keys, SORT_ASC, SORT_STRING);
        $zset = array_combine($keys, $zset);
    }

    private static function WithScores($zset, $members) {
        $results = [];
        foreach ($members as $member) {
            $results[] = "$member";
            $results[] = "$zset[$member]";
        }
        return $results;
    }

    public function zadd($key, ...$score_member) {
        $zset = &$this->_getzset($key);
        $opts = self::ParseOpts('[NX|XX] [CH] [INCR]', $score_member);
        if (!$score_member || ($opts['INCR'] && count($score_member) > 2)) {
            throw self::ErrorReply("ERR wrong number of arguments for 'zadd' command");
        }

        // finish parsing args before touching zset
        $scores = [];
        foreach (self::Tuples($score_member, 2, false) as $tuple) {
            list($score, $member) = $tuple;
            $member = self::ToString($member);
            $score = self::ParseFloat($score);
            $scores[$member] = $score;
        }

        $count = 0;
        foreach ($scores as $member => $score) {
            $old = $zset[$member] ?? null;
            if (!$opts['NX|XX'] || isset($old) == $opts['XX']) {
                $count += $opts['CH'] ? $old != $score : !isset($old);
                $score += $opts['INCR'] ? $old : 0.0;
                $zset[$member] = $score == $score ? $score : 0.0;
            }
        }
        self::ZSort($zset);
        return $opts['INCR'] ? (string)$zset[$member] : $count;
    }

    public function zcard($key) {
        return count($this->_getzset($key));
    }

    public function zcount($key, $min, $max) {
        return count($this->zrangebyscore($key, $min, $max));
    }

    public function zincrby($key, $increment, $member) {
        $zset = &$this->_getzset($key);
        $increment = self::ParseFloat($increment);
        $member = self::ToString($member);
        $score = ($zset[$member] ?? 0.0) + $increment;
        if (is_nan($score)) {
            throw self::ErrorReply('ERR resulting score is not a number (NaN)');
        }
        $zset[$member] = $score;
        self::ZSort($zset);
        return (string)$zset[$member];
    }

    private function _zxstore($destination, $numkeys, $args, $union) {
        // TODO: this has lots of messy custom parsing
        $numkeys = self::IntArg($numkeys);
        if ($numkeys < 1) {
            throw self::ErrorReply('ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE');
        }
        if (count($args) < $numkeys) {
            throw self::ErrorReply('ERR syntax error');
        }
        $keys = array_slice($args, 0, $numkeys);
        $args = array_slice($args, $numkeys);
        if ($args && strtoupper($args[0]) == 'WEIGHTS') {
            $weights = array_slice($args, 1, $numkeys);
            if (count($weights) != $numkeys) {
                throw self::ErrorReply('ERR syntax error');
            }
            $args = array_slice($args, 1+$numkeys);
        } else {
            $weights = array_fill(0.0, $numkeys, 1);
        }
        // FIXME: handle AGGREGATE before WEIGHTS
        $aggregate = 'array_sum';
        if (count($args) >= 2 && strtoupper($args[0]) == 'AGGREGATE') {
            $aggregate = $args[1];
            $args = array_slice($args, 2);
            switch (strtoupper($aggregate)) {
                case 'SUM': $aggregate = 'array_sum'; break;
                case 'MIN': $aggregate = 'min';       break;
                case 'MAX': $aggregate = 'max';       break;
                default:    throw self::ErrorReply('ERR syntax error');
            }
        }
        if ($args) {
            throw self::ErrorReply('ERR syntax error');
        }

        // transpose keys and members, with weight scores
        $members = [];
        for ($i = 0; $i < count($keys); $i++) {
            $weight = self::ParseFloat($weights[$i], $name='weight');
            $obj = $this->_getobj($keys[$i]);
            if (!$obj) {
                // noop
            } elseif ($obj[0] == 'zset') {
                foreach ($obj[1] as $member => $score) {
                    // weight the score, replace nans with 0s
                    $score *= $weight;
                    $members[$member][] = $score == $score ? $score : 0.0;
                }
            } elseif ($obj[0] == 'set') {
                foreach ($obj[1] as $member) {
                    $members[$member][] = $weight;
                }
            } else {
                throw self::ErrorReply('WRONGTYPE Operation against a key holding the wrong kind of value');
            }
        }

        // aggregate by member
        $zset = [];
        foreach ($members as $member => $scores) {
            if ($union || count($scores) == $numkeys) {
                // aggregate the scores, replace nans with 0s
                $score = $aggregate($scores);
                $zset[$member] = $score == $score ? $score : 0.0;
            }
        }

        self::ZSort($zset);
        $this->_setval($destination, 'zset', $zset);
        return count($zset);
    }

    public function zinterstore($destination, $numkeys, ...$args) {
        return $this->_zxstore($destination, $numkeys, $args, false);
    }

    public function zlexcount($key, $min, $max) {
        return count($this->zrangebylex($key, $min, $max));
    }

    public function zrange($key, $start, $stop, $withscores=null) {
        $zset = $this->_getzset($key);
        $members = self::Slice(array_keys($zset), $start, $stop);
        if (self::AssertToken($withscores, 'WITHSCORES')) {
            return self::WithScores($zset, $members);
        } else {
            return array_map('strval', $members);
        }
    }

    private function _zrangebylex($key, $min, $max, $args, $rev) {
        list($filter, $min, $max) = self::MinMaxLex($min, $max);
        $opts = self::ParseOpts('[LIMIT offset count]', $args);
        if ($args) {
            throw self::ErrorReply('ERR syntax error');
        }
        $zset = $this->_getzset($key);

        $members = [];
        foreach ($zset as $member => $score) {
            if ($filter($min, $max, $member)) {
                $members[] = "$member";
            }
        }
        if ($rev) {
            $members = array_reverse($members);
        }

        if ($opts['LIMIT']) {
            list($offset, $count) = $opts['LIMIT'];
            $members = array_slice($members, $offset, $count);
        }

        return $members;
    }

    public function zrangebylex($key, $min, $max, ...$args) {
        return $this->_zrangebylex($key, $min, $max, $args, false);
    }

    private function _zrangebyscore($key, $min, $max, $args, $rev) {
        list($filter, $min, $max) = self::MinMaxScore($min, $max);
        $opts = self::ParseOpts('[WITHSCORES] [LIMIT offset count]', $args);
        if ($args) {
            throw self::ErrorReply('ERR syntax error');
        }
        $zset = $this->_getzset($key);

        $members = [];
        foreach ($zset as $member => $score) {
            if ($filter($min, $max, $score)) {
                $members[] = "$member";
            }
        }
        if ($rev) {
            $members = array_reverse($members);
        }

        if ($opts['LIMIT']) {
            list($offset, $count) = $opts['LIMIT'];
            $members = array_slice($members, $offset, $count);
        }

        if ($opts['WITHSCORES']) {
            return self::WithScores($zset, $members);
        } else {
            return $members;
        }
    }

    public function zrangebyscore($key, $min, $max, ...$args) {
        return $this->_zrangebyscore($key, $min, $max, $args, false);
    }

    public function zrank($key, $member) {
        $zset = $this->_getzset($key);
        return array_flip(array_keys($zset))["$member"] ?? self::nil;
    }

    public function zrem($key, ...$members) {
        return self::Remove($this->_getzset($key), $members);
    }

    public function zremrangebylex($key, $min, $max) {
        return $this->zrem($key, ...$this->zrangebylex($key, $min, $max));
    }

    public function zremrangebyrank($key, $start, $stop) {
        return $this->zrem($key, ...$this->zrange($key, $start, $stop));
    }

    public function zremrangebyscore($key, $min, $max) {
        return $this->zrem($key, ...$this->zrangebyscore($key, $min, $max));
    }

    public function zrevrange($key, $start, $stop, $withscores=null) {
        $zset = $this->_getzset($key);
        $members = self::Slice(array_reverse(array_keys($zset)), $start, $stop);
        if (self::AssertToken($withscores, 'WITHSCORES')) {
            return self::WithScores($zset, $members);
        } else {
            return array_map('strval', $members);
        }
    }

    public function zrevrangebylex($key, $min, $max, ...$args) {
        return $this->_zrangebylex($key, $max, $min, $args, true);
    }

    public function zrevrangebyscore($key, $min, $max, ...$args) {
        return $this->_zrangebyscore($key, $max, $min, $args, true);
    }

    public function zrevrank($key, $member) {
        $zset = $this->_getzset($key);
        return array_flip(array_reverse(array_keys($zset)))["$member"] ?? self::nil;
    }

    public function zscore($key, $member) {
        return $this->_getzset($key)["$member"] ?? self::nil;
    }

    public function zunionstore($destination, $numkeys, ...$args) {
        return $this->_zxstore($destination, $numkeys, $args, true);
    }

    public function zscan($key, $cursor, ...$args) {
        return $this->_scan($this->_getzset($key), true, $args);
    }


    ///////////////////////////////////////////////////////////////////////////
    // Strings

    const BITCOUNT = '0112122312232334122323342334344512232334233434452334344534454556122323342334344523343445344545562334344534454556344545564556566712232334233434452334344534454556233434453445455634454556455656672334344534454556344545564556566734454556455656674556566756676778';
    const BITPOS_1 = '-766555544444444333333333333333322222222222222222222222222222222111111111111111111111111111111111111111111111111111111111111111100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000';
    const BITPOS_0 = '000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001111111111111111111111111111111111111111111111111111111111111111222222222222222222222222222222223333333333333333444444445555667-';

    private function &_getstring($key) {
        return $this->_getval($key, 'string', self::nil);
    }

    private function _weakget($key) {
        $string = $this->_getval($key, 'string', self::nil, false);
        return $string ?? self::nil;
    }

    public function append($key, $value) {
        $string = &$this->_getstring($key);
        $string .= $value;
        return strlen($string);
    }

    public function bitcount($key, $start=null, $end=null) {
        $string = $this->_getstring($key);
        if (isset($start) != isset($end)) {
            throw self::ErrorReply('ERR syntax error');
        }
        list($offset, $length) = self::ParseRange($start, $end, strlen($string));
        $count = 0;
        $len = min($offset+$length, strlen($string));
        for ($i = $offset; $i < $len; $i++) {
            $count += self::BITCOUNT[ord($string[$i])];
        }
        return $count;
    }

    public function bitfield($key, ...$args) {
        $spec = '[GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP|SAT|FAIL]';
        $opts = self::ParseOpts('[EX|PX ttl] [NX|XX]', $args);
        if ($args) {
            throw self::ErrorReply('ERR syntax error');
        }

        $string = &$this->_getstring($key);
        throw new NotImplementedException;
    }

    public function bitop($operation, $destkey, ...$keys) {
        $operation = strtoupper($operation);
        if ($operation == 'NOT') {
            if (count($keys) != 1) {
                throw self::ErrorReply('ERR BITOP NOT must be called with a single source key.');
            }
            $string = ~$this->_getstring($keys[0]);
        } else {
            if (!count($keys)) {
                throw self::ErrorReply("ERR wrong number of arguments for 'bitop' command");
            }
            $strings = [];
            $len = 0;
            foreach ($keys as $key) {
                $strings[] = $string = $this->_getstring($key) ?? '';
                $len = max($len, strlen($string));
            }
            $string = str_pad(array_pop($strings), $len, "\x00");
            switch ($operation) {
                case 'AND':
                    foreach ($strings as $s) {
                        $string &= str_pad($s, $len, "\x00");
                    }
                    break;

                case 'OR':
                    foreach ($strings as $s) {
                        $string |= str_pad($s, $len, "\x00");
                    }
                    break;

                case 'XOR':
                    foreach ($strings as $s) {
                        $string ^= str_pad($s, $len, "\x00");
                    }
                    break;

                default:
                    throw self::ErrorReply('ERR syntax error');
            }
        }
        $this->_setval($destkey, 'string', $string);
        return strlen($string);
    }

    public function bitpos($key, $bit, $start=null, $end=null) {
        $string = $this->_getstring($key);
        $bit = self::ParseBit($bit);
        list($offset, $length) = self::ParseRange($start, $end, strlen($string));
        echo "$offset->$length\n";
        if ($bit) {
            $span = strspn($string, "\x00", $offset, $length);
            if ($span >= $length) {
                return -1;
            }
            $offset += $span;
            return 8*$offset + self::BITPOS_1[ord($string[$offset])];
        } else {
            $span = strspn($string, "\xff", $offset, $length);
            if ($span >= $length) {
                return isset($end) ? -1 : 8*($offset + $length);
            }
            $offset += $span;
            return 8*$offset + self::BITPOS_0[ord($string[$offset])];
        }
    }

    public function decr($key) {
        return $this->decrby($key, 1);
    }

    public function decrby($key, $decrement) {
        return $this->incrby($key, -$decrement);
    }

    public function get($key) {
        return $this->_getstring($key);
    }

    public function getbit($key, $offset) {
        $string = $this->_getstring($key);
        if ($offset < 0 || $offset >= 4294967296) { // 2^32
            throw self::ErrorReply('ERR bit offset is not an integer or out of range');
        }
        $char = (int)($offset / 8);
        $mask = 1 << (7 - $offset % 8);
        if ($char >= strlen($string)) {
            return 0;
        }
        return (ord($string[$char]) & $mask) ? 1 : 0;
    }

    public function getrange($key, $start, $end) {
        $string = $this->_getstring($key);
        list($offset, $length) = self::ParseRange($start, $end, strlen($string));
        if ($offset === false) {
            return '';
        }
        return substr($string, $offset, $length) ?: '';
    }

    public function getset($key, $value) {
        $was = $this->get($key);
        $this->set($key, $value);
        return $was;
    }

    public function incr($key) {
        return $this->incrby($key, 1);
    }

    public function incrby($key, $increment) {
        $value = $this->get($key);
        $value = $value === self::nil ? 0 : self::ParseInt($value, 'value');
        $value += self::ParseInt($increment);
        if (!is_int($value)) {
            throw self::ErrorReply('ERR increment or decrement would overflow');
        }
        $this->set($key, $value);
        return $value;
    }

    public function incrbyfloat($key, $increment) {
        $value = $this->get($key);
        $value = $value === self::nil ? 0.0 : self::ParseFloat($value, 'value');
        $value += self::ParseFloat($increment);
        if (is_infinite($value) || is_nan($value)) {
            throw self::ErrorReply('ERR increment would produce NaN or Infinity');
        }
        $this->set($key, $value);
        return (string)$value;
    }

    public function mget(...$keys) {
        return array_map([$this, '_weakget'], $keys);
    }

    public function mset(...$key_value) {
        foreach (self::Tuples($key_value, 2, __FUNCTION__) as $tuple) {
            list($key, $value) = $tuple;
            $this->set($key, $value);
        }
        return self::OK;
    }

    public function msetnx(...$key_value) {
        $tuples = self::Tuples($key_value, 2, __FUNCTION__);
        foreach ($tuples as $tuple) {
            list($key, $value) = $tuple;
            if ($this->exists($key)) {
                return 0;
            }
        }
        foreach ($tuples as $tuple) {
            list($key, $value) = $tuple;
            $this->setnx($key, $value);
        }
        return 1;
    }

    public function psetex($key, $milliseconds, $value) {
        $milliseconds = self::ParseInt($milliseconds);
        if ($milliseconds <= 0) {
            throw self::ErrorReply('ERR invalid expire time in psetex');
        }
        $this->_setval($key, 'string', "$value", $milliseconds / 1000);
        return self::OK;
    }

    public function set($key, $value, ...$args) {
        $opts = self::ParseOpts('[EX|PX ttl] [NX|XX]', $args);
        if ($args) {
            throw self::ErrorReply('ERR syntax error');
        }
        if ($opts['NX|XX']) {
            $obj = $this->_getobj($key);
            if (isset($obj) != $opts['XX']) {
                return self::nil;
            }
        }
        if ($opts['EX']) {
            $ttl = self::ParseInt($opts['EX']);
        } elseif ($opts['PX']) {
            $ttl = self::ParseInt($opts['PX']) / 1000;
        } else {
            $ttl = null;
        }
        $this->_setval($key, 'string', "$value", $ttl);
        return self::OK;
    }

    public function setbit($key, $offset, $value) {
        $string = &$this->_getstring($key);
        if ($offset < 0 || $offset >= 4294967296) { // 2^32
            throw self::ErrorReply('ERR bit offset is not an integer or out of range');
        }
        $value = self::ParseBit($value);
        $char = (int)($offset / 8);
        $mask = 1 << (7 - $offset % 8);
        $string = str_pad($string, $char+1, "\0");
        $ord = ord($string[$char]);
        $string[$char] = $value ? chr($ord | $mask) : chr($ord & ~$mask);
        return ($ord & $mask) ? 1 : 0;
    }

    public function setex($key, $seconds, $value) {
        $seconds = self::ParseInt($seconds);
        if ($seconds <= 0) {
            throw self::ErrorReply('ERR invalid expire time in setex');
        }
        $this->_setval($key, 'string', "$value", $seconds);
        return self::OK;
    }

    public function setnx($key, $value) {
        return $this->_getobj($key) ? 0 : (int)$this->set($key, $value);
    }

    public function setrange($key, $offset, $value) {
        $string = &$this->_getstring($key);
        $value = "$value";
        $value_len = strlen($value);
        if (!$value_len) {
            return strlen($string);
        }
        if ($offset < 0) {
            throw self::ErrorReply('ERR offset is out of range');
        }
        if ($offset + $value_len > 536870912) { // 512MB
            throw self::ErrorReply('ERR string exceeds maximum allowed size (512MB)');
        }
        $string = str_pad($string, $offset, "\0");
        $string = substr_replace($string, $value, $offset, $value_len);
        return strlen($string);
    }

    public function strlen($key) {
        $string = $this->_getstring($key);
        return isset($string) ? strlen($string) : 0;
    }

    // renamed after redis 2.0
    public function substr($key, $start, $end) {
        return $this->getrange($key, $start, $end);
    }


    ///////////////////////////////////////////////////////////////////////////
    // Transactions (API reference only, not yet implemented)

    public function discard() {
        throw new NotImplementedException;
    }

    public function exec() {
        throw new NotImplementedException;
    }

    public function multi() {
        throw new NotImplementedException;
    }

    public function unwatch() {
        throw new NotImplementedException;
    }

    public function watch(...$keys) {
        throw new NotImplementedException;
    }



    ///////////////////////////////////////////////////////////////////////////////
    // Private

    /** @var string The name for this redis instance, typically "host:port" */
    private $_server;

    /** @var int The currently selected db */
    private $_index;

    /**
     * @var array All values for this server, in the form: [
     *                (int)index => [
     *                    (string)key => [
     *                        (string)type: hash|list|set|string|zset,
     *                        (null|string|array)value,
     *                        (float)expiration,
     *                    ],
     *                    ...,
                      ],
     *                ...,
     *            ]
     */
    private $_dbs;

    /**
     * @var MockRedisPersistence Object to manage storage across instances.
     *                           Specified at construction.
     *                           [default self::$persistenceClass]
     */
    private $_persistence;

    /** @var string The class to instaniate $this->_scripting. */
    private $_scriptingClass;

    /**
     * @var MockRedisScripting Object to manage Lua scripting commands.
     *                         Instantiated on demand.
     */
    private $_scripting = null;

    private function &_getdbs($prune=false) {
        $dbs = &$this->_dbs;
        if ($prune) {
            // touch every object
            // TODO: this is sorta hacky, merge it with _getdb()
            $_index = $this->_index;
            foreach ($dbs as $index => $db) {
                $this->_index = $index;
                foreach ($db as $key => $_) {
                    $this->_getobj($key);
                }
                if (!$dbs[$index]) {
                    unset($dbs[$index]);
                }
            }
            $this->_index = $_index;
        }
        return $dbs;
    }

    private static function PhpType($var) {
        $type = gettype($var);
        if ($type == 'object') {
            $type = 'object:'.get_class($var);
        } elseif ($type == 'resource') {
            $type = 'resource:'.get_resource_type($var);
        }
        return $type;
    }

    private static function ValidateType($var, $type, $name) {
        if (gettype($var) != $type) {
            $vartype = self::PhpType($var);
            throw new \Exception("Invalid $name type '$vartype', should be '$type' for ".var_export($var, true));
        }
    }

    private static function ValidateObj($obj) {
        self::ValidateType($obj, 'array', 'object');
        if (count($obj) != 3) {
            throw new \Exception("Invalid object, bad structure");
        }
        if (!isset($obj[0])) {
            throw new \Exception("Invalid object, missing type");
        }
        if (!isset($obj[2])) {
            throw new \Exception("Invalid object, missing expiration");
        }
        // TODO: encode persistant values as null or make -1 a constant
        if ($obj[2] != -1) {
            self::ValidateType($obj[2], 'double', 'expiration');
        }

        list($type, $val, $exp) = $obj;

        // type/val
        self::ValidateType($type, 'string', 'object type');
        switch ($type) {
            case 'hash':
                self::ValidateType($val, 'array', 'hash');
                foreach ($val as $k => $v) {
                    // php may cast keys to ints, we must cast them
                    // back before using them.
                    // self::ValidateType($k, 'string', 'hash field');
                    self::ValidateType($v, 'string', 'hash value');
                }
                break;

            case 'list':
                self::ValidateType($val, 'array', 'list');
                $i = 0;
                foreach ($val as $k => $v) {
                    self::ValidateType($k, 'integer', 'list index');
                    self::ValidateType($v, 'string', 'list value');
                    if ($k !== $i++) {
                        throw new \Exception("Invalid list index $index");
                    }
                }
                break;

            case 'set':
                self::ValidateType($val, 'array', 'set');
                foreach ($val as $k => $v) {
                    // php may cast keys to ints, we must cast them
                    // back before using them.
                    // self::ValidateType($k, 'string', 'set member key');
                    self::ValidateType($v, 'string', 'set member value');
                    if ("$k" !== $v) {
                        throw new \Exception("Invalid set member pair '$k','$v'");
                    }
                }
                break;

            case 'zset':
                self::ValidateType($val, 'array', 'zset');
                foreach ($val as $k => $v) {
                    // php may cast keys to ints, we must cast them
                    // back before using them.
                    // self::ValidateType($k, 'string', 'zset member');
                    self::ValidateType($v, 'double', 'zset score');
                }
                break;

            case 'string':
                if ($val !== self::nil) {
                    self::ValidateType($val, 'string', 'string');
                }
                break;

            default:
                throw new \Exception("Invalid object type '$type'");
        }

        if (!is_float($exp) && !is_int($exp)) {
            $type = self::PhpType($exp);
            throw new \Exception("Invalid object ttl type '$type'");
        }
    }

    private static function ValidateDbs($dbs) {
        self::ValidateType($dbs, 'array', 'DBs');
        foreach ($dbs as $index => $db) {
            self::ValidateType($index, 'integer', 'DB index');
            self::ValidateType($db, 'array', 'DB');
            foreach ($db as $obj) {
                // no need to check keys.  php only allows strings and ints,
                // and forces simple integer-like strings to ints.
                self::ValidateObj($obj);
            }
        }
    }

    private function &_getdb($prune=false) {
        $db = &$this->_getdbs()[$this->_index];
        if ($prune) {
            // touch every object
            foreach ($db as $key => $_) {
                $this->_getobj($key);
            }
        }
        return $db;
    }

    private function &_getobj($key, $required=false) {
        $key = "$key";

        $db = &$this->_getdb();

        if (isset($db[$key])) {
            $obj = &$db[$key];

            $time = self::FloatTime();
            if ($obj[1] !== self::nil && $obj[1] !== []
                && ($obj[2] < 0 || $obj[2] > $time))
            {
                return $obj;
            }

            unset($db[$key]);
        }

        if ($required) {
            throw self::ErrorReply('ERR no such key');
        } else {
            $null = null;
            return $null;
        }
    }

    private function &_getval($key, $type, $default, $throw=true) {
        $key = "$key";
        $db = &$this->_getdb();
        $obj = &$this->_getobj($key);
        if (!$obj) {
            $db[$key] = [$type, $default, -1];
        } elseif ($obj[0] != $type) {
            if ($throw) {
                throw self::ErrorReply('WRONGTYPE Operation against a key holding the wrong kind of value');
            } else {
                $null = null;
                return $null;
            }
        }
        return $db[$key][1];
    }

    private function _setval($key, $type, $val, $seconds=null) {
        $db = &$this->_getdb();
        $exp = is_null($seconds) ? -1 : self::FloatTime() + $seconds;
        $db["$key"] = [$type, $val, $exp];
    }

    private function _subcommand($command, $subcommand, $args) {
        $method = '_'.$command.'_'.str_replace('-', '_', $subcommand);
        if (!method_exists($this, $method)) {
            $command = strtoupper($command);
            throw self::ErrorReply("ERR Unknown $command subcommand or wrong number of arguments for '$subcommand'");
        }
        return $this->$method(...$args);
    }

    private static function Remove(&$array, $keys) {
        $count = 0;
        foreach ($keys as $key) {
            $key = "$key";
            $count += (int)isset($array[$key]);
            unset($array[$key]);
        }
        return $count;
    }

    private static function AssertToken($token, $expected) {
        if (isset($token) && strtoupper($token) != $expected) {
            throw self::ErrorReply('ERR syntax error');
        }
        return isset($token);
    }

    /** Ensure $arg is a valid int. */
    private static function IntArg($arg, $param='value') {
        if (!is_int($arg) && (!is_string($arg) || !ctype_digit($arg))) {
            throw self::ErrorReply("ERR invalid $param");
        }
        return (int)$arg;
    }

    // FIXME: this is a workaround for a redisent bug
    private static function ToString($value) {
        try {
            strlen($value);
            return (string)$value;
        } catch (\Throwable $e) {
            throw self::ErrorReply('ERR');
        }
    }

    private static function ParseBit($value, $name='bit') {
        $value = "$value";
        if ($value !== '0' && $value !== '1') {
            throw self::ErrorReply("ERR $name is not an integer or out of range");
        }
        return (int)$value;
    }

    private static function ParseInt($value, $name='value') {
        if (!preg_match('/^(?:0|-?[1-9][0-9]*)$/', $value)) {
            throw self::ErrorReply("ERR $name is not an integer or out of range");
        }
        return (int)$value;
    }

    private static function ParseFloat($value, $name='value') {
        if ($value === '+inf') {
            return INF;
        } elseif ($value === '-inf') {
            return -INF;
        }
        if (!is_numeric($value) || $value[0] == ' ') {
            throw self::ErrorReply("ERR $name is not a valid float or out of range");
        }
        return (float)$value;
    }

    private static function ParseLongLat($longitude, $latitude) {
        $longitude = self::ParseFloat($longitude, 'longitude');
        $latitude = self::ParseFloat($latitude, 'latitude');
        if ($longitude < -180 || $longitude > 180
            || $latitude < -85.05112878 || $latitude > 85.05112878)
        {
            throw self::ErrorReply("ERR invalid longitude,latitude pair $longitude,$latitude");
        }
        throw new NotImplementedException;
    }

    private static function ParseChoice($value, $choices, $name='value') {
        $value = strtoupper($value);
        if (!in_array($value, $choices)) {
            throw self::ErrorReply("ERR syntax error");
        }
        return $value;
    }

    private static function ParseOpts($spec_str, &$args) {
        static $specs = [];
        if (!isset($specs[$spec_str])) {
            $spec = [];
            if ($spec_str[0] != '[' || substr($spec_str, -1) != ']') {
                    throw new \Exception("(internal) Invalid option spec '$spec_str'");
            }
            foreach (explode('] [', substr($spec_str, 1, -1)) as $opt) {
                if (!preg_match('/^([A-Z]+(?:\|[A-Z]+)*)((?: [a-z]+)*)$/', $opt, $m)) {
                    throw new \Exception("(internal) Invalid option spec '$opt'");
                }
                $tokens = explode('|', $m[1]);
                if (count($tokens) > 1) {
                    $opt = $m[1];
                    $spec[$opt] = true;
                } else {
                    $opt = null;
                }
                foreach ($tokens as $token) {
                    $spec[$token] = [$opt, substr_count($m[2], ' ')];
                }
            }
            $specs[$spec_str] = $spec;
        }
        $spec = $specs[$spec_str];

        $opts = array_fill_keys(array_keys($spec), null);
        while ($args) {
            $token = strtoupper($args[0]);
            if (!ctype_upper($token) || !isset($spec[$token])) {
                break;
            }
            list($opt, $count) = $spec[$token];
            if ($opt) {
                if (isset($opts[$opt]) && $opts[$opt] != $token) {
                    throw self::ErrorReply("ERR $token and $opts[$opt] options at the same time are not compatible");
                }
                $opts[$opt] = $token;
            }
            array_shift($args);
            if ($count == 0) {
                $opts[$token] = true;
            } elseif ($count == 1) {
                $opts[$token] = array_shift($args);
            } else {
                $opts[$token] = [];
                for ($i = 0; $i < $count; $i++) {
                    $opts[$token][] = array_shift($args);
                }
            }
        }
        return $opts;
    }

    private static function ScoreGELE($min, $max, $score) {
        return $score >= $min && $score <= $max;
    }

    private static function ScoreGTLE($min, $max, $score) {
        return $score > $min && $score <= $max;
    }

    private static function ScoreGELT($min, $max, $score) {
        return $score >= $min && $score < $max;
    }

    private static function ScoreGTLT($min, $max, $score) {
        return $score > $min && $score < $max;
    }

    /** Convert range arguments to filter callback. */
    private static function MinMaxScore($min, $max) {
        $min = "$min";
        $max = "$max";

        if ($min[0] == '(') {
            $g = 'GT';
            $min = substr($min, 1);
        } else {
            $g = 'GE';
        }
        if ($min == '-inf') {
            $min = -INF;
        } elseif ($min == '+inf' || $min == 'inf') {
            $min = INF;
        } elseif (!is_numeric($min)) {
            throw self::ErrorReply("ERR min [$min] is not a float");
        }

        if ($max[0] == '(') {
            $l = 'LT';
            $max = substr($max, 1);
        } else {
            $l = 'LE';
        }
        if ($max == '-inf') {
            $max = -INF;
        } elseif ($max == '+inf' || $max == 'inf') {
            $max = INF;
        } elseif (!is_numeric($max)) {
            throw self::ErrorReply("ERR  max [$max] is not a float");
        }

        return [self::class."::Score$g$l", (float)$min, (float)$max];
    }

    private static function LexGELE($min, $max, $member) {
        return strcmp($member, $min) >= 0 && strcmp($member, $max) <= 0;
    }

    private static function LexGTLE($min, $max, $member) {
        return strcmp($member, $min) >  0 && strcmp($member, $max) <= 0;
    }

    private static function LexGELT($min, $max, $member) {
        return strcmp($member, $min) >= 0 && strcmp($member, $max) <  0;
    }

    private static function LexGTLT($min, $max, $member) {
        return strcmp($member, $min) >  0 && strcmp($member, $max) <  0;
    }

    /** Convert range arguments to filter callback. */
    private static function MinMaxLex($min, $max) {
        $min = "$min";
        $max = "$max";

        if ($min == '-') {
            $g = 'GE';
            $min = ''; // less than or equal to any string (always true)
        } elseif ($min == '+') {
            $g = 'GT';
            $min = "\xff"; // FIXME: not always false
        } elseif ($min[0] == '(') {
            $g = 'GT';
            $min = substr($min, 1);
        } elseif ($min[0] == '[') {
            $g = 'GE';
            $min = substr($min, 1);
        } else {
            throw self::ErrorReply("ERR min [$min] or max [$max] not valid string range item");
        }

        if ($max == '-') {
            $l = 'LT';
            $max = ''; // less than or equal to any string (always false)
        } elseif ($max == '+') {
            $l = 'LE';
            $max = "\xff"; // FIXME: not always true
        } elseif ($max[0] == '(') {
            $l = 'LT';
            $max = substr($max, 1);
        } elseif ($max[0] == '[') {
            $l = 'LE';
            $max = substr($max, 1);
        } else {
            throw self::ErrorReply("ERR min [$min] or max [$max] not valid string range item");
        }

        return [self::class."::Lex$g$l", $min, $max];
    }

    private static function ParseRange($start, $stop, $count) {
        if ($start === null) {
            $start = 0;
        } else {
            $start = self::ParseInt($start, 'start');
            if ($start < 0) {
                $start += $count;
                if ($start < 0)
                    $start = 0;
            }
        }

        if ($stop === null) {
            $stop = $count - 1;
        } else {
            $stop = self::ParseInt($stop, 'stop');
            if ($stop >= $count) {
                $stop = $count - 1;
            } elseif ($stop < 0) {
                $stop += $count;
            }
        }

        if ($stop < $start || $stop < 0) {
            return [false, false];
        } else {
            return [$start, $stop - $start + 1];
        }
    }

    /** Apply redis slice semantics to an array. */
    private static function Slice($array, $start, $stop) {
        list($offset, $length) = self::ParseRange($start, $stop, count($array));
        if ($offset === false) {
            return [];
        }
        return array_slice($array, $offset, $length);
    }

    /** Break args into sets of size $size. */
    private static function Tuples($args, $size, $command) {
        if (count($args) % $size) {
            if ($command) {
                throw self::ErrorReply('ERR wrong number of arguments for '.strtoupper($command));
            } else {
                // some unit tests expect a simpler message...
                throw self::ErrorReply("ERR syntax error");
            }
        }
        return array_chunk($args, $size);
    }
}
