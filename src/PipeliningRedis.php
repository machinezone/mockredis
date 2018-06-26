<?php // Copyright (c) 2017-2018 Machine Zone, Inc. All rights reserved.

namespace mz\mockredis;

/**
 * This class uses an instance of MockRedis to emulate the redisent pipelining
 * API.
 */
class PipeliningRedis {
    const DEFAULT_DSN = 'redis://localhost:6379';

    private $_redis;
    private $_results = null;

    public function __construct(
        $dsn = self::DEFAULT_DSN,
        $_timeout = null, // UNUSED, here for compatibility with Redisent
        $persistence = null,
        $scriptingClass = null)
    {
        $dsn = parse_url($dsn);
        $host = $dsn['host'] ?? 'localhost';
        $port = $dsn['port'] ?? 6379;
        $name = "$host:$port";

        $this->_redis = new MockRedis($name, $persistence, $scriptingClass);
    }

    public function __call($name, $args) {
        if (!$this->_redis) {
            throw new \Exception('Not connected');
        }
        if (isset($this->_results)) {
            try {
                $result = $this->_redis->$name(...$args);
            } catch(\Exception $e) {
                if ($e instanceof MockRedis::$exceptionClass) {
                    $result = $e;
                } else {
                    throw $e;
                }
            }
            $this->_results[] = $result;
            return $this;
        } else {
            return $this->_redis->$name(...$args);;
        }
    }

    public function setReadTimeout($timeout) {
        return $this->connected();
    }

    public function connected() {
        return (bool)$this->_redis;
    }

    public function disconnect() {
        $this->_redis = null;
    }

    public function pipeline() {
        $this->_results = [];
        return $this;
    }

    public function uncork() {
        if (!isset($this->_results)) {
            return null;
        }

        $results = $this->_results;
        $this->_results = null;
        foreach ($results as $result) {
            if ($result instanceof MockRedis::$exceptionClass) {
                throw $result;
            }
        }
        return $results;
    }
}
