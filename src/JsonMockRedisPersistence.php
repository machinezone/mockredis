<?php // Copyright (c) 2017-2018 Machine Zone, Inc. All rights reserved.

namespace mz\mockredis;

/**
 * Simple on-disk json storage backend for MockRedis.
 *
 * This is faster and more readable than [un]serialize(), but it is not safe
 * for binary strings.
 */
class JsonMockRedisPersistence extends FileMockRedisPersistence {
    const FILE_EXT = 'json';
    const ENCODE_OPTS = JSON_PRESERVE_ZERO_FRACTION; // | JSON_PRETTY_PRINT;

    protected function encode($dbs) {
        return json_encode($dbs, self::ENCODE_OPTS);
    }

    protected function decode($contents) {
        return json_decode($contents, true);
    }
}
