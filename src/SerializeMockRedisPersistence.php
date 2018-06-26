<?php // Copyright (c) 2017-2018 Machine Zone, Inc. All rights reserved.

namespace mz\mockredis;

/** Simple on-disk storage backend for MockRedis unsing [un]serialize(). */
class SerializeMockRedisPersistence extends FileMockRedisPersistence {
    protected function encode($dbs) {
        return serialize($dbs);
    }

    protected function decode($contents) {
        return unserialize($contents);
    }
}
