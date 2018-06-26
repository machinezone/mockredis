<?php // Copyright (c) 2017-2018 Machine Zone, Inc. All rights reserved.

namespace mz\mockredis;

/**
 * Simple memory-only storage backend for MockRedis.
 *
 * State will be remembered between MockRedis instances for the duration of
 * this script.
 */
class MemoryMockRedisPersistence implements MockRedisPersistence {
    private static $_servers = [];
    private static $_lastsaves = [];

    public function &load($server, $time) {
        if (!isset(self::$_servers[$server])) {
            self::$_servers[$server] = [];
            self::$_lastsaves[$server] = (int)$time;
        }
        return self::$_servers[$server];
    }

    public function lastsave($server) {
        return self::$_lastsaves[$server];
    }

    public function save($server, $dbs, $time) {
        self::$_lastsaves[$server] = (int)$time;
    }
}
