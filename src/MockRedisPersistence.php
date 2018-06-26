<?php // Copyright (c) 2017-2018 Machine Zone, Inc. All rights reserved.

namespace mz\mockredis;

/** Classes that implement a storage backend for MockRedis. */
interface MockRedisPersistence {
    /**
     * Load the given server's state, for startup and the DEBUG RELOAD command.
     *
     * @param string $server The server name
     * @param float $time    The current server time in seconds
     * @return array         The server's state (see MockRedis::$_dbs)
     */
    public function &load($server, $time);

    /**
     * The given server's last save time, for the LASTSAVE command.
     *
     * @param string $server The server name
     * @return float|int     The time of the last save
     */
    public function lastsave($server);

    /**
     * Persist the given server's state, for the SAVE command.
     *
     * @param string $server The server name
     * @param array $dbs     The server state
     * @param float $time    The current server time in seconds
     */
    public function save($server, $dbs, $time);
}
