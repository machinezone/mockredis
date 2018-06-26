<?php // Copyright (c) 2017-2018 Machine Zone, Inc. All rights reserved.

namespace mz\mockredis;

/**
 * Simple on-disk storage backend for MockRedis.
 *
 * Files will be stored in a given directory, and named based on the server
 * name being mocked.  E.g. "localhost.db" or "127.0.0.1.6379.db".
 */
abstract class FileMockRedisPersistence implements MockRedisPersistence {
    const FILE_EXT = 'db';

    private static $_files = [];

    private $_dir;

    abstract protected function encode($dbs);
    abstract protected function decode($contents);

    private function _file($server) {
        $server = str_replace(':', '.', $server);
        return "$this->_dir/$server.".static::FILE_EXT;
    }

    /**
     * A new persistence handler.
     *
     * @param string $dir  An existing directory to store server state
     */
    public function __construct($dir) {
        if (!is_dir($dir)) {
            throw new \Exception("$dir does not exist");
        }
        $this->_dir = $dir;
    }

    public function &load($server, $_time) {
        $file = $this->_file($server);
        if (!isset(self::$_files[$file])) {
            // only allow one php instance at a time to manipulate a given db.
            // this gives some semblance of command serialization across
            // scripts.
            $FILE = fopen($file, 'c+');
            flock($FILE, LOCK_EX);
            $contents = file_get_contents($file);
            $dbs = strlen($contents)  ? $this->decode($contents) : [];
            self::$_files[$file] = [$FILE, $dbs];
        }
        return self::$_files[$file][1];
    }

    public function lastsave($server) {
        return filemtime($this->_file($server));
    }

    public function save($server, $dbs, $time) {
        $file = $this->_file($server);
        file_put_contents($file, $this->encode($dbs));
        touch($file, (int)$time);
    }
}
