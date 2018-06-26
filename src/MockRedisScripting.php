<?php // Copyright (c) 2017-2018 Machine Zone, Inc. All rights reserved.

namespace mz\mockredis;

/** Classes that implement a Lua scripting engine for MockRedis. */
interface MockRedisScripting {
    /**
     * Initialize the scripting environment.
     *
     * @param string $lua_init     Lua code to initialize the Lua state
     * @param callable $redis_call The handler for the redis.call() function
     */
    public function __construct($lua_init, $redis_call);

    /**
     * Execute some Lua code in the current Lua state.
     *
     * @param string $lua The Lua code
     * @return array      [bool, mixed] Whether the code finished without
     *                    error, and the return value on success or the error
     *                    message or object on failure.
     */
    public function scriptDo($lua);
}
