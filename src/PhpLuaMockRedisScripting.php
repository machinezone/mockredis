<?php // Copyright (c) 2017-2018 Machine Zone, Inc. All rights reserved.

namespace mz\mockredis;

/** Lua scripting engine for MockRedis using php-lua. */
class PhpLuaMockRedisScripting implements MockRedisScripting {
    const LUA_INIT_CALL = <<<'LUA_INIT'
        function redis.call(...)
            if __mockredis_call(...) then
                return __mockredis_result
            else
                error(__mockredis_result)
            end
        end

        function redis.pcall(...)
            if __mockredis_call(...) then
                return __mockredis_result
            else
                return redis.error_reply(__mockredis_result)
            end
        end
LUA_INIT;

    private $L;
    private $redis_call;

    public function __construct($lua_init, $redis_call) {
        $this->redis_call = $redis_call;

        $this->L = new \Lua;
        $this->L->registerCallback('__mockredis_call', [$this, 'call']);
        $this->L->eval($lua_init);
        $this->L->eval(self::LUA_INIT_CALL);
    }

    /** Adjust a normal PHP numeric array to be 1-based for Lua. */
    private static function ShiftArray(&$array) {
        array_unshift($array, null);
        unset($array[0]);
    }

    private static function ToLua(&$value) {
        if (is_array($value)) {
            self::ShiftArray($value);
            foreach ($value as &$v) {
                self::ToLua($v);
            }
        } elseif ($value === MockRedis::nil) {
            $value = false;
        } elseif ($value === MockRedis::OK) {
            $value = ['ok' => 'OK'];
        } elseif ($value === MockRedis::PONG) {
            // FIXME: this may convert a bulk "PONG" to a status
            $value = ['ok' => 'PONG'];
        }
    }

    /** Handle a redis.call(), redirecting to the given callback. */
    public function call(...$args) {
        $redis_call = $this->redis_call;
        $result = $redis_call(...$args);
        if ($result instanceof \Throwable) {
            $this->L->assign('__mockredis_result', $result->getMessage());
            return false;
        } else {
            self::ToLua($result);
            $this->L->assign('__mockredis_result', $result);
            return true;
        }
    }

    /** Convert arrays in a Lua return value to normal PHP numeric arrays. */
    private static function ToResult($value) {
        if (!is_array($value)) {
            return $value;
        } elseif (isset($value['err']) && is_string($value['err'])) {
            return $value;
        } elseif (isset($value['ok']) && is_string($value['ok'])) {
            return $value;
        } else {
            $retval = [];
            // stop at the first nil
            for ($i = 1; isset($value[$i]); $i++) {
                $retval[] = self::ToResult($value[$i]);
            }
            return $retval;
        }
    }

    public function scriptDo($lua, $keys=null, $argv=null) {
        if (isset($keys) && isset($argv)) {
            self::ShiftArray($keys);
            $this->L->assign('KEYS', $keys);

            self::ShiftArray($argv);
            $this->L->assign('ARGV', $argv);
        }

        try {
            $retval = $this->L->eval($lua);
        } catch (\LuaException $e) {
            return [false, $e->getMessage()];
        }

        if (is_array($retval)) {
            // php-lua quirk.  if the first key is 0, L->eval() returned
            // multiple values, not a table.
            reset($retval);
            if (key($retval) === 0) {
                $retval = $retval[0];
            }
        }

        return [true, self::ToResult($retval)];
    }
}
