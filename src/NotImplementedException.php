<?php // Copyright (c) 2017-2018 Machine Zone, Inc. All rights reserved.

namespace mz\mockredis;

/** Exception for unimplemented commands, breaking the API. */
class NotImplementedException extends MockRedisException {
    public function __construct($message = "", $code = 0, Throwable $previous = NULL) {
        parent::__construct($message, $code, $previous);
        if (!$message) {
            $command = 'that command';
            // scrape the stack to guess the command name
            foreach ($this->getTrace() as $frame) {
                if (isset($frame['function']) && $frame['function'][0] != '_') {
                    $command = "'".strtoupper($frame['function'])."'";
                    break;
                }
            }
            $this->message = "ERR This instance does not support $command or this usage of it";
        }
    }
}
