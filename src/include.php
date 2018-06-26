<?php // Copyright (c) 2017-2018 Machine Zone, Inc. All rights reserved.

// include everything, if you don't want to use autoloading

require_once __DIR__.'/MockRedisException.php';
require_once __DIR__.'/NotImplementedException.php';

require_once __DIR__.'/MockRedisPersistence.php';
require_once __DIR__.'/MemoryMockRedisPersistence.php';
require_once __DIR__.'/FileMockRedisPersistence.php';
require_once __DIR__.'/JsonMockRedisPersistence.php';
require_once __DIR__.'/SerializeMockRedisPersistence.php';

require_once __DIR__.'/MockRedisScripting.php';
require_once __DIR__.'/PhpLuaMockRedisScripting.php';

require_once __DIR__.'/MockRedis.php';
require_once __DIR__.'/PipeliningRedis.php';
