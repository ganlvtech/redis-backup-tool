<?php

namespace RedisBackup\RedisMysqlWriter;

use RedisBackup\Exception\RedisBackupException;
use RedisBackup\Exception\RedisBackupRedisFailedException;
use RedisBackup\RedisMysqlWriter;

class RedisStringMysqlWriter extends RedisMysqlWriter
{
    public function fetchOneKeyValueUsingPipe($queued_key)
    {
        $this->redis->type($queued_key);
        $this->redis->get($queued_key);
    }

    public function processOneKeyValueFromPipe($queued_key, $redis_result_array)
    {
        $redis_result = $redis_result_array[0];
        if ($redis_result === false) {
            throw new RedisBackupRedisFailedException('TYPE', $this->redis);
        } elseif ($redis_result === \Redis::REDIS_NOT_FOUND) {
            throw new RedisBackupException('Redis Key 不存在: ' . $this->currentKey);
        } elseif ($redis_result !== \Redis::REDIS_STRING) {
            throw new RedisBackupException('Redis Key 类型不为 string: ' . $this->currentKey);
        }

        $this->currentValue = $redis_result_array[1];
        if ($this->currentValue === false) {
            throw new RedisBackupRedisFailedException('GET', $this->redis);
        }
    }
}
