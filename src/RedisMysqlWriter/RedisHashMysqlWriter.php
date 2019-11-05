<?php

namespace RedisBackup\RedisMysqlWriter;

use RedisBackup\Exception\RedisBackupException;
use RedisBackup\Exception\RedisBackupJsonEncodeFailedException;
use RedisBackup\Exception\RedisBackupRedisFailedException;
use RedisBackup\RedisMysqlWriter;

class RedisHashMysqlWriter extends RedisMysqlWriter
{
    public function fetchOneKeyValueUsingPipe($queued_key)
    {
        $this->redis->type($queued_key);
        $this->redis->hGetAll($queued_key);
    }

    public function processOneKeyValueFromPipe($queued_key, $redis_result_array)
    {
        $redis_result = $redis_result_array[0];
        if ($redis_result === false) {
            throw new RedisBackupRedisFailedException('TYPE', $this->redis);
        } elseif ($redis_result === \Redis::REDIS_NOT_FOUND) {
            throw new RedisBackupException('Redis Key 不存在: ' . $queued_key);
        } elseif ($redis_result !== \Redis::REDIS_HASH) {
            throw new RedisBackupException('Redis Key 类型不为 hash: ' . $queued_key);
        }

        $this->currentValue = $redis_result_array[1];
        if ($this->currentValue === false) {
            throw new RedisBackupRedisFailedException('HGETALL', $this->redis);
        }

        $this->currentValue = json_encode($this->currentValue);
        if ($this->currentValue === false) {
            throw new RedisBackupJsonEncodeFailedException($this->currentValue);
        }
    }
}
