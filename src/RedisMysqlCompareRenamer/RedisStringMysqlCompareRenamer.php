<?php

namespace RedisBackup\RedisMysqlCompareRenamer;

use RedisBackup\Exception\RedisBackupException;
use RedisBackup\Exception\RedisBackupRedisFailedException;
use RedisBackup\RedisMysqlCompareRenamer;

class RedisStringMysqlCompareRenamer extends RedisMysqlCompareRenamer
{
    public function fetchKeyRedisValueUsingPipe($key)
    {
        $this->redis->type($key);
        $this->redis->get($key);
        return 2;
    }

    public function processKeyRedisValueFromPipe($key, $redis_result_array)
    {
        $redis_result = $redis_result_array[0];
        if ($redis_result === false) {
            throw new RedisBackupRedisFailedException('TYPE', $this->redis);
        } elseif ($redis_result === \Redis::REDIS_NOT_FOUND) {
            throw new RedisBackupException('Redis Key 不存在: ' . $key);
        } elseif ($redis_result !== \Redis::REDIS_STRING) {
            throw new RedisBackupException('Redis Key 类型不为 string: ' . $key);
        }

        $redis_result = $redis_result_array[1];
        if ($redis_result === false) {
            throw new RedisBackupRedisFailedException('GET', $this->redis);
        }

        return $redis_result;
    }

    public function processMysqlValue($key, $value)
    {
        return $value;
    }
}
