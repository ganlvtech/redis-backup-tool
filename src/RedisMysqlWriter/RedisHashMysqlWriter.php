<?php

namespace RedisBackup\RedisMysqlWriter;

use RedisBackup\Exception\RedisBackupException;
use RedisBackup\Exception\RedisBackupJsonEncodeFailedException;
use RedisBackup\Exception\RedisBackupRedisFailedException;
use RedisBackup\RedisMysqlWriter;

class RedisHashMysqlWriter extends RedisMysqlWriter
{
    public function fetchKeyRedisValueUsingPipe($key)
    {
        $this->redis->type($key);
        $this->redis->hGetAll($key);
        return 2;
    }

    public function processKeyRedisValueFromPipe($key, $redis_result_array)
    {
        $redis_result = $redis_result_array[0];
        if ($redis_result === false) {
            throw new RedisBackupRedisFailedException('TYPE', $this->redis);
        } elseif ($redis_result === \Redis::REDIS_NOT_FOUND) {
            throw new RedisBackupException('Redis Key 不存在: ' . $key);
        } elseif ($redis_result !== \Redis::REDIS_HASH) {
            throw new RedisBackupException('Redis Key 类型不为 hash: ' . $key);
        }

        $redis_result = $redis_result_array[1];
        if ($redis_result === false) {
            throw new RedisBackupRedisFailedException('HGETALL', $this->redis);
        }

        return $redis_result;
    }

    public function buildMysqlValue($key, $value)
    {
        $mysql_values = json_encode($value);
        if ($mysql_values === false) {
            throw new RedisBackupJsonEncodeFailedException($value);
        }

        return $mysql_values;
    }
}
