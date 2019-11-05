<?php

namespace RedisBackup\RedisMysqlCompareRenamer;

use RedisBackup\Exception\RedisBackupException;
use RedisBackup\Exception\RedisBackupJsonDecodeFailedException;
use RedisBackup\Exception\RedisBackupJsonEncodeFailedException;
use RedisBackup\Exception\RedisBackupRedisFailedException;
use RedisBackup\RedisMysqlCompareRenamer;

class RedisHashMysqlCompareRenamer extends RedisMysqlCompareRenamer
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

        ksort($redis_result);
        $value = json_encode($redis_result);
        if ($value === false) {
            throw new RedisBackupJsonEncodeFailedException($redis_result);
        }

        return $value;
    }

    public function processMysqlValue($key, $value)
    {
        $mysql_values = json_decode($value, true);
        if ($mysql_values === null) {
            throw new RedisBackupJsonDecodeFailedException($value);
        }

        ksort($mysql_values);
        $value = json_encode($mysql_values);
        if ($value === false) {
            throw new RedisBackupJsonEncodeFailedException($mysql_values);
        }

        return $value;
    }
}
