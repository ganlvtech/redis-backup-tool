<?php

namespace RedisBackup\Exception;

class RedisBackupValueNotEqualsException extends RedisBackupException
{
    public $key;
    public $mysqlValue;
    public $redisValue;

    public function __construct($key, $mysqlValue, $redisValue)
    {
        parent::__construct("MySQL 与 Redis 数据不一致. Key: {$key}. MySQL: {$mysqlValue}. Redis: {$redisValue}");
        $this->key = $key;
        $this->mysqlValue = $mysqlValue;
        $this->redisValue = $redisValue;
    }
}
