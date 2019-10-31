<?php

namespace RedisBackup\Exception;

class RedisBackupRedisFailedException extends RedisBackupException
{
    /** @var \Redis */
    public $redis;

    /**
     * @param string $command
     * @param \Redis $redis
     */
    public function __construct($command, $redis)
    {
        parent::__construct("Redis {$command} 命令返回 false. Redis 错误: {$redis->getLastError()}");
        $this->redis = $redis;
    }
}
