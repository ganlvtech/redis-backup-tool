<?php

namespace RedisBackup;

use RedisBackup\Exception\RedisBackupRedisFailedException;
use RedisBackup\Util\Logger;
use RedisBackup\Util\Sampler;
use RedisBackup\Util\Timer;

class RedisKeyRemover
{
    /** @var \Redis */
    public $redis;
    /** @var \RedisBackup\KeyFileReader */
    public $keyFileReader;
    /** @var \RedisBackup\KeyFileWriter */
    public $keyFileWriter;

    public $currentKey;
    public $keyCount;
    public $isFinished;

    public function __construct($redis, $keyFileReader, $keyFileWriter)
    {
        $this->redis = $redis;
        $this->keyFileReader = $keyFileReader;
        $this->keyFileWriter = $keyFileWriter;
        $this->keyCount = 0;
        $this->isFinished = false;
    }

    public function isFinished()
    {
        return $this->isFinished;
    }

    public function run()
    {
        Logger::info("开始删除 Redis key");
        Timer::start();
        while (!$this->isFinished()) {
            $this->currentKey = $this->keyFileReader->getKey();
            if ($this->currentKey === false) {
                $this->isFinished = true;
                break;
            }

            $redis_result = $this->redis->del($this->currentKey);
            if ($redis_result === false) {
                throw new RedisBackupRedisFailedException('DEL', $this->redis);
            }

            ++$this->keyCount;
            if (Sampler::sample(300)) {
                $t = Timer::time();
                Logger::info("已删除 key 数量: {$this->keyCount}\t耗时: {$t} s");
            }
        }
        $t = Timer::time();
        Logger::info("删除完成！key 数量: {$this->keyCount}\t耗时: {$t} s");
    }
}
