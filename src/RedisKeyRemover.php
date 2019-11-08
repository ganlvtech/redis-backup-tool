<?php

namespace RedisBackup;

use RedisBackup\Exception\RedisBackupCountNotEqualsException;
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

    public $queuedKeys;

    public function __construct($redis, $keyFileReader, $keyFileWriter)
    {
        $this->redis = $redis;
        $this->keyFileReader = $keyFileReader;
        $this->keyFileWriter = $keyFileWriter;
        $this->keyCount = 0;
        $this->queuedKeys = array();
    }

    public function isFinished()
    {
        return $this->keyFileReader->isFinished();
    }

    public function queueKey($key)
    {
        $this->queuedKeys[] = $key;
    }

    public function delQueuedKeys()
    {
        $redis_result = call_user_func_array(array($this->redis, 'del'), $this->queuedKeys);
        return $redis_result;
    }

    public function clearQueuedKeys()
    {
        $this->queuedKeys = array();
    }

    public function runBatch()
    {
        if (!$this->queuedKeys) {
            return;
        }
        $redis_result = $this->delQueuedKeys();
        if ($redis_result === false) {
            throw new RedisBackupRedisFailedException('DEL', $this->redis);
        }
        $del_count = (int)$redis_result;
        if ($del_count != count($this->queuedKeys)) {
            throw new RedisBackupCountNotEqualsException('Redis 删除 Key', count($this->queuedKeys), $del_count);
        }
        $this->keyCount += $del_count;
        $this->keyFileWriter->writeMultiple($this->queuedKeys);
        $this->clearQueuedKeys();

        $t = Timer::time();
        Logger::info("已删除 key 数量: {$this->keyCount}\t耗时: {$t} s");
    }

    public function run($batch = 300)
    {
        Logger::info("开始删除 Redis key");
        Timer::start();
        while (!$this->isFinished()) {
            $this->currentKey = $this->keyFileReader->getKey();
            if ($this->currentKey === false) {
                break;
            }
            $this->queueKey($this->currentKey);
            if (Sampler::sample($batch)) {
                $this->runBatch();
            }
        }
        $this->runBatch();
        Logger::info('删除完成！');
    }
}
