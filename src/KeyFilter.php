<?php

namespace RedisBackup;

use RedisBackup\Exception\RedisBackupCountNotEqualsException;
use RedisBackup\Exception\RedisBackupException;
use RedisBackup\Exception\RedisBackupMySQLFailedException;
use RedisBackup\Exception\RedisBackupRedisFailedException;
use RedisBackup\Exception\RedisBackupValueNotEqualsException;
use RedisBackup\Util\Logger;
use RedisBackup\Util\Sampler;
use RedisBackup\Util\Timer;

abstract class KeyFilter
{
    public $keyFileReader;
    public $keyFileWriter;

    public $queuedKeys;
    public $queuedFilteredKeys;

    public $currentKey;
    public $keyCount;
    public $scannedKeyCount;

    /**
     * @param \RedisBackup\KeyFileReader $keyFileReader
     * @param \RedisBackup\KeyFileWriter $keyFileWriter
     */
    public function __construct($keyFileReader, $keyFileWriter)
    {
        $this->keyFileReader = $keyFileReader;
        $this->keyFileWriter = $keyFileWriter;

        $this->queuedKeys = array();
        $this->queuedFilteredKeys = array();

        $this->currentKey = '';
        $this->keyCount = 0;
        $this->scannedKeyCount = 0;
    }

    public function isFinished()
    {
        return $this->keyFileReader->isFinished();
    }

    public function queueKey($key)
    {
        $this->queuedKeys[] = $key;
    }

    public function clearQueuedKeys()
    {
        $this->queuedKeys = array();
    }

    public function queueFilteredKey($key)
    {
        $this->queuedFilteredKeys[] = $key;
    }

    public function clearQueuedFilteredKeys()
    {
        $this->queuedFilteredKeys = array();
    }

    abstract public function filterQueuedKeys();

    public function runBatch()
    {
        $this->filterQueuedKeys();
        $this->scannedKeyCount += count($this->queuedKeys);
        $this->keyCount += count($this->queuedFilteredKeys);
        $this->keyFileWriter->writeMultiple($this->queuedFilteredKeys);
        $this->clearQueuedFilteredKeys();
        $this->clearQueuedKeys();

        $t = Timer::time();
        Logger::info("已过滤 key 数量: {$this->scannedKeyCount}\t过滤后数量: {$this->keyCount}\t耗时: {$t} s");
    }

    public function run($batch = 300)
    {
        Logger::info("开始过滤 key");
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
        Logger::info('过滤完成！');
    }
}
