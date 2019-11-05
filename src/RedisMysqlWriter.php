<?php

namespace RedisBackup;

use RedisBackup\Exception\RedisBackupMySQLFailedException;
use RedisBackup\Exception\RedisBackupRedisFailedException;
use RedisBackup\Exception\RedisBackupWriteCountNotEnoughException;
use RedisBackup\Util\Logger;
use RedisBackup\Util\Sampler;
use RedisBackup\Util\Timer;

abstract class RedisMysqlWriter
{
    public $redis;
    public $mysqli;
    public $keyFileReader;
    public $keyFileWriter;
    public $tableName;
    public $currentKey;
    public $currentValue;
    public $keyCount;

    public $queuedKeys;
    public $queuedKeyValues;

    /**
     * @param \Redis $redis
     * @param \mysqli $mysqli
     * @param \RedisBackup\KeyFileReader $keyFileReader
     * @param \RedisBackup\KeyFileWriter $keyFileWriter
     * @param string $tableName
     */
    public function __construct($redis, $mysqli, $keyFileReader, $keyFileWriter, $tableName)
    {
        $this->redis = $redis;
        $this->mysqli = $mysqli;
        $this->keyFileReader = $keyFileReader;
        $this->keyFileWriter = $keyFileWriter;
        $this->tableName = $tableName;
        $this->currentKey = '';
        $this->keyCount = 0;
        $this->queuedKeys = array();
        $this->queuedKeyValues = array();
    }

    public function isFinished()
    {
        return $this->keyFileReader->isFinished();
    }

    abstract public function fetchOneKeyValueUsingPipe($queued_key);

    abstract public function processOneKeyValueFromPipe($queued_key, $redis_result_array);

    public function queueKey($key)
    {
        $this->queuedKeys[] = $key;
    }

    public function fetchQueuedKeyValues()
    {
        $this->redis->multi(\Redis::PIPELINE);
        foreach ($this->queuedKeys as $queued_key) {
            $this->currentKey = $queued_key;
            $this->fetchOneKeyValueUsingPipe($queued_key);
        }
        $redis_result_array = $this->redis->exec();
        if ($redis_result_array === false) {
            throw new RedisBackupRedisFailedException('批量获取 Redis EXEC', $this->redis);
        }
        $each_key_result_count = count($redis_result_array) / count($this->queuedKeys);
        $i = 0;
        foreach ($this->queuedKeys as $queued_key) {
            $this->currentKey = $queued_key;
            $this->processOneKeyValueFromPipe($queued_key, array_slice($redis_result_array, $i, $each_key_result_count));
            $this->queueKeyValue($queued_key, $this->currentValue);
            $i += $each_key_result_count;
        }
    }

    public function clearQueuedKeys()
    {
        $this->queuedKeys = array();
    }

    public function queueKeyValue($key, $value)
    {
        $this->queuedKeyValues[$key] = $value;
    }

    public function insertQueuedKeyValue()
    {
        $lines = array();
        foreach ($this->queuedKeyValues as $key => $value) {
            $key = $this->mysqli->real_escape_string($key);
            $value = $this->mysqli->real_escape_string($value);
            $line = "('{$key}', '{$value}', CURRENT_TIMESTAMP())";
            $lines[] = $line;
        }
        $values = implode(',', $lines);
        $sql = "INSERT INTO {$this->tableName} (k, v, created_at) VALUES {$values} ON DUPLICATE KEY UPDATE v = VALUES(v), updated_at = CURRENT_TIMESTAMP()";
        if (!$this->mysqli->query($sql)) {
            throw new RedisBackupMySQLFailedException("MySQL 插入失败 SQL: {$sql}", $this->mysqli);
        }
        $expect_count = count($this->queuedKeyValues);
        if ($this->mysqli->affected_rows < $expect_count) {
            throw new RedisBackupWriteCountNotEnoughException($expect_count, $this->mysqli->affected_rows, $this->mysqli);
        }
    }

    public function clearQueuedKeyValues()
    {
        $this->queuedKeyValues = array();
    }

    public function runBatch()
    {
        $this->fetchQueuedKeyValues();
        $this->clearQueuedKeys();
        $this->insertQueuedKeyValue();
        $this->keyCount += count($this->queuedKeyValues);
        $this->keyFileWriter->writeMultiple(array_keys($this->queuedKeyValues));
        $this->clearQueuedKeyValues();

        $t = Timer::time();
        Logger::info("已写入 key 数量: {$this->keyCount}\t耗时: {$t} s");
    }

    public function run($batch = 300)
    {
        Logger::info("开始读取 Redis 并写入 MySQL");
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
        Logger::info('写入完成！');
    }
}
