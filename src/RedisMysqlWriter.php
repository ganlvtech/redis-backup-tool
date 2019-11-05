<?php

namespace RedisBackup;

use RedisBackup\Exception\RedisBackupCountNotEqualsException;
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

    public $queuedKeys;
    public $queuedValues;

    public $currentKey;
    public $keyCount;

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

        $this->queuedKeys = array();
        $this->queuedValues = array();

        $this->currentKey = '';
        $this->keyCount = 0;
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

    // ==================== Redis 部分 ====================

    /**
     * @param string $key
     *
     * @return int 此方法中执行了几次 Redis 操作（即有几个返回值）
     */
    abstract public function fetchKeyRedisValueUsingPipe($key);

    /**
     * @param string $key
     * @param string $redis_result_array
     *
     * @return mixed Redis Value
     */
    abstract public function processKeyRedisValueFromPipe($key, $redis_result_array);

    public function fetchQueuedKeyRedisValues()
    {
        $this->redis->multi(\Redis::PIPELINE);
        $expect_result_count = 0;
        foreach ($this->queuedKeys as $key) {
            $this->currentKey = $key;
            $expect_result_count += $this->fetchKeyRedisValueUsingPipe($key);
        }
        $redis_result_array = $this->redis->exec();
        if ($redis_result_array === false) {
            throw new RedisBackupRedisFailedException('PIPELINE & EXEC', $this->redis);
        }

        $actual_result_count = count($redis_result_array);
        if ($actual_result_count !== $expect_result_count) {
            throw new RedisBackupCountNotEqualsException('Redis Pipeline 返回结果', $expect_result_count, $actual_result_count);
        }

        $each_key_result_count = $actual_result_count / count($this->queuedKeys);
        $i = 0;
        foreach ($this->queuedKeys as $key) {
            $this->currentKey = $key;
            $this->queuedValues[$key] = $this->processKeyRedisValueFromPipe($key, array_slice($redis_result_array, $i, $each_key_result_count));
            $i += $each_key_result_count;
        }
    }

    // ==================== MySQL 部分 ====================

    /**
     * @param string $key
     * @param mixed $value
     *
     * @return string 待写入 MySQL 的 Redis Value
     */
    abstract public function buildMysqlValue($key, $value);

    public function insertQueuedValues()
    {
        $lines = array();
        foreach ($this->queuedValues as $key => $value) {
            $key = $this->mysqli->real_escape_string($key);
            $value = $this->buildMysqlValue($key, $value);
            $value = $this->mysqli->real_escape_string($value);
            $line = "('{$key}', '{$value}', CURRENT_TIMESTAMP())";
            $lines[] = $line;
        }
        $values = implode(',', $lines);
        $sql = "INSERT INTO {$this->tableName} (k, v, created_at) VALUES {$values} ON DUPLICATE KEY UPDATE v = VALUES(v), updated_at = CURRENT_TIMESTAMP()";
        if (!$this->mysqli->query($sql)) {
            throw new RedisBackupMySQLFailedException("MySQL 插入失败 SQL: {$sql}", $this->mysqli);
        }
        $expect_count = count($this->queuedValues);
        $actual_count = $this->mysqli->affected_rows;
        if ($actual_count < $expect_count) {
            throw new RedisBackupWriteCountNotEnoughException($expect_count, $this->mysqli->affected_rows, $this->mysqli);
        }
    }

    public function clearQueuedValues()
    {
        $this->queuedValues = array();
    }

    // ==================== 主程序 ====================

    public function runBatch()
    {
        $this->fetchQueuedKeyRedisValues();
        $this->insertQueuedValues();
        $this->clearQueuedValues();

        $this->keyCount += count($this->queuedKeys);
        $this->keyFileWriter->writeMultiple($this->queuedKeys);
        $this->clearQueuedKeys();

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
