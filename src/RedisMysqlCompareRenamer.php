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

abstract class RedisMysqlCompareRenamer
{
    public $redis;
    public $mysqli;
    public $keyFileReader;
    public $keyFileWriter;
    public $renameSuffix;
    public $tableName;

    public $queuedKeys;
    public $queuedMysqlValues;
    public $queuedRedisValues;
    public $queuedRenamedKeys;

    public $currentKey;
    public $keyCount;
    public $isDebug = false;

    /**
     * @param \Redis $redis
     * @param \mysqli $mysqli
     * @param \RedisBackup\KeyFileReader $keyFileReader
     * @param \RedisBackup\KeyFileWriter $keyFileWriter
     * @param string $renameSuffix
     * @param string $tableName
     */
    public function __construct($redis, $mysqli, $keyFileReader, $keyFileWriter, $renameSuffix, $tableName)
    {
        $this->redis = $redis;
        $this->mysqli = $mysqli;
        $this->keyFileReader = $keyFileReader;
        $this->keyFileWriter = $keyFileWriter;
        $this->renameSuffix = $renameSuffix;
        $this->tableName = $tableName;

        $this->queuedKeys = array();
        $this->queuedMysqlValues = array();
        $this->queuedRedisValues = array();

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
     * @return string 待比较的 Redis Value
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
            $this->queuedRedisValues[$key] = $this->processKeyRedisValueFromPipe($key, array_slice($redis_result_array, $i, $each_key_result_count));
            $i += $each_key_result_count;
        }
    }

    // ==================== MySQL 部分 ====================

    /**
     * @param string $key
     * @param string $value
     *
     * @return string 待比较的 MySQL Value
     */
    abstract public function processMysqlValue($key, $value);

    public function fetchQueuedKeyMysqlValues()
    {
        if (!$this->queuedKeys) {
            throw new \Exception("Unexpected empty \$this->queuedKeys");
        }

        $lines = array();
        foreach ($this->queuedKeys as $key) {
            $key = $this->mysqli->real_escape_string($key);
            $line = "'{$key}'";
            $lines[] = $line;
        }
        $keys = implode(',', $lines);

        $sql = "SELECT k, v FROM {$this->tableName} WHERE k IN ({$keys})";
        $mysql_result = $this->mysqli->query($sql);
        if (!$mysql_result) {
            throw new RedisBackupMySQLFailedException("MySQL 读取失败 SQL: {$sql}", $this->mysqli);
        }

        $actual_count = 0;
        while ($row = $mysql_result->fetch_assoc()) {
            $this->currentKey = $row['k'];
            $this->queuedMysqlValues[$row['k']] = $this->processMysqlValue($row['k'], $row['v']);
            ++$actual_count;
        }
        $mysql_result->close();

        $expect_count = count($this->queuedKeys);
        if ($actual_count < $expect_count) {
            throw new RedisBackupCountNotEqualsException('MySQL 批量获取结果行数', $expect_count, $actual_count);
        }

        foreach ($this->queuedKeys as $key) {
            $this->currentKey = $key;
            if (!isset($this->queuedMysqlValues[$key])) {
                throw new RedisBackupException("MySQL Key 不存在. Key: {$key}");
            }
        }
    }

    // ==================== Redis 与 MySQL 比较部分 ====================

    public function compareQueuedValues()
    {
        foreach ($this->queuedKeys as $key) {
            $this->currentKey = $key;
            if ($this->queuedMysqlValues[$key] !== $this->queuedRedisValues[$key]) {
                throw new RedisBackupValueNotEqualsException($key, $this->queuedMysqlValues[$key], $this->queuedRedisValues[$key]);
            }
        }
    }

    public function clearQueuedValues()
    {
        $this->queuedMysqlValues = array();
        $this->queuedRedisValues = array();
    }

    // ==================== 重命名部分 ====================

    protected function buildRenamedKey($key)
    {
        return '{' . $key . '}' . $this->renameSuffix;
    }

    public function renameQueuedKeys()
    {
        $this->queuedRenamedKeys = array();
        if ($this->isDebug) {
            foreach ($this->queuedKeys as $key) {
                $this->currentKey = $key;
                $renamedKey = $this->buildRenamedKey($key);
                echo "Redis> RENAME $key $renamedKey", PHP_EOL;
                $this->queuedRenamedKeys[] = $renamedKey;
            }
        } else {
            $this->redis->multi(\Redis::PIPELINE);
            $expect_result_count = 0;
            foreach ($this->queuedKeys as $key) {
                $this->currentKey = $key;
                $renamedKey = $this->buildRenamedKey($key);
                $this->redis->rename($key, $renamedKey);
                ++$expect_result_count;
                $this->queuedRenamedKeys[] = $renamedKey;
            }

            $redis_result_array = $this->redis->exec();
            if ($redis_result_array === false) {
                throw new RedisBackupRedisFailedException('PIPELINE & EXEC', $this->redis);
            }

            $actual_result_count = count($redis_result_array);
            if ($actual_result_count !== $expect_result_count) {
                throw new RedisBackupCountNotEqualsException('Redis Pipeline 返回结果', $expect_result_count, $actual_result_count);
            }

            $i = 0;
            foreach ($this->queuedKeys as $key) {
                $this->currentKey = $key;
                if ($redis_result_array[$i] === false) {
                    throw new RedisBackupRedisFailedException('RENAME', $this->redis);
                }
                ++$i;
            }
        }
    }

    // ==================== 主程序 ====================

    public function runBatch()
    {
        $this->fetchQueuedKeyRedisValues();
        $this->fetchQueuedKeyMysqlValues();
        $this->compareQueuedValues();
        $this->clearQueuedValues();

        $this->renameQueuedKeys();
        $this->clearQueuedKeys();
        $this->keyCount += count($this->queuedRenamedKeys);
        $this->keyFileWriter->writeMultiple($this->queuedRenamedKeys);

        $t = Timer::time();
        Logger::info("已重命名 key 数量: {$this->keyCount}\t耗时: {$t} s");
    }

    public function run($batch = 300)
    {
        Logger::info("开始读取 Redis 和 MySQL 并进行比较，成功后修改 Redis key 的名称");
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
        Logger::info('重命名完成！');
    }
}
