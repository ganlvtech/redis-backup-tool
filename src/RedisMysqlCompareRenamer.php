<?php

namespace RedisBackup;

use RedisBackup\Exception\RedisBackupRedisFailedException;

abstract class RedisMysqlCompareRenamer
{
    public $redis;
    public $mysqli;
    public $keyFileReader;
    public $keyFileWriter;
    public $renameSuffix;
    public $tableName;
    public $currentKey;
    public $keyCount;
    public $isFinished;
    public $isDebug = false;

    /**
     * RedisMysqlCompareRenamer constructor.
     *
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
        $this->currentKey = '';
        $this->keyCount = 0;
        $this->isFinished = false;
    }

    public function isFinished()
    {
        return $this->isFinished;
    }

    protected function buildRenamedKey($key)
    {
        return '{' . $key . '}' . $this->renameSuffix;
    }

    protected function rename($key)
    {
        $renamedKey = $this->buildRenamedKey($key);
        if ($this->isDebug) {
            echo "Redis> RENAME $key $renamedKey", PHP_EOL;
        } else {
            $redis_result = $this->redis->rename($key, $renamedKey);
            if ($redis_result === false) {
                throw new RedisBackupRedisFailedException('RENAME', $this->redis);
            }
        }
        $this->keyFileWriter->write($renamedKey);
        return true;
    }

    abstract public function run();
}
