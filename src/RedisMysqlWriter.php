<?php

namespace RedisBackup;

abstract class RedisMysqlWriter
{
    public $redis;
    public $mysqli;
    public $keyFileReader;
    public $keyFileWriter;
    public $tableName;
    public $currentKey;
    public $keyCount;
    public $isFinished;

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
        $this->isFinished = false;
    }

    public function isFinished()
    {
        return $this->isFinished;
    }

    abstract public function run();
}
