<?php

namespace RedisBackup;

abstract class RedisMysqlWriter
{
    public $redis;
    public $mysqli;
    public $keyFileReader;
    public $keyFileWriter;
    public $currentKey;
    public $keyCount;
    public $isFinished;

    /**
     * @param \Redis $redis
     * @param \mysqli $mysqli
     * @param \RedisBackup\KeyFileReader $keyFileReader
     * @param \RedisBackup\KeyFileWriter $keyFileWriter
     */
    public function __construct($redis, $mysqli, $keyFileReader, $keyFileWriter)
    {
        $this->redis = $redis;
        $this->mysqli = $mysqli;
        $this->keyFileReader = $keyFileReader;
        $this->keyFileWriter = $keyFileWriter;
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
