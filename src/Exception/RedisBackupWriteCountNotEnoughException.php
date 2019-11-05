<?php

namespace RedisBackup\Exception;

class RedisBackupWriteCountNotEnoughException extends RedisBackupException
{
    /** @var \mysqli */
    public $mysqli;

    /**
     * @param int $expect
     * @param int $actual
     * @param \mysqli $mysqli
     */
    public function __construct($expect, $actual, $mysqli)
    {
        parent::__construct("MySQL 影响行数不足。期望影响行数: {$expect}, 实际影响行数: {$actual}");
        $this->mysqli = $mysqli;
    }
}
