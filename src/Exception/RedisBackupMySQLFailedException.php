<?php

namespace RedisBackup\Exception;

class RedisBackupMySQLFailedException extends RedisBackupException
{
    /** @var \mysqli */
    public $mysqli;

    /**
     * @param string $message
     * @param \mysqli $mysqli
     */
    public function __construct($message, $mysqli)
    {
        parent::__construct("{$message}. MySQL 错误: {$mysqli->error}");
        $this->mysqli = $mysqli;
    }
}
