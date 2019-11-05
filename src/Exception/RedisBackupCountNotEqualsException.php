<?php

namespace RedisBackup\Exception;

class RedisBackupCountNotEqualsException extends RedisBackupException
{
    /** @var int */
    public $expect;
    /** @var int */
    public $actual;

    /**
     * @param string $message
     * @param int $expect
     * @param int $actual
     */
    public function __construct($message, $expect, $actual)
    {
        parent::__construct("{$message} 数量与期望数量不一致。期望数量: {$expect}。实际数量: {$actual}");
        $this->expect = $expect;
        $this->actual = $actual;
    }
}
