<?php

namespace RedisBackup\Exception;

class RedisBackupDeleteCountNotEqualsException extends RedisBackupException
{
    /** @var int */
    public $expect;
    /** @var int */
    public $actual;
    /** @var \Redis */
    public $redis;

    /**
     * @param int $expect
     * @param int $actual
     * @param \Redis $redis
     */
    public function __construct($expect, $actual, $redis)
    {
        parent::__construct("Redis DEL 删除数量不一致。期望数量: {$expect}, 实际删除数量: {$actual}");
        $this->expect = $expect;
        $this->actual = $actual;
        $this->redis = $redis;
    }
}
