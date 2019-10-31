<?php

namespace RedisBackup\Exception;

class RedisBackupJsonEncodeFailedException extends RedisBackupException
{
    public $data;

    public function __construct($data)
    {
        parent::__construct('json_encode 编码错误: $data = ' . var_export($data, true));
        $this->data = $data;
    }
}
