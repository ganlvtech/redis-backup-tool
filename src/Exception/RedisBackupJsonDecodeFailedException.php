<?php

namespace RedisBackup\Exception;

class RedisBackupJsonDecodeFailedException extends RedisBackupException
{
    public $data;

    public function __construct($data)
    {
        parent::__construct('json_decode 解码错误: $data = ' . var_export($data, true));
        $this->data = $data;
    }
}
