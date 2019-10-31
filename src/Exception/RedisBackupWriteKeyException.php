<?php

namespace RedisBackup\Exception;

class RedisBackupWriteKeyException extends RedisBackupException
{
    public $key;
    public $file;

    public function __construct($key, $file)
    {
        parent::__construct("向文件中写入 key 错误。Key: {$key}. 文件: {$file}");
        $this->key = $key;
        $this->file = $file;
    }
}
