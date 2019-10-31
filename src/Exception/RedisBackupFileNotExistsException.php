<?php

namespace RedisBackup\Exception;

class RedisBackupFileNotExistsException extends RedisBackupException
{
    public $path;

    public function __construct($path)
    {
        parent::__construct("文件不存在: {$path}");
        $this->path = $path;
    }
}
