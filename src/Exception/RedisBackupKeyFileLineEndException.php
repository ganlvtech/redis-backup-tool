<?php

namespace RedisBackup\Exception;

class RedisBackupKeyFileLineEndException extends RedisBackupException
{
    public $path;
    public $lineNumber;
    public $line;

    public function __construct($path, $lineNumber, $line)
    {
        parent::__construct("Key 文件行不完整，没有以 \\n 结尾. 文件: {$path}. 行: {$lineNumber} {$line}");
        $this->path = $path;
        $this->lineNumber = $lineNumber;
        $this->line = $line;
    }
}
