<?php

namespace RedisBackup\Exception;

class RedisBackupWriteFileException extends RedisBackupException
{
    public $content;
    public $file;

    public function __construct($file, $content)
    {
        parent::__construct("写入文件错误。文件: {$file}. 内容: {$content}");
        $this->content = $content;
        $this->file = $file;
    }
}
