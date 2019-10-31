<?php

namespace RedisBackup\Util;

use RedisBackup\Exception\RedisBackupWriteFileException;

class FileStorage
{
    public $path;

    public function __construct($path, $backup = true)
    {
        if ($backup) {
            FileBackup::backup($path, true);
        }
        $this->path = $path;
    }

    public function get()
    {
        if (!file_exists($this->path)) {
            return false;
        }
        return file_get_contents($this->path);
    }

    public function set($value)
    {
        if (!file_put_contents($this->path, $value)) {
            throw new RedisBackupWriteFileException($this->path, $value);
        }
    }
}
