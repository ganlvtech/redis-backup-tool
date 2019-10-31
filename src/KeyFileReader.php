<?php

namespace RedisBackup;

use RedisBackup\Exception\RedisBackupFileNotExistsException;
use RedisBackup\Exception\RedisBackupKeyFileLineEndException;

class KeyFileReader
{
    public $path;
    public $handle;
    public $lineNumber;

    public function __construct($path)
    {
        $this->handle = fopen($path, 'r');
        if (!$this->handle) {
            throw new RedisBackupFileNotExistsException($path);
        }
        $this->path = $path;
        $this->lineNumber = 0;
    }

    public function getKey()
    {
        ++$this->lineNumber;
        $line = fgets($this->handle);
        if ($line === false) {
            return false;
        }
        $key = $this->fileLineToKey($line);
        if (!$key) {
            return false;
        }
        return $key;
    }

    /**
     * 去除一行末尾的回车
     *
     * @param string $line
     *
     * @return false|string
     * @throws \RedisBackup\Exception\RedisBackupKeyFileLineEndException
     */
    protected function fileLineToKey($line)
    {
        if (substr($line, -1, 1) !== "\n") {
            throw new RedisBackupKeyFileLineEndException($this->path, $this->lineNumber, $line);
        }
        $key = substr($line, 0, -1);
        return $key;
    }

    public function jumpAfter($key)
    {
        while ($this->getKey() === $key) {
            // do nothing
        }
    }
}
