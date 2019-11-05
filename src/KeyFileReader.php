<?php

namespace RedisBackup;

use RedisBackup\Exception\RedisBackupFileNotExistsException;
use RedisBackup\Exception\RedisBackupKeyFileLineEndException;

class KeyFileReader
{
    /** @var string 文件路径 */
    protected $path;
    protected $handle;
    /** @var int 当前行号，默认为 0，每读取完一行 +1 */
    protected $lineNumber;
    /** @var bool 是否达到文件末尾 */
    protected $isEnd;

    public function __construct($path)
    {
        $this->handle = fopen($path, 'r');
        if (!$this->handle) {
            throw new RedisBackupFileNotExistsException($path);
        }
        $this->path = $path;
        $this->lineNumber = 0;
        $this->isEnd = false;
    }

    public function eof()
    {
        $this->isEnd = true;
        fclose($this->handle);
    }

    public function isFinished()
    {
        return $this->isEnd;
    }

    public function getKey()
    {
        if ($this->isFinished()) {
            return false;
        }
        ++$this->lineNumber;
        $line = fgets($this->handle);
        if ($line === false) {
            $this->eof();
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
        while ($this->getKey() !== $key) {
            if ($this->isFinished()) {
                break;
            }
        }
    }

    /**
     * @return string
     */
    public function getPath()
    {
        return $this->path;
    }

    /**
     * @return int
     */
    public function getLineNumber()
    {
        return $this->lineNumber;
    }
}
