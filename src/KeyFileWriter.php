<?php

namespace RedisBackup;

use RedisBackup\Exception\RedisBackupWriteKeyException;
use RedisBackup\Util\FileBackup;

class KeyFileWriter
{
    public $path;

    /**
     * 打开一个文件，待写入
     *
     * @param string $path
     * @param bool $append 设为 true 会在文件存在时重命名文件，设为 false 直接在文件末尾追加
     */
    public function __construct($path, $append = false)
    {
        if (!$append) {
            FileBackup::backup($path);
        }
        $this->path = $path;
    }

    /**
     * 写入一个 key
     *
     * @param string $key
     *
     * @throws \RedisBackup\Exception\RedisBackupWriteKeyException
     */
    public function write($key)
    {
        if (!static::saveKeyToFile($this->path, $key)) {
            throw new RedisBackupWriteKeyException($key, $this->path);
        }
    }

    /**
     * 写入多个 key
     *
     * @param string[] $keys
     *
     * @throws \RedisBackup\Exception\RedisBackupWriteKeyException
     */
    public function writeMultiple($keys)
    {
        if (!$keys) {
            return;
        }
        if (!static::saveKeysToFile($this->path, $keys)) {
            throw new RedisBackupWriteKeyException(implode(',', $keys), $this->path);
        }
    }

    /**
     * 写入文件
     *
     * @param string $path
     * @param string[] $keys
     *
     * @return bool|false|int 成功返回写入字节数，失败返回 false
     */
    public static function saveKeysToFile($path, $keys)
    {
        if ($keys) {
            return file_put_contents($path, implode("\n", $keys) . "\n", FILE_APPEND);
        }
        return false;
    }

    /**
     * 写入文件
     *
     * @param string $path
     * @param string $key
     *
     * @return bool|false|int 成功返回写入字节数，失败返回 false
     */
    public static function saveKeyToFile($path, $key)
    {
        return file_put_contents($path, $key . "\n", FILE_APPEND);
    }
}
