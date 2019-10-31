<?php

namespace RedisBackup;

use RedisBackup\Exception\RedisBackupException;
use RedisBackup\Exception\RedisBackupRedisFailedException;
use RedisBackup\Util\Logger;
use RedisBackup\Util\Sampler;
use RedisBackup\Util\Timer;

class RedisKeyRenameReverter
{
    /** @var \Redis */
    public $redis;
    /** @var \RedisBackup\KeyFileReader */
    public $keyFileReader;
    /** @var \RedisBackup\KeyFileWriter */
    public $keyFileWriter;
    /** @var string */
    public $renameSuffix;

    public $currentKey;
    public $keyCount;
    public $isFinished;
    public $isDebug;

    public function __construct($redis, $keyFileReader, $keyFileWriter, $renameSuffix)
    {
        $this->redis = $redis;
        $this->keyFileReader = $keyFileReader;
        $this->keyFileWriter = $keyFileWriter;
        $this->renameSuffix = $renameSuffix;
        $this->keyCount = 0;
        $this->isFinished = false;
    }

    public function isFinished()
    {
        return $this->isFinished;
    }

    protected function getOriginalKey($renamedKey)
    {
        if (substr($renamedKey, -strlen($this->renameSuffix)) !== $this->renameSuffix) {
            return false;
        }
        return substr($renamedKey, 0, -strlen($this->renameSuffix));
    }

    protected function revertRename($renamedKey)
    {
        $key = $this->getOriginalKey($renamedKey);
        if ($key === false) {
            return false;
        }
        if ($this->isDebug) {
            echo "Redis> RENAME $renamedKey $key", PHP_EOL;
        } else {
            $redis_result = $this->redis->rename($renamedKey, $key);
            if ($redis_result === false) {
                throw new RedisBackupRedisFailedException('RENAME', $this->redis);
            }
        }
        $this->keyFileWriter->write($key);
        return true;
    }

    public function run()
    {
        Logger::info("开始回滚重命名的 Redis key");
        Timer::start();
        while (!$this->isFinished()) {
            $this->currentKey = $this->keyFileReader->getKey();
            if ($this->currentKey === false) {
                $this->isFinished = true;
                break;
            }

            if (!$this->revertRename($this->currentKey)) {
                throw new RedisBackupException("Key {$this->currentKey} 后缀不正确");
            }

            ++$this->keyCount;
            if (Sampler::sample(300)) {
                $t = Timer::time();
                Logger::info("已回滚 key 数量: {$this->keyCount}\t耗时: {$t} s");
            }
        }
        $t = Timer::time();
        Logger::info("回滚完成！key 数量: {$this->keyCount}\t耗时: {$t} s");
    }
}
