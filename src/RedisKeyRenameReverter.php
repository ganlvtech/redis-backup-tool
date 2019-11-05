<?php

namespace RedisBackup;

use RedisBackup\Exception\RedisBackupCountNotEqualsException;
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

    public $queuedKeys;
    public $queuedOriginalKeys;

    public $currentKey;
    public $keyCount;
    public $isDebug;

    public function __construct($redis, $keyFileReader, $keyFileWriter, $renameSuffix)
    {
        $this->redis = $redis;
        $this->keyFileReader = $keyFileReader;
        $this->keyFileWriter = $keyFileWriter;
        $this->renameSuffix = $renameSuffix;

        $this->queuedKeys = array();

        $this->currentKey = '';
        $this->keyCount = 0;
    }

    public function isFinished()
    {
        return $this->keyFileReader->isFinished();
    }

    public function queueKey($key)
    {
        $this->queuedKeys[] = $key;
    }

    public function clearQueuedKeys()
    {
        $this->queuedKeys = array();
    }

    // ==================== 重命名部分 ====================

    protected function getOriginalKey($renamedKey)
    {
        if (substr($renamedKey, -strlen($this->renameSuffix)) !== $this->renameSuffix) {
            return false;
        }
        $renamedKey = substr($renamedKey, 0, -strlen($this->renameSuffix));
        if (substr($renamedKey, 0, 1) !== '{') {
            return false;
        }
        if (substr($renamedKey, -1) !== '}') {
            return false;
        }
        $renamedKey = substr($renamedKey, 1, -1);
        return $renamedKey;
    }

    public function renameQueuedKeys()
    {
        $this->queuedOriginalKeys = array();
        if ($this->isDebug) {
            foreach ($this->queuedKeys as $key) {
                $this->currentKey = $key;
                $originalKey = $this->getOriginalKey($key);
                echo "Redis> RENAME $key $originalKey", PHP_EOL;
                $this->queuedOriginalKeys[] = $originalKey;
            }
        } else {
            $this->redis->multi(\Redis::PIPELINE);
            $expect_result_count = 0;
            foreach ($this->queuedKeys as $key) {
                $this->currentKey = $key;
                $originalKey = $this->getOriginalKey($key);
                $this->redis->rename($key, $originalKey);
                ++$expect_result_count;
                $this->queuedOriginalKeys[] = $originalKey;
            }

            $redis_result_array = $this->redis->exec();
            if ($redis_result_array === false) {
                throw new RedisBackupRedisFailedException('PIPELINE & EXEC', $this->redis);
            }

            $actual_result_count = count($redis_result_array);
            if ($actual_result_count !== $expect_result_count) {
                throw new RedisBackupCountNotEqualsException('Redis Pipeline 返回结果', $expect_result_count, $actual_result_count);
            }

            $i = 0;
            foreach ($this->queuedKeys as $key) {
                $this->currentKey = $key;
                if ($redis_result_array[$i] === false) {
                    throw new RedisBackupRedisFailedException('RENAME', $this->redis);
                }
                ++$i;
            }
        }
    }

    // ==================== 主程序 ====================

    public function runBatch()
    {
        $this->renameQueuedKeys();
        $this->clearQueuedKeys();
        $this->keyCount += count($this->queuedOriginalKeys);
        $this->keyFileWriter->writeMultiple($this->queuedOriginalKeys);

        $t = Timer::time();
        Logger::info("已回滚 key 数量: {$this->keyCount}\t耗时: {$t} s");
    }

    public function run($batch = 300)
    {
        Logger::info("开始回滚重命名的 Redis key");
        Timer::start();
        while (!$this->isFinished()) {
            $this->currentKey = $this->keyFileReader->getKey();
            if ($this->currentKey === false) {
                break;
            }
            $this->queueKey($this->currentKey);
            if (Sampler::sample($batch)) {
                $this->runBatch();
            }
        }
        $this->runBatch();
        Logger::info("回滚完成！");
    }
}
