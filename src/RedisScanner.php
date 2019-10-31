<?php

namespace RedisBackup;

use RedisBackup\Exception\RedisBackupRedisFailedException;
use RedisBackup\Util\Logger;
use RedisBackup\Util\Timer;

class RedisScanner
{
    /** @var \Redis */
    public $redis;
    /** @var \RedisBackup\KeyFileWriter */
    public $keyFileWriter;
    /** @var \RedisBackup\Util\FileStorage */
    public $pointerStorage;
    /** @var bool */
    public $redisIsCluster;
    /** @var string */
    public $redisNodeId;

    public $pattern = '*';
    public $type = 'hash';
    public $startPointer;
    public $pointer;

    public $targetScanKeyCount;
    public $scannedRound;
    public $scannedKeyCount;
    public $keyCount;

    /**
     * RedisScanner constructor.
     *
     * @param \Redis $redis
     * @param bool $redisIsCluster
     * @param string $redisNodeId
     * @param string $pattern
     * @param string $type todo unused
     * @param int $startPointer
     * @param \RedisBackup\KeyFileWriter $keyFileWriter
     * @param \RedisBackup\Util\FileStorage $pointerStorage
     */
    public function __construct($redis, $redisIsCluster, $redisNodeId,
                                $pattern, $type, $startPointer,
                                $keyFileWriter, $pointerStorage)
    {
        $this->redis = $redis;
        $this->redisIsCluster = $redisIsCluster;
        $this->redisNodeId = $redisNodeId;

        $this->pattern = $pattern;
        $this->type = $type;

        $this->keyFileWriter = $keyFileWriter;
        $this->pointerStorage = $pointerStorage;

        $this->scannedRound = 0;
        $this->scannedKeyCount = 0;
        $this->keyCount = 0;

        $this->startPointer = $startPointer;
        $this->pointer = $this->pointerStorage->get();
        if ($this->pointer === false) {
            $this->pointer = $this->startPointer;
        } else {
            $this->pointer = (int)$this->pointer;
            if ($this->pointer === $this->startPointer) {
                $this->scannedRound = 1;
            }
        }
    }

    /**
     * 支持集群的 scan 命令
     *
     * @param int $pointer
     * @param string $pattern
     * @param int $count
     *
     * @return array|false [ next_pointer, [ key1, key2 ]]
     */
    public function scanCommand($pointer, $pattern, $count)
    {
        if ($this->redisIsCluster) {
            $redis_result = $this->redis->rawCommand('SCAN', $pointer, 'MATCH', $pattern, 'COUNT', $count, $this->redisNodeId);
        } else {
            $redis_result = $this->redis->rawCommand('SCAN', $pointer, 'MATCH', $pattern, 'COUNT', $count);
        }
        return $redis_result;
    }

    public function runOnce($batch = 1000)
    {
        $redis_result = $this->scanCommand($this->pointer, $this->pattern, $batch);
        if ($redis_result === false) {
            throw new RedisBackupRedisFailedException('SCAN', $this->redis);
        }
        $this->pointer = (int)$redis_result[0];
        // 保存当前 scan pointer
        $this->pointerStorage->set($this->pointer);
        if ($this->pointer == $this->startPointer) {
            ++$this->scannedRound;
        }
        $keys = $redis_result[1];
        $this->keyFileWriter->writeMultiple($keys);
        $this->scannedKeyCount += $batch;
        $this->keyCount += count($keys);
    }

    public function run($batch = 1000)
    {
        Logger::info("开始扫描。初始 SCAN 指针: {$this->pointer}");
        Timer::start();
        while (!$this->isFinished()) {
            $this->runOnce($batch);
            $t = Timer::time();
            Logger::info("已扫描: {$this->scannedKeyCount}\t已找到: {$this->keyCount}\tSCAN 指针: {$this->pointer}\t耗时: {$t} s");
        }
        Logger::info('扫描结束');
    }

    public function setTargetScanKeyCount($targetScanKeyCount)
    {
        $this->targetScanKeyCount = $targetScanKeyCount;
    }

    public function isFinished()
    {
        return $this->scannedKeyCount >= $this->targetScanKeyCount || $this->scannedRound > 0;
    }
}
