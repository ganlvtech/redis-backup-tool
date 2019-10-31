<?php

namespace RedisBackup\RedisMysqlWriter;

use RedisBackup\Exception\RedisBackupJsonEncodeFailedException;
use RedisBackup\Exception\RedisBackupMySQLFailedException;
use RedisBackup\Exception\RedisBackupRedisFailedException;
use RedisBackup\RedisMysqlWriter;
use RedisBackup\Util\Logger;
use RedisBackup\Util\Sampler;
use RedisBackup\Util\Timer;

class RedisHashMysqlWriter extends RedisMysqlWriter
{
    public function getOneKeyValue()
    {
        $this->currentKey = $this->keyFileReader->getKey();
        if ($this->currentKey === false) {
            $this->isFinished = true;
            return false;
        }

        $redis_result = $this->redis->hGetAll($this->currentKey);
        if ($redis_result === false) {
            throw new RedisBackupRedisFailedException('HGETALL', $this->redis);
        }

        $value = json_encode($redis_result);
        if ($value === false) {
            throw new RedisBackupJsonEncodeFailedException($value);
        }

        return $value;
    }

    public function run()
    {
        Logger::info("开始读取 Redis 并写入 MySQL");

        $sql = "INSERT INTO redis_hash_backup (k, v, created_at) VALUES (?, ?, CURRENT_TIMESTAMP()) ON DUPLICATE KEY UPDATE v = ?, updated_at = CURRENT_TIMESTAMP()";
        $stmt = $this->mysqli->prepare($sql);
        if (!$stmt) {
            throw new RedisBackupMySQLFailedException('MySQL Prepare 失败. SQL: ' . $sql, $this->mysqli);
        }
        $stmt->bind_param('sss', $key, $value, $value);

        Timer::start();
        while (!$this->isFinished()) {
            // 读 Redis
            $value = $this->getOneKeyValue();
            if ($value == false) {
                break;
            }
            $key = $this->currentKey;

            // 写 MySQL
            if (!$stmt->execute()) {
                throw new RedisBackupMySQLFailedException('MySQL redis_hash_backup 插入失败', $this->mysqli);
            }
            $this->keyFileWriter->write($key);

            ++$this->keyCount;
            if (Sampler::sample(300)) {
                $t = Timer::time();
                Logger::info("已写入 hash 数量: {$this->keyCount}\t耗时: {$t} s");
            }
        }
        $t = Timer::time();
        Logger::info("写入完成！hash 数量: {$this->keyCount}\t耗时: {$t} s");
    }
}
