<?php

namespace RedisBackup\RedisMysqlCompareRenamer;

use RedisBackup\Exception\RedisBackupException;
use RedisBackup\Exception\RedisBackupJsonDecodeFailedException;
use RedisBackup\Exception\RedisBackupJsonEncodeFailedException;
use RedisBackup\Exception\RedisBackupMySQLFailedException;
use RedisBackup\Exception\RedisBackupRedisFailedException;
use RedisBackup\RedisMysqlCompareRenamer;
use RedisBackup\Util\Logger;
use RedisBackup\Util\Sampler;
use RedisBackup\Util\Timer;

class RedisHashMysqlCompareRenamer extends RedisMysqlCompareRenamer
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

        ksort($redis_result);
        $value = json_encode($redis_result);
        if ($value === false) {
            throw new RedisBackupJsonEncodeFailedException($value);
        }

        return $value;
    }

    public function run()
    {
        Logger::info("开始读取 Redis 和 MySQL 并进行比较，成功后修改 Redis key 的名称");

        $sql = "SELECT v FROM {$this->tableName} WHERE k = ?";
        $stmt = $this->mysqli->prepare($sql);
        if (!$stmt) {
            throw new RedisBackupMySQLFailedException('MySQL Prepare 失败. SQL: ' . $sql, $this->mysqli);
        }
        $stmt->bind_param('s', $key);
        $stmt->bind_result($value);

        Timer::start();
        while (!$this->isFinished()) {
            // 读 Redis
            $redis_values_encoded = $this->getOneKeyValue();
            if ($redis_values_encoded == false) {
                break;
            }
            $key = $this->currentKey;

            // 读 MySQL
            if (!$stmt->execute()) {
                throw new RedisBackupMySQLFailedException('MySQL redis_hash_backup 读取失败', $this->mysqli);
            }
            if (!$stmt->fetch()) {
                throw new RedisBackupMySQLFailedException('MySQL redis_hash_backup 读取数据失败', $this->mysqli);
            }
            $mysql_values = json_decode($value, true);
            if ($mysql_values === null) {
                throw new RedisBackupJsonDecodeFailedException($value);
            }
            ksort($mysql_values);
            $mysql_values_encoded = json_encode($mysql_values);
            if ($mysql_values_encoded === false) {
                throw new RedisBackupJsonEncodeFailedException($value);
            }

            // 比较
            if ($mysql_values_encoded !== $redis_values_encoded) {
                throw new RedisBackupException('MySQL 与 Redis 数据不一致. MySQL: ' . $mysql_values_encoded . ' . Redis: ' . $redis_values_encoded);
            }

            // 重命名
            $this->rename($key);

            ++$this->keyCount;
            if (Sampler::sample(300)) {
                $t = Timer::time();
                Logger::info("已重命名 hash 数量: {$this->keyCount}\t耗时: {$t} s");
            }
        }
        $t = Timer::time();
        Logger::info("重命名完成！hash 数量: {$this->keyCount}\t耗时: {$t} s");
    }
}
