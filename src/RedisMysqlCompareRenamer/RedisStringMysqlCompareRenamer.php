<?php

namespace RedisBackup\RedisMysqlCompareRenamer;

use RedisBackup\Exception\RedisBackupMySQLFailedException;
use RedisBackup\Exception\RedisBackupRedisFailedException;
use RedisBackup\Exception\RedisBackupValueNotEqualsException;
use RedisBackup\RedisMysqlCompareRenamer;
use RedisBackup\Util\Logger;
use RedisBackup\Util\Sampler;
use RedisBackup\Util\Timer;

class RedisStringMysqlCompareRenamer extends RedisMysqlCompareRenamer
{

    public function getOneKeyValue()
    {
        $this->currentKey = $this->keyFileReader->getKey();
        if ($this->currentKey === false) {
            $this->isFinished = true;
            return false;
        }

        $redis_result = $this->redis->get($this->currentKey);
        if ($redis_result === false) {
            throw new RedisBackupRedisFailedException('GET', $this->redis);
        }

        return $redis_result;
    }

    public function run()
    {
        Logger::info("开始读取 Redis 和 MySQL 并进行比较，成功后修改 Redis key 的名称");

        $sql = "SELECT v FROM redis_string_backup WHERE k = ?";
        $stmt = $this->mysqli->prepare($sql);
        if (!$stmt) {
            throw new RedisBackupMySQLFailedException('MySQL Prepare 失败. SQL: ' . $sql, $this->mysqli);
        }
        $stmt->bind_param('s', $key);
        $stmt->bind_result($value);

        Timer::start();
        while (!$this->isFinished()) {
            // 读 Redis
            $redis_value = $this->getOneKeyValue();
            if ($redis_value == false) {
                break;
            }
            $key = $this->currentKey;

            // 读 MySQL
            if (!$stmt->execute()) {
                throw new RedisBackupMySQLFailedException('MySQL redis_string_backup 读取失败', $this->mysqli);
            }
            if (!$stmt->fetch()) {
                throw new RedisBackupMySQLFailedException('MySQL redis_string_backup 读取数据失败', $this->mysqli);
            }
            $mysql_value = $value;

            // 比较
            if ($redis_value !== $mysql_value) {
                throw new RedisBackupValueNotEqualsException($key, $mysql_value, $redis_value);
            }

            // 重命名
            $this->rename($key);

            ++$this->keyCount;
            if (Sampler::sample(300)) {
                $t = Timer::time();
                Logger::info("已重命名 string 数量: {$this->keyCount}\t耗时: {$t} s");
            }
        }
        $t = Timer::time();
        Logger::info("重命名完成！string 数量: {$this->keyCount}\t耗时: {$t} s");
    }
}
