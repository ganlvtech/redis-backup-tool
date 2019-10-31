<?php

namespace RedisBackup;

use RedisBackup\Exception\RedisBackupClusterNotSelectNodeException;
use RedisBackup\Exception\RedisBackupException;
use RedisBackup\RedisMysqlCompareRenamer\RedisHashMysqlCompareRenamer;
use RedisBackup\RedisMysqlCompareRenamer\RedisStringMysqlCompareRenamer;
use RedisBackup\RedisMysqlWriter\RedisHashMysqlWriter;
use RedisBackup\RedisMysqlWriter\RedisStringMysqlWriter;

/**
 * 备份 Redis 的 4 个过程 scan -> write -> compare & rename -> del
 *
 * 1. 扫描匹配指定 pattern 的 Keys 保存的一个文本文件中，每行一个 Key
 * 2. 读取这个文本文件的每一行，然后从 Redis 读取，写入数据库
 * 3. 读取这个文本文件的每一行，读取数据库，再读 Redis，比较是否相等
 *    如果相等则计划执行 rename 操作，把 rename 后的 key 写另外一个文本文件
 *    如果不相等报错停止执行，提示 key 名称和不相等的值，二者的 json_encode，
 *    然后再执行 rename 操作
 * 4. 打开客户端测试此备份操作是否影响使用
 * 5. 读取 rename 后的 key 文本文件，执行 del 操作
 */
class RedisBackup
{
    /** @var \Redis */
    public $redis;
    public $redisIsCluster = false;
    public $redisNodeId = null;
    /** @var \mysqli */
    public $mysqli;

    /**
     * @param \Redis $redis
     * @param \mysqli $mysqli
     * @param string $redisNodeId
     */
    public function __construct($redis, $mysqli = null, $redisNodeId = '')
    {
        $this->redis = $redis;
        $this->mysqli = $mysqli;
        $this->redisNodeId = $redisNodeId;
    }

    /**
     * @throws \RedisBackup\Exception\RedisBackupClusterNotSelectNodeException
     */
    public function init()
    {
        $this->checkRedisCluster();
    }

    protected function checkRedisCluster()
    {
        $info = $this->redis->info('server');
        if (isset($info['redis_mode']) && $info['redis_mode'] === 'cluster') {
            $this->redisIsCluster = true;
            if (empty($this->redisNodeId)) {
                throw new RedisBackupClusterNotSelectNodeException($this->getClusterNodesInfo());
            }
        } else {
            $this->redisIsCluster = false;
        }
        return true;
    }

    public function getClusterNodesInfo()
    {
        $nodes_string = $this->redis->rawCommand('cluster', 'nodes');
        return "Node ID                                  IP                      主从  对应主节点 ID\n"
            . "======================================== ======================= ===== ========================================\n"
            . $nodes_string;
    }

    /**
     * 构造 Scanner
     *
     * 扫描匹配指定 pattern 的 keys 保存的一个文本文件中，每行一个 Key
     *
     * @param string $pattern 匹配模式
     * @param string $type key 的类型（暂时没有使用）
     * @param int $startPointer SCAN 命令的指针
     * @param \RedisBackup\KeyFileWriter $scannedKeysFileWriter
     * @param \RedisBackup\Util\FileStorage $scanPointerFileStorage
     *
     * @return \RedisBackup\RedisScanner
     */
    public function scanner($pattern, $type, $startPointer, $scannedKeysFileWriter, $scanPointerFileStorage)
    {
        return new RedisScanner(
            $this->redis,
            $this->redisIsCluster,
            $this->redisNodeId,
            $pattern,
            $type,
            $startPointer,
            $scannedKeysFileWriter,
            $scanPointerFileStorage
        );
    }

    /**
     * 构造 Writer
     *
     * 从 Redis 读取，写入数据库
     *
     * @param string $type
     * @param \RedisBackup\KeyFileReader $scannedKeysFileReader
     * @param \RedisBackup\KeyFileWriter $writtenKeysFileWriter
     *
     * @return \RedisBackup\RedisMysqlWriter
     * @throws \RedisBackup\Exception\RedisBackupException
     */
    public function writer($type, $scannedKeysFileReader, $writtenKeysFileWriter)
    {
        switch ($type) {
            case 'string':
                return new RedisStringMysqlWriter($this->redis, $this->mysqli, $scannedKeysFileReader, $writtenKeysFileWriter);
            case 'hash':
                return new RedisHashMysqlWriter($this->redis, $this->mysqli, $scannedKeysFileReader, $writtenKeysFileWriter);
            default:
                throw new RedisBackupException("不支持 $type 类型");
        }
    }

    /**
     * 构造 Renamer
     *
     * 读取数据库，再读 Redis，比较是否相等，如果相等则执行 rename 操作，把 rename 后的 key 写另外一个文本文件
     *
     * @param string $type
     * @param \RedisBackup\KeyFileReader $writtenKeysFileReader
     * @param \RedisBackup\KeyFileWriter $renamedKeysFileWriter
     * @param string $renameSuffix
     *
     * @return \RedisBackup\RedisMysqlCompareRenamer
     * @throws \RedisBackup\Exception\RedisBackupException
     */
    public function renamer($type, $writtenKeysFileReader, $renamedKeysFileWriter, $renameSuffix)
    {
        switch ($type) {
            case 'string':
                return new RedisStringMysqlCompareRenamer($this->redis, $this->mysqli, $writtenKeysFileReader, $renamedKeysFileWriter, $renameSuffix);
            case 'hash':
                return new RedisHashMysqlCompareRenamer($this->redis, $this->mysqli, $writtenKeysFileReader, $renamedKeysFileWriter, $renameSuffix);
            default:
                throw new RedisBackupException("不支持 $type 类型");
        }
    }

    /**
     * 构造 Reverter
     *
     * 将 Redis 中的 Keys 名称恢复回原来的名称
     *
     * @param \RedisBackup\KeyFileReader $renamedKeysFileReader 保存重命名成功 keys 的文件
     * @param \RedisBackup\KeyFileWriter $revertedKeysFileWriter 恢复重命名成功 keys 的文件
     * @param string $renameSuffix
     *
     * @return \RedisBackup\RedisKeyRenameReverter
     */
    public function reverter($renamedKeysFileReader, $revertedKeysFileWriter, $renameSuffix)
    {
        return new RedisKeyRenameReverter($this->redis, $renamedKeysFileReader, $revertedKeysFileWriter, $renameSuffix);
    }

    /**
     * 构造 Remover
     *
     * 读取 rename 后的 key 文本文件，执行 del 操作
     *
     * @param \RedisBackup\KeyFileReader $renamedKeysFileReader
     * @param \RedisBackup\KeyFileWriter $deletedKeysFileWriter
     *
     * @return \RedisBackup\RedisKeyRemover
     */
    public function remover($renamedKeysFileReader, $deletedKeysFileWriter)
    {
        return new RedisKeyRemover($this->redis, $renamedKeysFileReader, $deletedKeysFileWriter);
    }
}
