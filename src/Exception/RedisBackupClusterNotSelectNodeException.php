<?php

namespace RedisBackup\Exception;

class RedisBackupClusterNotSelectNodeException extends RedisBackupException
{
    public $clusterNodesInfo;

    public function __construct($clusterNodesInfo)
    {
        parent::__construct("Redis 集群未设置节点\n{$clusterNodesInfo}");
        $this->clusterNodesInfo = $clusterNodesInfo;
    }
}
